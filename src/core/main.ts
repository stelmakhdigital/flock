import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { serve } from '@hono/node-server';
import { openStore, crashStaleRuns, listPods, currentRun, endRun } from './store.js';
import { readRunnerState, paneCommand, SHELL_COMMANDS } from './terminal.js';
import { Ticks } from './ticks.js';
import { createHttp, startPodSocket } from './http.js';
import { runWatchdogTick } from './watchdog.js';
import { runArbiterTick, ARBITER_INTERVAL_MS } from './arbiter.js';
import { runHealthTick } from './health.js';
import { pmTick, pmNotify } from './pm.js';
import { podRuntime } from './agent.js';
import { writePodAgentsMd } from './ops.js';
import type { CoreCtx } from './ops.js';

export const FLOCK_HOME = process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock');
export const FLOCK_PORT = Number(process.env.FLOCK_PORT ?? 7460);

const startedAt = new Date().toISOString();
const store = openStore(FLOCK_HOME);

// Auto-registration (W2): every pod window gets flock on PATH, so the agent
// can register its own watchdog jobs. The shim points at this core's CLI.
function writeFlockShim(home: string): void {
  const binDir = path.join(home, 'bin');
  fs.mkdirSync(binDir, { recursive: true });
  const binJs = path.join(import.meta.dirname, '..', 'bin.js');
  const shimPath = path.join(binDir, 'flock');
  const content = `#!/bin/sh\nexec ${process.execPath} ${JSON.stringify(binJs)} "$@"\n`;
  try {
    if (fs.existsSync(shimPath) && fs.readFileSync(shimPath, 'utf8') === content) return;
  } catch {}
  fs.writeFileSync(shimPath, content, { mode: 0o755 });
}
writeFlockShim(FLOCK_HOME);

// Restart safety: any run left open by a dead core is marked crashed.
// No special recovery code — the same logic continues from the DB.
const crashed = crashStaleRuns(store);
if (crashed > 0) console.log(`[core] marked ${crashed} stale run(s) crashed`);

// Refresh the protocol doc for every existing pod dir (pi reads it on start).
for (const p of listPods(store)) {
  try {
    writePodAgentsMd(p.dir, p.role);
  } catch {}
}

const ticks = new Ticks();
// stage 0: heartbeat only (liveness proof for /healthz).
ticks.register('heartbeat', 10_000, () => {});
// stage 3:   health 30s (stall/retry/re-wake) — will become built-in watchdog jobs
// stage 4:   PM 5min (pipeline/intake) + goal loop (LLM lead, on trigger)

const ctx: CoreCtx = { store, ticks, startedAt };
const { app, injectWebSocket, emit } = createHttp(ctx);
ctx.emit = emit;
// stage 1: arbiter — claim/verify/handoff of the task queue
ticks.register('arbiter', ARBITER_INTERVAL_MS, () => runArbiterTick(ctx));
// stage 4.1: built-in pod health (gate/idle wake-ladder, 20s)
ticks.register('health', 20_000, () => runHealthTick(ctx));
// runkeeper (5s): the agent process is dead -> mark the run crashed fast.
// Fast detection layer for "window alive, agent dead": the persistent pane
// outlives the runner, so pid-liveness alone would miss it.
// runkeeper (5s): the agent process is dead -> mark the run crashed fast.
// Two signals: (1) typed — the runner sidecar records the pi exit code,
// launchId-scoped; (2) pid liveness — covers non-runner pods and pane death.
function runLaunchId(run: { meta: string }): string | null {
  try {
    const arr = JSON.parse(run.meta || '[]');
    if (!Array.isArray(arr)) return null;
    const created = arr.find((e: unknown) => (e as { kind?: string })?.kind === 'created');
    return created && typeof (created as { launchId?: string }).launchId === 'string' ? (created as { launchId: string }).launchId : null;
  } catch {
    return null;
  }
}

function checkRunLiveness(): void {
  for (const pod of listPods(store)) {
    if (pod.state !== 'live') continue;
    const run = currentRun(store, pod.role);
    if (!run || run.ended_at) continue;
    // 1) typed: sidecar exit (only when it belongs to THIS run's launch)
    const launchId = runLaunchId(run);
    const st = readRunnerState(store.home, pod.role);
    if (launchId && st?.exited && st.launchId === launchId) {
      const ex = st.exited;
      const state = ex.code === 0 && !ex.signal ? 'clean' : `crashed(${ex.signal ? `signal ${ex.signal}` : `code ${ex.code}`})`;
      endRun(store, run.id, state);
      console.log(`[core] runkeeper: run ${run.id} (pod ${pod.role}) ${state} [sidecar]`);
      ctx.emit?.({ type: 'run_ended', pod: pod.role, run: run.id, state });
      continue;
    }
    // 2) foreground guard (pi pods, persistent pane): the pane is back at the
    // shell while THIS run's sidecar has no typed exit = the runner died
    // untyped (killed -9, OOM, ...). Relaunch never leaves an unended run
    // during the stop->paste gap, so "at shell" here is a real death.
    if (podRuntime(pod.agent) === 'pi' && launchId && st && st.launchId === launchId && !st.exited) {
      void (async () => {
        const fg = await paneCommand(pod.terminal_target!).catch(() => '');
        if (!SHELL_COMMANDS.has(fg)) return;
        endRun(store, run.id, 'crashed(runner gone, pane at shell)');
        console.log(`[core] runkeeper: run ${run.id} (pod ${pod.role}) runner gone (pane at shell) -> crashed`);
        ctx.emit?.({ type: 'run_crashed', pod: pod.role, run: run.id });
        void pmNotify(ctx, { type: 'pod_crashed', detail: `под ${pod.role}: runner умер нетипизированно (pane на shell)` }).catch(() => {});
      })();
      continue;
    }
    // 3) pid liveness (bash/cmd pods: the window's process; also catches a
    // killed window for any pod)
    if (!run.pid) continue;
    let alive = true;
    try {
      process.kill(run.pid, 0);
    } catch (e) {
      alive = (e as NodeJS.ErrnoException).code !== 'ESRCH'; // EPERM = alive
    }
    if (!alive) {
      endRun(store, run.id, 'crashed');
      console.log(`[core] runkeeper: pid ${run.pid} (pod ${pod.role}, run ${run.id}) dead -> crashed`);
      ctx.emit?.({ type: 'run_crashed', pod: pod.role, run: run.id, pid: run.pid });
      void pmNotify(ctx, { type: 'pod_crashed', detail: `под ${pod.role}: процесс агента (pid ${run.pid}) умер` }).catch(() => {});
    }
  }
}
ticks.register('runkeeper', 5000, checkRunLiveness);
// pm (goal loop, 5min): sweep the pipeline, wake the pm pod only on change
ticks.register('pm', 60_000, () => pmTick(ctx));
// watchdog: declarative checks registered by agents/CLI (1s tick)
ticks.register('watchdog', 1000, () => runWatchdogTick(ctx));

const pidFile = path.join(FLOCK_HOME, 'core.pid');

const server = serve({ fetch: app.fetch, hostname: '127.0.0.1', port: FLOCK_PORT }, () => {
  injectWebSocket(server);
  // pod-local unix sockets (visible to the pi sandbox, no network needed)
  for (const pod of listPods(store)) {
    if (pod.state === 'live') startPodSocket(ctx, pod.dir);
  }
  fs.writeFileSync(pidFile, String(process.pid));
  console.log(`[core] listening http://127.0.0.1:${FLOCK_PORT} pid=${process.pid} home=${FLOCK_HOME}`);
});

let stopping = false;
function shutdown(sig: string): void {
  if (stopping) return;
  stopping = true;
  console.log(`[core] ${sig}, shutting down`);
  ticks.stop();
  server.close(() => {
    try {
      fs.rmSync(pidFile, { force: true });
    } catch {}
    try {
      store.db.close();
    } catch {}
    console.log('[core] stopped clean');
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 5000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
