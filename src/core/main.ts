import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { serve } from '@hono/node-server';
import { openStore, crashStaleRuns, listPods, currentRun, endRun, type Run } from './store.js';
import { Ticks } from './ticks.js';
import { createHttp, startPodSocket } from './http.js';
import { runWatchdogTick } from './watchdog.js';
import { runArbiterTick, ARBITER_INTERVAL_MS } from './arbiter.js';
import { runHealthTick } from './health.js';
import { notifyPm } from './pm.js';
import { runRetentionSweep } from './retention.js';
import { writePodAgentsMd, adapterForPod } from './ops.js';
import { runEscalationTick } from './escalation.js';
import { startCodexShim } from './codex-shim.js';
import { codexShimPort } from './codex-protocol.js';
import type { RunLike } from './runtime-adapter.js';
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
const { app, emit } = createHttp(ctx);
ctx.emit = emit;
// stage 1: arbiter — claim/verify/handoff of the task queue
ticks.register('arbiter', ARBITER_INTERVAL_MS, () => runArbiterTick(ctx));
// stage 4.1: built-in pod health (gate/idle wake-ladder, 20s)
ticks.register('health', 20_000, () => runHealthTick(ctx));
// runkeeper (5s): the agent process is dead -> mark the run crashed fast.
// Runtime-agnostic: the ADAPTER answers "is this run's agent alive?" with
// its own typed signals (pi: launchId-scoped sidecar exit + foreground
// guard; claude: pane foreground). No per-runtime branches in the core.
// Adapters without liveness (bash/cmd) degrade to the generic pid check.
async function checkRunLiveness(): Promise<void> {
  for (const pod of listPods(store)) {
    if (pod.state !== 'live') continue;
    const run = currentRun(store, pod.role);
    if (!run || run.ended_at) continue;
    const like: RunLike = { id: run.id, pid: run.pid, meta: run.meta, started_at: run.started_at };
    const resolved = adapterForPod(pod, ctx);
    if (resolved?.adapter.liveness) {
      let res;
      try {
        res = await resolved.adapter.liveness(resolved.binding, like);
      } catch {
        continue; // transient probe failure: retry next tick
      }
      if (!res.alive) {
        const state = res.reason ?? 'crashed';
        endRun(store, run.id, state);
        const crashed = state.startsWith('crashed');
        console.log(`[core] runkeeper: run ${run.id} (pod ${pod.role}) ${state} [${resolved.adapter.runtime}]`);
        ctx.emit?.(crashed ? { type: 'run_crashed', pod: pod.role, run: run.id } : { type: 'run_ended', pod: pod.role, run: run.id, state });
        if (crashed) void notifyPm(ctx, { type: 'pod_crashed', detail: `под ${pod.role}: ${state}` }).catch(() => {});
      }
      continue;
    }
    // fallback: generic pid liveness (bash/cmd: the window's process; also
    // catches a killed window for any adapterless pod)
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
      void notifyPm(ctx, { type: 'pod_crashed', detail: `под ${pod.role}: процесс агента (pid ${run.pid}) умер` }).catch(() => {});
    }
  }
}
ticks.register('runkeeper', 5000, () => {
  checkRunLiveness().catch((e) => {
    console.warn('[core] runkeeper tick failed:', e instanceof Error ? e.message : e);
  });
});
// pm (goal loop, 5min): sweep the pipeline, wake the pm pod only on change
// retention (24h): archive old runs, head-trim activity logs, rotate core.log
ticks.register('retention', 24 * 3600_000, () => {
  runRetentionSweep({ store, home: FLOCK_HOME }).then((r) => {
    if (r.archivedRuns || r.trimmedActivity.length || r.coreLogRotated) {
      console.log(`[core] retention: runs=${r.archivedRuns} activity=${r.trimmedActivity.join(',') || '-'} log=${r.coreLogRotated ? 'rotated' : '-'}`);
    }
  }).catch((e) => {
    console.warn('[core] retention tick failed:', e instanceof Error ? e.message : e);
  });
});
// 5.4c durable escalation ladder (30s): walk open -> pm_notified -> escalated,
// auto-resolve when the condition heals. Always on: durability is the point.
ticks.register('escalation', 30_000, () => {
  runEscalationTick(ctx).catch((e) => {
    console.warn('[core] escalation tick failed:', e instanceof Error ? e.message : e);
  });
});
// watchdog: declarative checks registered by agents/CLI (1s tick)
ticks.register('watchdog', 1000, () => runWatchdogTick(ctx));

const pidFile = path.join(FLOCK_HOME, 'core.pid');

const server = serve({ fetch: app.fetch, hostname: '127.0.0.1', port: FLOCK_PORT }, () => {
  // codex shim (5.5): in-process OpenAI-responses proxy that merges codex's
  // developer-role messages into `instructions` (vLLM rejects the role in
  // input). Only the pod's codex reaches it; local-only, no auth.
  try {
    const shim = startCodexShim(codexShimPort(FLOCK_PORT), process.env.FLOCK_CODEX_UPSTREAM ?? 'http://192.168.1.114:8000');
    console.log(`[core] codex shim listening http://127.0.0.1:${shim.port} (upstream ${process.env.FLOCK_CODEX_UPSTREAM ?? 'http://192.168.1.114:8000'})`);
  } catch (e) {
    // shim down is not fatal: pi/claude pods are unaffected; codex spawns
    // will fail at the first model call (visible in the pod pane)
    console.warn('[core] codex shim failed to start:', e instanceof Error ? e.message : e);
  }
  // pod-local unix sockets (visible to the pi sandbox, no network needed);
  // role tags the socket so the operator token arriving on it is scoped to
  // that pod's 'pod'-scope ops (5.3)
  for (const pod of listPods(store)) {
    if (pod.state === 'live') startPodSocket(ctx, pod.dir, pod.role);
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
