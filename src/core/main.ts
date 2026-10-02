import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { serve } from '@hono/node-server';
import { openStore, crashStaleRuns, listPods, currentRun, endRun } from './store.js';
import { Ticks } from './ticks.js';
import { createHttp } from './http.js';
import { runWatchdogTick } from './watchdog.js';
import { runArbiterTick, ARBITER_INTERVAL_MS } from './arbiter.js';
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
// runkeeper (5s): the agent process is dead -> mark the run crashed fast.
// Fast detection layer for "window alive, agent dead" (OpenRig's
// seat-identity reconciler, in our naming: run = the live occupant).
function checkRunLiveness(): void {
  for (const pod of listPods(store)) {
    if (pod.state !== 'live') continue;
    const run = currentRun(store, pod.role);
    if (!run || run.ended_at || !run.pid) continue;
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
    }
  }
}
ticks.register('runkeeper', 5000, checkRunLiveness);
// watchdog: declarative checks registered by agents/CLI (1s tick, OpenRig-style)
ticks.register('watchdog', 1000, () => runWatchdogTick(ctx));

const pidFile = path.join(FLOCK_HOME, 'core.pid');

const server = serve({ fetch: app.fetch, hostname: '127.0.0.1', port: FLOCK_PORT }, () => {
  injectWebSocket(server);
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
