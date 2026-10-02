import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { serve } from '@hono/node-server';
import { openStore, crashStaleRuns } from './store.js';
import { Ticks } from './ticks.js';
import { createHttp } from './http.js';
import { runWatchdogTick } from './watchdog.js';
import type { CoreCtx } from './ops.js';

export const FLOCK_HOME = process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock');
export const FLOCK_PORT = Number(process.env.FLOCK_PORT ?? 7460);

const startedAt = new Date().toISOString();
const store = openStore(FLOCK_HOME);

// Restart safety: any run left open by a dead core is marked crashed.
// No special recovery code — the same logic continues from the DB.
const crashed = crashStaleRuns(store);
if (crashed > 0) console.log(`[core] marked ${crashed} stale run(s) crashed`);

const ticks = new Ticks();
// stage 0: heartbeat only (liveness proof for /healthz).
ticks.register('heartbeat', 10_000, () => {});
// stage 1+: arbiter 10s (claim/handoff/verify)
// stage 3:   health 30s (stall/retry/re-wake)
// stage 4:   PM 5min (pipeline/intake) + goal loop (LLM lead, on trigger)

const ctx: CoreCtx = { store, ticks, startedAt };
const { app, injectWebSocket, emit } = createHttp(ctx);
ctx.emit = emit;
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
