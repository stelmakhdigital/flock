import fs from 'node:fs';
import path from 'node:path';
import type { Server } from 'node:http';
import { Hono } from 'hono';
import { createAdaptorServer } from '@hono/node-server';
import { createNodeWebSocket } from '@hono/node-ws';
import * as store from './store.js';
import * as terminal from './terminal.js';
import { resolveAgent, manifestRuntime } from './agent.js';
import { getAdapter, type AdapterEnv } from './runtime-adapter.js';
import { apply, OpError, type CoreCtx } from './ops.js';

const safeJson = (s: string): unknown => {
  try {
    return JSON.parse(s);
  } catch {
    return s;
  }
};

export function createHttp(ctx: CoreCtx) {
  const app = new Hono();
  const { injectWebSocket, upgradeWebSocket } = createNodeWebSocket({ app });

  app.use('*', async (c, next) => {
    const got = c.req.header('authorization')?.replace(/^Bearer\s+/i, '');
    if (got !== ctx.store.token) return c.json({ error: 'unauthorized' }, 401);
    await next();
  });

  app.get('/healthz', (c) => {
    let dbOk = true;
    try {
      ctx.store.db.prepare('SELECT 1 AS ok').get();
    } catch {
      dbOk = false;
    }
    return c.json({
      ok: dbOk,
      db: dbOk ? 'ok' : 'error',
      pid: process.pid,
      startedAt: ctx.startedAt,
      ticks: ctx.ticks.all(),
      pods: store.listPods(ctx.store),
    });
  });

  app.get('/api/pods', async (c) => {
    // pods + typed runner state (sidecar) + checkReady (live: sidecar +
    // foreground-pane guard) + runs with meta parsed
    const adapterEnv: AdapterEnv = {
      home: ctx.store.home,
      token: ctx.store.token,
      runnerPath: path.join(import.meta.dirname, 'runner.js'),
    };
    const pods = (await Promise.all(
      store.listPods(ctx.store).map(async (p) => {
        const row: (store.Pod & { runner: unknown; ready?: unknown }) & Record<string, unknown> = {
          ...p,
          runner: p.state === 'live' ? terminal.readRunnerState(ctx.store.home, p.role) : null,
        };
        if (p.state === 'live' && p.agent && p.agent !== 'cmd') {
          const resolved = resolveAgent(p.agent, null);
          if (resolved && manifestRuntime(resolved.manifest) !== 'cmd') {
            const adapter = getAdapter(resolved.manifest, adapterEnv);
            if (adapter) {
              row.ready = await adapter
                .checkReady({ role: p.role, cwd: p.dir })
                .catch(() => ({ ready: false, reason: 'check failed' }));
            }
          }
        }
        return row;
      }),
    )) as (store.Pod & { runner: unknown; ready?: unknown })[];
    const runs = store.listRuns(ctx.store).map((r) => ({
      ...r,
      meta: r.meta ? safeJson(r.meta) : null,
    }));
    return c.json({ pods, runs });
  });

  app.get('/api/watchdog', (c) => {
    return c.json({ jobs: store.listWatchdogJobs(ctx.store) });
  });

  app.get('/api/watchdog/:id/history', (c) => {
    const job = store.getWatchdogJob(ctx.store, c.req.param('id'));
    if (!job) return c.json({ error: `no watchdog job: ${c.req.param('id')}` }, 404);
    return c.json({ job, history: store.listWatchdogHistory(ctx.store, job.id) });
  });

  app.get('/api/tasks', (c) => {
    const status = c.req.query('status');
    return c.json({ tasks: store.listTasks(ctx.store, status) });
  });

  app.post('/api/ops', async (c) => {
    const op = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
    if (!op || typeof op !== 'object' || Array.isArray(op)) {
      return c.json({ ok: false, error: 'invalid op json' }, 400);
    }
    try {
      const result = await apply(op, ctx);
      return c.json({ ok: true, result });
    } catch (e) {
      if (e instanceof OpError) return c.json({ ok: false, error: e.message }, e.status as 400);
      return c.json({ ok: false, error: e instanceof Error ? e.message : String(e) }, 500);
    }
  });

  // bus -> websocket broadcast (UI will plug in at stage 5)
  const clients = new Set<unknown>();
  const emit = (e: Record<string, unknown>) => {
    const s = JSON.stringify(e);
    for (const ws of clients) {
      try {
        const w = ws as { readyState: number; send: (s: string) => void };
        if (w.readyState === 1) w.send(s);
      } catch {
        // dead client, closed by onClose
      }
    }
  };

  app.get(
    '/events',
    upgradeWebSocket(() => ({
      onOpen: (_evt: unknown, ws: unknown) => {
        clients.add(ws);
      },
      onMessage: (_evt: unknown, _ws: unknown) => {},
      onClose: (_evt: unknown, ws: unknown) => {
        clients.delete(ws);
      },
    })),
    (c: any) => c.body(null),
  );

  return { app, injectWebSocket, emit };
}

// Per-pod unix socket: <pod dir>/core.sock. The pod dir is the workspace —
// the only part of $HOME visible to the pi sandbox (bwrap masks /home). A
// unix socket needs no network namespace, so even an untrusted (no-net)
// pod can reach the daemon API with its CLI. Same auth (Bearer token).
const podSockets = new Map<string, Server>();

export function podSocketPath(podDir: string): string {
  return path.join(podDir, 'core.sock');
}

export function startPodSocket(ctx: CoreCtx, podDir: string): void {
  if (podSockets.has(podDir)) return;
  const sockPath = podSocketPath(podDir);
  try {
    fs.rmSync(sockPath, { force: true });
  } catch {
    /* stale file is fine, listen() replaces it */
  }
  const { app } = createHttp(ctx);
  const server = createAdaptorServer({ fetch: app.fetch }) as Server;
  server.on('error', (e: NodeJS.ErrnoException) => {
    if (e.code !== 'EADDRINUSE') console.error(`pod socket ${sockPath}: ${e.message}`);
  });
  server.listen(sockPath, () => {
    try {
      fs.chmodSync(sockPath, 0o660);
    } catch {
      /* best effort */
    }
  });
  podSockets.set(podDir, server);
}

export function stopPodSocket(podDir: string): void {
  const server = podSockets.get(podDir);
  if (!server) return;
  podSockets.delete(podDir);
  try {
    server.close();
  } catch {
    /* already gone */
  }
  try {
    fs.rmSync(podSocketPath(podDir), { force: true });
  } catch {
    /* best effort */
  }
}
