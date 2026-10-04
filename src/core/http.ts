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
import { apply, listOps, OpError, type CoreCtx } from './ops.js';
import { listAlerts, healthOptsFromEnv } from './health.js';
import { pmDigest } from './pm.js';

const safeJson = (s: string): unknown => {
  try {
    return JSON.parse(s);
  } catch {
    return s;
  }
};

declare module 'hono' {
  // pod-socket caller identity (set by the auth middleware when the
  // request arrived tagged with flockRole)
  interface ContextVariableMap {
    flockCaller: { kind: 'pod'; role: string } | undefined;
  }
}

export function createHttp(ctx: CoreCtx) {
  const app = new Hono();
  const { injectWebSocket, upgradeWebSocket } = createNodeWebSocket({ app });

  app.use('*', async (c, next) => {
    const got = c.req.header('authorization')?.replace(/^Bearer\s+/i, '');
    if (got !== ctx.store.token) return c.json({ error: 'unauthorized' }, 401);
    // pod socket: mark the caller so op auth can narrow scope (the token is
    // the same core token; the SCOPE comes from which socket the request
    // arrived on — the operator CLI and the in-pod CLI share it)
    const env = c.env as { flockRole?: string };
    if (env?.flockRole) c.set('flockCaller', { kind: 'pod', role: env.flockRole });
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

  // checkReady cache: N tmux calls per /api/pods is wasted on a busy board;
  // ready-state changes on the scale of seconds, so 5s TTL is imperceptible.
  // Override with FLOCK_READY_CACHE_MS (0 = disabled).
  const readyCacheTtlMs = Number(process.env.FLOCK_READY_CACHE_MS ?? 5000);
  const readyCache = new Map<string, { at: number; result: unknown }>();
  const checkReadyCached = async (adapter: { checkReady(b: { role: string; cwd: string }): Promise<unknown> }, b: { role: string; cwd: string }) => {
    if (readyCacheTtlMs <= 0) {
      return adapter.checkReady(b).catch(() => ({ ready: false, reason: 'check failed' }));
    }
    const hit = readyCache.get(b.role);
    const now = Date.now();
    if (hit && now - hit.at < readyCacheTtlMs) return hit.result;
    const result = await adapter
      .checkReady(b)
      .catch(() => ({ ready: false, reason: 'check failed' }));
    readyCache.set(b.role, { at: now, result });
    return result;
  };

  app.get('/api/pods', async (c) => {
    // pods + typed runner state (sidecar) + checkReady (cached 5s) + runs
    const adapterEnv: AdapterEnv = {
      home: ctx.store.home,
      token: ctx.store.token,
      runnerPath: path.join(import.meta.dirname, 'runner.js'),
      codexBridgePath: path.join(import.meta.dirname, 'codex-bridge.js'),
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
            if (adapter) row.ready = await checkReadyCached(adapter, { role: p.role, cwd: p.dir });
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

  app.get('/api/health', (c) => {
    const alerts = listAlerts(ctx);
    const escs = store.listEscalations(ctx.store, true);
    return c.json({ alerts, opts: healthOptsFromEnv(), activeEscalations: escs.map((e) => ({ id: e.id, key: e.key, state: e.state, kind: e.kind, subject: e.subject, created_at: e.created_at })) });
  });

  app.get('/api/ops', (c) => {
    return c.json({ ops: listOps() });
  });

  app.get('/api/pm', async (c) => {
    return c.json({ pm: await pmDigest(ctx), alerts: listAlerts(ctx), usage: store.usageSummary(ctx.store) });
  });

  app.get('/api/usage', (c) => {
    const role = c.req.query('role') ?? undefined;
    const since = c.req.query('since') ?? undefined;
    return c.json({ usage: store.usageSummary(ctx.store, { role, since }) });
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
    const limit = Math.max(1, Math.min(500, Number(c.req.query('limit') ?? 50)));
    return c.json({ tasks: store.listTasks(ctx.store, status, limit) });
  });

  app.post('/api/ops', async (c) => {
    const op = (await c.req.json().catch(() => null)) as Record<string, unknown> | null;
    if (!op || typeof op !== 'object' || Array.isArray(op)) {
      return c.json({ ok: false, error: 'invalid op json' }, 400);
    }
    try {
      const caller = c.get('flockCaller') as { kind: 'pod'; role: string } | undefined;
      const result = await apply(op, { ...ctx, caller });
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

export function startPodSocket(ctx: CoreCtx, podDir: string, role?: string): void {
  if (podSockets.has(podDir)) return;
  const sockPath = podSocketPath(podDir);
  try {
    fs.rmSync(sockPath, { force: true });
  } catch {
    /* stale file is fine, listen() replaces it */
  }
  // pod-scoped authorization: the pod token may only reach ops with the
  // 'pod' scope (own-pod narrowing in the op handlers); the operator token
  // keeps full access to the socket. The wrapper clones the shared app and
  // injects the caller for op auth.
  const { app } = createHttp(ctx);
  // pod socket: the node-server adaptor calls fetch(req, { incoming,
  // outgoing }) — position 2 is the Hono env slot. Wrapping fetch tags
  // every request with the pod role so the auth middleware marks the
  // caller for op scope narrowing. The operator token stays valid on the
  // socket (its CLI uses the same core token) and stays unrestricted.
  const bound = app.fetch.bind(app);
  const fetchFn = role
    ? (async (req: Request, env?: unknown) => {
        const e = (env ?? {}) as Record<string, unknown>;
        return bound(req, { ...e, flockRole: role });
      })
    : bound;
  const server = createAdaptorServer({ fetch: fetchFn }) as Server;
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
