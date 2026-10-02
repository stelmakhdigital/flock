import { Hono } from 'hono';
import { createNodeWebSocket } from '@hono/node-ws';
import * as store from './store.js';
import { apply, OpError, type CoreCtx } from './ops.js';

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

  app.get('/api/pods', (c) => {
    return c.json({ pods: store.listPods(ctx.store), runs: store.listRuns(ctx.store) });
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
