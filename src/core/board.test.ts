// web board (U1 + U2 + minimal health): hermetic HTTP checks.
// createHttp is the same code path the real core serves (no tmux, no ticks).
// Verifies: the shell and its assets are served WITHOUT auth (no data in
// the shell; the token travels as Bearer on data fetches), the new read
// endpoint /api/events shape + auth, and the store shapes the board's
// Pods/Tasks/Health panels render from.
// Run: node dist/core/board.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { openStore, insertEvent, upsertEscalation } from './store.js';
import { createHttp } from './http.js';
import type { CoreCtx } from './ops.js';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-board-'));
const store = openStore(home);
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  raw.prepare("INSERT OR IGNORE INTO pods(id, role, dir, terminal_target, agent, state, created_at) VALUES ('pod_dev', 'dev', ?, 't', 'pi', 'live', ?)")
    .run(path.join(home, 'pods', 'dev'), new Date().toISOString());
  raw.prepare("INSERT OR IGNORE INTO pods(id, role, dir, terminal_target, agent, state, created_at) VALUES ('pod_rev', 'rev', ?, 't', 'bash', 'closed', ?)")
    .run(path.join(home, 'pods', 'rev'), new Date().toISOString());
  // [U4] a live pod whose role belongs to the `conveyor` preset, so the
  // topology_ls live-mark is observable (conveyor.live must contain 'review').
  raw.prepare("INSERT OR IGNORE INTO pods(id, role, dir, terminal_target, agent, state, created_at) VALUES ('pod_review', 'review', ?, 't', 'pi', 'live', ?)")
    .run(path.join(home, 'pods', 'review'), new Date().toISOString());
  raw.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES ('t_q', 'queued work', 'dev', 'queued', ?)").run(new Date().toISOString());
  raw.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at, closed) VALUES ('t_h', 'handed work', 'dev', 'done', ?, ?)")
    .run(new Date().toISOString(), JSON.stringify({ reason: 'handed-off', target: 'rev', at: new Date().toISOString(), by: 'cli' }));
  raw.close();
}
// a few durable events (the /api/events data source)
const id1 = insertEvent(store, { kind: 'pod_spawned', actor: 'operator', subject: 'dev', payload: { role: 'dev' } });
const id2 = insertEvent(store, { kind: 'task_claimed', actor: 'core', subject: 'dev', payload: { taskId: 't_q' } });
assert.ok(id2 > id1);

const ctx: CoreCtx = { store: store as never, ticks: { register() {}, all: () => [] } as never, startedAt: new Date().toISOString(), emit: () => {} } as never;
const app = createHttp(ctx).app;
const token = fs.readFileSync(path.join(home, 'token'), 'utf8').trim();
const get = (p: string, authed = true) =>
  app.fetch(new Request(`http://core${p}`, { headers: authed ? { authorization: `Bearer ${token}` } : {} }));
const postOp = (op: Record<string, unknown>) =>
  app.fetch(new Request('http://core/api/ops', {
    method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify(op),
  }));

// 1) shell + assets: 200 WITHOUT auth (the shell carries no data)
{
  const r = await get('/board', false);
  assert.strictEqual(r.status, 200, 'GET /board must be open (no data in the shell)');
  assert.match(r.headers.get('content-type') ?? '', /text\/html/);
  const html = await r.text();
  assert.match(html, /id="token"/, 'login form marker');
  assert.match(html, /board\.js/);

  const js = await get('/board/assets/board.js', false);
  assert.strictEqual(js.status, 200);
  assert.match(js.headers.get('content-type') ?? '', /text\/javascript/);
  assert.match(await js.text(), /flock web board/);

  const css = await get('/board/assets/board.css', false);
  assert.strictEqual(css.status, 200);
  assert.match(css.headers.get('content-type') ?? '', /text\/css/);

  // path-shape abuse is refused (only board.js / board.css)
  assert.strictEqual((await get('/board/assets/evil.html', false)).status, 404);
  assert.strictEqual((await get('/board/assets/..%2Fstore.js', false)).status, 404);
}

// 2) GET /api/events: shape + auth + since/limit
{
  // without token -> 401 (auth surface is NOT widened)
  const anon = await get('/api/events?since=0&limit=10', false);
  assert.strictEqual(anon.status, 401, '/api/events requires the Bearer token');

  const r = await get('/api/events?since=0&limit=10');
  assert.strictEqual(r.status, 200);
  const body = (await r.json()) as { events: Array<{ id: number; at: string; kind: string; actor: string; subject: string; payload: unknown }> };
  assert.strictEqual(body.events.length, 2);
  for (const ev of body.events) {
    assert.ok(Number.isInteger(ev.id), 'row has integer id');
    assert.ok(ev.at && ev.kind, 'row has at/kind');
    assert.ok('actor' in ev && 'subject' in ev, 'row has actor/subject');
  }
  assert.strictEqual(body.events[0].kind, 'pod_spawned');
  assert.deepStrictEqual(body.events[1].payload, { taskId: 't_q' }, 'payload is parsed JSON');

  // since filter (the nudge-append mechanism)
  const since = await (await get(`/api/events?since=${id1}&limit=10`)).json() as { events: unknown[] };
  assert.strictEqual(since.events.length, 1);
  const limited = await (await get('/api/events?since=0&limit=1')).json() as { events: unknown[] };
  assert.strictEqual(limited.events.length, 1, 'limit is honored');
  // limit cap: 1000 max
  const capped = await (await get('/api/events?since=0&limit=99999')).json() as { events: unknown[] };
  assert.ok(capped.events.length <= 2);
}

// 3) GET /api/pods: the Pods panel shape (role/agent/state, live + closed)
{
  const r = await get('/api/pods');
  assert.strictEqual(r.status, 200);
  const body = (await r.json()) as { pods: Array<{ role: string; agent: string | null; state: string }>; runs: unknown[] };
  const dev = body.pods.find((p) => p.role === 'dev')!;
  const rev = body.pods.find((p) => p.role === 'rev')!;
  assert.strictEqual(dev.state, 'live');
  assert.strictEqual(dev.agent, 'pi');
  assert.strictEqual(rev.state, 'closed');
  assert.ok(Array.isArray(body.runs), 'runs present (run meta drives readiness)');
}

// 4) GET /api/tasks: the Tasks panel shape (status filter, closed.target = handoff)
{
  const all = (await (await get('/api/tasks?limit=100')).json()) as { tasks: Array<{ id: string; status: string; pod_role: string; closed: string | null }> };
  assert.strictEqual(all.tasks.length, 2);
  const q = (await (await get('/api/tasks?status=queued&limit=100')).json()) as { tasks: Array<{ id: string }> };
  assert.strictEqual(q.tasks.length, 1);
  assert.strictEqual(q.tasks[0].id, 't_q');
  const done = (await (await get('/api/tasks?status=done&limit=100')).json()) as { tasks: Array<{ closed: string | null }> };
  const target = JSON.parse(done.tasks[0].closed!) as { target: string };
  assert.strictEqual(target.target, 'rev', 'handoff target is visible via closed.target');
}

// 5) GET /api/health: the minimal Health panel shape (alerts + activeEscalations)
{
  const r = await get('/api/health');
  assert.strictEqual(r.status, 200);
  const body = (await r.json()) as { alerts: unknown[]; opts: unknown; activeEscalations: unknown[] };
  assert.ok(Array.isArray(body.alerts), 'alerts array');
  assert.ok(Array.isArray(body.activeEscalations), 'activeEscalations array');
  assert.ok(body.opts, 'health opts present');
}

// 6) [U3] Escalations: op esc_ls (array shape) + esc_ack (acknowledge + honest
//    error on a missing id — a handled ok:false, NOT an uncaught crash).
{
  // seed an active escalation (state 'open') the way the ladder would
  const { id: escId } = upsertEscalation(store, { key: 'pod:dev:idle', kind: 'idle', subject: 'task t_q stalled' });

  // 6a) esc_ls → 200 ok:true, an ARRAY of escalations with the panel's fields
  const ls = await postOp({ type: 'esc_ls' });
  assert.strictEqual(ls.status, 200, 'esc_ls is a 200');
  const lsBody = (await ls.json()) as { ok: boolean; result: Array<Record<string, unknown>> };
  assert.strictEqual(lsBody.ok, true, 'esc_ls ok:true');
  assert.ok(Array.isArray(lsBody.result), 'esc_ls result is an array');
  const row = lsBody.result.find((e) => e.id === escId);
  assert.ok(row, 'the seeded escalation is listed');
  for (const f of ['id', 'key', 'state', 'kind', 'subject', 'created_at']) {
    assert.ok(f in row!, `escalation row has ${f}`);
  }
  assert.strictEqual(row!.state, 'open', 'freshly seeded escalation is active (open)');

  // 6b) esc_ack on a MISSING id → ok:false with an honest error, and the core
  //     keeps serving (the "не 500 crash" guarantee: a handled refusal, not a
  //     process crash — proven by the follow-up read still answering 200)
  const missing = await postOp({ type: 'esc_ack', id: 'esc_does_not_exist' });
  const missBody = (await missing.json()) as { ok: boolean; error?: string };
  assert.strictEqual(missBody.ok, false, 'esc_ack on missing id is ok:false');
  assert.match(missBody.error ?? '', /not found/i, 'honest error names the cause');
  const alive = await postOp({ type: 'esc_ls' });
  assert.strictEqual(alive.status, 200, 'core still serves ops after the refused ack (no crash)');

  // 6c) esc_ack on the ACTIVE id → ok:true, row flips to acknowledged (durable)
  const ack = await postOp({ type: 'esc_ack', id: escId });
  assert.strictEqual(ack.status, 200, 'esc_ack on an active id is a 200');
  const ackBody = (await ack.json()) as { ok: boolean; result: { state: string } };
  assert.strictEqual(ackBody.ok, true, 'esc_ack ok:true');
  assert.strictEqual(ackBody.result.state, 'acknowledged', 'row transitions to acknowledged');
  const re = (await (await postOp({ type: 'esc_ls' })).json()) as { result: Array<Record<string, unknown>> };
  assert.strictEqual(re.result.find((e) => e.id === escId)?.state, 'acknowledged', 'state persisted');
}

// 7) [U4] structural panels: the new read-op topology_ls (catalog shape +
//    live-mark) plus the existing workflow_ls / campaign_ls the panels render
//    from. topology_ls must be reachable from a POD-scoped caller too.
{
  // 7a) topology_ls → 200 ok:true, an ARRAY of the 4 presets, each with
  //     name/summary/pods + a live-mark; the live pod 'review' marks conveyor.
  const tl = await postOp({ type: 'topology_ls' });
  assert.strictEqual(tl.status, 200, 'topology_ls is a 200');
  const tlBody = (await tl.json()) as { ok: boolean; result: Array<Record<string, unknown>> };
  assert.strictEqual(tlBody.ok, true, 'topology_ls ok:true');
  assert.ok(Array.isArray(tlBody.result), 'topology_ls result is an array');
  assert.strictEqual(tlBody.result.length, 4, 'catalog has the 4 presets');
  for (const f of ['name', 'summary', 'pods', 'live']) {
    assert.ok(f in tlBody.result[0], `topology row has ${f}`);
  }
  const names = tlBody.result.map((t) => t.name as string).sort();
  assert.deepStrictEqual(names, ['adversarial-review', 'conveyor', 'research-team', 'secrets-manager'], 'the 4 preset names');
  const conv = tlBody.result.find((t) => t.name === 'conveyor')!;
  assert.ok(Array.isArray(conv.pods) && (conv.pods as string[]).includes('review'), 'conveyor lists its pods incl. review');
  assert.deepStrictEqual(conv.live, ['review'], 'live pod review marks conveyor (live-mark works)');
  const ar = tlBody.result.find((t) => t.name === 'adversarial-review')!;
  assert.deepStrictEqual(ar.live, [], 'a preset with no live roles has an empty live-mark');

  // 7b) workflow_ls → 200 ok:true with the two arrays the panel renders.
  const wl = await postOp({ type: 'workflow_ls' });
  assert.strictEqual(wl.status, 200, 'workflow_ls is a 200');
  const wlBody = (await wl.json()) as { ok: boolean; result: { workflows: unknown[]; instances: unknown[] } };
  assert.strictEqual(wlBody.ok, true, 'workflow_ls ok:true');
  assert.ok(Array.isArray(wlBody.result.workflows), 'workflow_ls result.workflows is an array');
  assert.ok(Array.isArray(wlBody.result.instances), 'workflow_ls result.instances is an array');

  // 7c) campaign_ls → 200 ok:true with the campaigns array.
  const cl = await postOp({ type: 'campaign_ls' });
  assert.strictEqual(cl.status, 200, 'campaign_ls is a 200');
  const clBody = (await cl.json()) as { ok: boolean; result: { campaigns: unknown[] } };
  assert.strictEqual(clBody.ok, true, 'campaign_ls ok:true');
  assert.ok(Array.isArray(clBody.result.campaigns), 'campaign_ls result.campaigns is an array');

  // 7d) topology_ls under a POD-scoped caller (env.flockRole) → 200 ok:true.
  //     The op is registered with scopes ['operator','pod'], so a pod token
  //     must reach it (unlike operator-only ops such as campaign_pause).
  const podReq = new Request('http://core/api/ops', {
    method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ type: 'topology_ls' }),
  });
  const podRes = await app.fetch(podReq, { flockRole: 'dev' });
  assert.strictEqual(podRes.status, 200, 'topology_ls is reachable from a pod scope');
  const podBody = (await podRes.json()) as { ok: boolean };
  assert.strictEqual(podBody.ok, true, 'topology_ls ok:true from a pod scope');
  // control: an operator-only op under the same pod caller is refused (403),
  // proving the scope narrowing is what 7d exercises, not a blanket allow.
  const opOnly = await app.fetch(new Request('http://core/api/ops', {
    method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ type: 'campaign_pause', id: 'cam_none' }),
  }), { flockRole: 'dev' });
  assert.strictEqual(opOnly.status, 403, 'operator-only op is refused for a pod caller (control)');
}

fs.rmSync(home, { recursive: true, force: true });
console.log('board.test.js: all checks passed');
