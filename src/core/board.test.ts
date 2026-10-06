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
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
// [U8] MUST be imported first: sets FLOCK_HOME to a fresh tmp home BEFORE
// terminal.ts computes TMUX_SESSION — the real pod_spawn test then runs in
// an isolated tmux session, never the operator's.
import { testHome } from './board.test-env.js';
import { TMUX_SESSION } from './terminal.js';
import { DatabaseSync } from 'node:sqlite';
import { openStore, insertEvent, upsertEscalation } from './store.js';
import { createHttp, stopPodSocket } from './http.js';
import type { CoreCtx } from './ops.js';

const home = testHome;
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
  // [U6] operator actions: an active task (Done) and a blocked task (Unblock)
  raw.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES ('t_a', 'active work', 'dev', 'active', ?)").run(new Date().toISOString());
  raw.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES ('t_b', 'blocked work', 'dev', 'blocked', ?)").run(new Date().toISOString());
  raw.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES ('t_c', 'handoff source', 'dev', 'active', ?)").run(new Date().toISOString());
  raw.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES ('t_d', 'cancel target', 'dev', 'queued', ?)").run(new Date().toISOString());
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
  assert.strictEqual(all.tasks.length, 6); // t_q, t_h + the [U6] seeds t_a..t_d
  const q = (await (await get('/api/tasks?status=queued&limit=100')).json()) as { tasks: Array<{ id: string }> };
  assert.strictEqual(q.tasks.length, 2); // t_q + t_d (both queued at this point)
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

  // ---- 8) [U5] read-only panels: fleet / watchdog / messages / pm -----------
  // The four panels are read-only and use EXISTING ops/GET endpoints (no new
  // ops in this task); here we fix the response SHAPES the panels render:
  // fleet_ls → {profiles, pending}; message_list → {to, messages} (unclaimed
  // filter); watchdog_list → {jobs}; GET /api/watchdog → {jobs};
  // GET /api/watchdog/:id/history → {job, history} (404 — honest, not 500);
  // GET /api/pm → {pm: digest with tasks/…/unclaimedMessages, alerts}.

  // 8a) fleet_ls → 200 ok:true with the two sections the panel renders.
  const fl = await postOp({ type: 'fleet_ls' });
  assert.strictEqual(fl.status, 200, 'fleet_ls is a 200');
  const flBody = (await fl.json()) as { ok: boolean; result: { profiles: unknown[]; pending: { handoffs: unknown[]; messages: unknown[] } } };
  assert.strictEqual(flBody.ok, true, 'fleet_ls ok:true');
  assert.ok(Array.isArray(flBody.result.profiles), 'fleet_ls result.profiles is an array');
  assert.ok(Array.isArray(flBody.result.pending.handoffs), 'fleet_ls pending.handoffs is an array');
  assert.ok(Array.isArray(flBody.result.pending.messages), 'fleet_ls pending.messages is an array');

  // 8b) message_list → 200 ok:true with {to, messages}; the unclaimed filter
  //     narrows the list. A message is delivered to a live pod inbox (dev is
  //     live in this store), so the operator sees it unclaimed first.
  const ms = await postOp({ type: 'message_send', to: 'dev', text: 'board.test [U5] inbox row' });
  assert.strictEqual(ms.status, 200, 'message_send (seed) is a 200');
  const ml = await postOp({ type: 'message_list', to: 'dev' });
  assert.strictEqual(ml.status, 200, 'message_list is a 200');
  const mlBody = (await ml.json()) as { ok: boolean; result: { to: string; messages: { text: string; claimed: number }[] } };
  assert.strictEqual(mlBody.ok, true, 'message_list ok:true');
  assert.strictEqual(mlBody.result.to, 'dev', 'message_list scoped to the role');
  assert.ok(mlBody.result.messages.some((m) => m.text === 'board.test [U5] inbox row' && m.claimed === 0), 'the sent message is listed, unclaimed');
  const mlUn = await postOp({ type: 'message_list', to: 'dev', unclaimed: true });
  assert.strictEqual(mlUn.status, 200, 'message_list unclaimed is a 200');
  const mlUnBody = (await mlUn.json()) as { result: { messages: { claimed: number }[] } };
  assert.ok(mlUnBody.result.messages.length >= 1, 'unclaimed filter still returns the fresh message');
  assert.ok(mlUnBody.result.messages.every((m) => m.claimed === 0), 'unclaimed filter excludes claimed rows');

  // 8c) watchdog_list op → 200 {jobs}; GET /api/watchdog → {jobs} (same rows).
  const wd = await postOp({ type: 'watchdog_register', policy: 'timer', target: 'dev', spec: { afterSeconds: 3600 } });
  assert.strictEqual(wd.status, 200, 'watchdog_register (seed) is a 200');
  const wdList = await postOp({ type: 'watchdog_list' });
  assert.strictEqual(wdList.status, 200, 'watchdog_list is a 200');
  const wdBody = (await wdList.json()) as { ok: boolean; result: { jobs: { id: string; policy: string; target_pod: string; state: string }[] } };
  assert.strictEqual(wdBody.ok, true, 'watchdog_list ok:true');
  const job = wdBody.result.jobs.find((j) => j.policy === 'timer' && j.target_pod === 'dev');
  assert.ok(job, 'the registered timer job is listed');
  assert.strictEqual(job!.state, 'active', 'the fresh job is active');
  const wdGet = await get('/api/watchdog');
  assert.strictEqual(wdGet.status, 200, 'GET /api/watchdog is a 200');
  const wdGetBody = (await wdGet.json()) as { jobs: { id: string }[] };
  assert.ok(Array.isArray(wdGetBody.jobs) && wdGetBody.jobs.some((j) => j.id === job!.id), 'GET /api/watchdog lists the same job');

  // 8d) GET /api/watchdog/:id/history → 200 {job, history}; a nonexistent id
  //     is an honest 404 (not a 500).
  const wh = await get(`/api/watchdog/${job!.id}/history`);
  assert.strictEqual(wh.status, 200, 'watchdog history is a 200');
  const whBody = (await wh.json()) as { job: { id: string }; history: unknown[] };
  assert.strictEqual(whBody.job.id, job!.id, 'history carries the job');
  assert.ok(Array.isArray(whBody.history), 'history is an array');
  const wh404 = await get('/api/watchdog/wd_none/history');
  assert.strictEqual(wh404.status, 404, 'nonexistent watchdog id is a 404, not a 500');
  const wh404Body = (await wh404.json()) as { error: string };
  assert.ok(typeof wh404Body.error === 'string' && wh404Body.error.length > 0, 'the 404 carries an error message');

  // 8e) GET /api/pm → 200 with the digest (tasks counts + … + unclaimedMessages)
  //     and the alerts array (same health_alerts the Health panel shows).
  const pm = await get('/api/pm');
  assert.strictEqual(pm.status, 200, 'GET /api/pm is a 200');
  const pmBody = (await pm.json()) as { pm: { at: string; tasks: Record<string, number>; openTasks: unknown[]; unclaimedMessages: unknown[] }; alerts: unknown[] };
  assert.ok(pmBody.pm, '/api/pm has a pm digest');
  assert.ok(typeof pmBody.pm.at === 'string', 'digest.at is set');
  assert.ok(typeof pmBody.pm.tasks === 'object' && pmBody.pm.tasks !== null, 'digest.tasks is the status counts');
  assert.ok(Array.isArray(pmBody.pm.openTasks), 'digest.openTasks is an array');
  assert.ok(Array.isArray(pmBody.pm.unclaimedMessages), 'digest.unclaimedMessages is an array');
  assert.ok(Array.isArray(pmBody.alerts), '/api/pm carries the alerts array');
  const pmCounts = Object.values(pmBody.pm.tasks).reduce((a, b) => a + b, 0);
  assert.ok(pmCounts > 0, 'the seeded tasks appear in the digest counts');

  // ---- 9) [U6] operator actions through the op path (the board buttons) ---
  // The Tasks/Pods panels call EXISTING ops through act() → POST /api/ops;
  // here we fix the mutation results the panels re-render after re-fetch:
  // task_done → done (closure), task_unblock → queued, task_cancel →
  // cancelled, task_handoff → done(handed-off) + successor at {to},
  // pod_close → state closed. Wrong id → ok:false with an honest error
  // (not a 500; the core keeps serving).
  const statusOf = async (id: string): Promise<{ status: string; closed: string | null }> => {
    const r = await postOp({ type: 'task_list', limit: 500 });
    const body = (await r.json()) as { result: { tasks: { id: string; status: string; closed: string | null }[] } };
    const t = body.result.tasks.find((x) => x.id === id);
    assert.ok(t, `task ${id} is listed`);
    return { status: t!.status, closed: t!.closed };
  };

  // 9a) task_done {id, reason:'finished'} → ok:true + the task is done.
  const done = await postOp({ type: 'task_done', id: 't_a', reason: 'finished' });
  assert.strictEqual(done.status, 200, 'task_done is a 200');
  const doneBody = (await done.json()) as { ok: boolean };
  assert.strictEqual(doneBody.ok, true, 'task_done ok:true');
  const stA = await statusOf('t_a');
  assert.strictEqual(stA.status, 'done', 'task t_a is done after task_done');
  assert.strictEqual(JSON.parse(stA.closed!).reason, 'finished', 'the closure reason is persisted');

  // 9b) task_done on a nonexistent id → ok:false with an honest error
  //     (404, not a 500) and the core keeps serving afterwards.
  const doneBad = await postOp({ type: 'task_done', id: 't_none', reason: 'finished' });
  assert.strictEqual(doneBad.status, 404, 'wrong id is a 404, not a 500');
  const doneBadBody = (await doneBad.json()) as { ok: boolean; error: string };
  assert.strictEqual(doneBadBody.ok, false, 'wrong id is ok:false');
  assert.ok(doneBadBody.error, 'the error is honest (carries a message)');
  const alive = await postOp({ type: 'task_list' });
  assert.strictEqual(alive.status, 200, 'the core keeps serving after the error');

  // 9c) task_unblock on a blocked task → queued.
  const unblk = await postOp({ type: 'task_unblock', id: 't_b' });
  assert.strictEqual(unblk.status, 200, 'task_unblock is a 200');
  const unblkBody = (await unblk.json()) as { ok: boolean };
  assert.strictEqual(unblkBody.ok, true, 'task_unblock ok:true');
  assert.strictEqual((await statusOf('t_b')).status, 'queued', 'blocked → queued');

  // 9d) task_handoff {id, to} → the source closes handed-off + a successor
  //     task appears at the target role (rev exists in this store).
  const ho = await postOp({ type: 'task_handoff', id: 't_c', to: 'rev' });
  assert.strictEqual(ho.status, 200, 'task_handoff is a 200');
  const hoBody = (await ho.json()) as { ok: boolean; result: { next: { id: string; pod_role: string } } };
  assert.strictEqual(hoBody.ok, true, 'task_handoff ok:true');
  assert.strictEqual(hoBody.result.next.pod_role, 'rev', 'the successor is at the target role');
  const stC = await statusOf('t_c');
  assert.strictEqual(stC.status, 'done', 'the handoff source is closed');
  assert.strictEqual(JSON.parse(stC.closed!).reason, 'handed-off', 'closed with reason handed-off');
  assert.strictEqual(JSON.parse(stC.closed!).target, 'rev', 'closed.target names the successor pod');
  const succ = await statusOf(hoBody.result.next.id);
  assert.ok(succ.status, 'the successor task exists (visible after re-fetch)');

  // 9e) task_cancel → cancelled.
  const canc = await postOp({ type: 'task_cancel', id: 't_d' });
  assert.strictEqual(canc.status, 200, 'task_cancel is a 200');
  const cancBody = (await canc.json()) as { ok: boolean };
  assert.strictEqual(cancBody.ok, true, 'task_cancel ok:true');
  assert.strictEqual((await statusOf('t_d')).status, 'cancelled', 'task t_d is cancelled');

  // 9f) pod_close {role} → state closed (kill-window is a no-op here — no
  //     tmux; the store state is what the Pods panel re-renders).
  const pclose = await postOp({ type: 'pod_close', role: 'review' });
  assert.strictEqual(pclose.status, 200, 'pod_close is a 200');
  const pcloseBody = (await pclose.json()) as { ok: boolean };
  assert.strictEqual(pcloseBody.ok, true, 'pod_close ok:true');
  const pods = await get('/api/pods');
  const podsBody = (await pods.json()) as { pods: { role: string; state: string }[] };
  assert.strictEqual(podsBody.pods.find((p) => p.role === 'review')!.state, 'closed', 'pod review is closed');

  // ---- 10) [U7] `flock board` CLI — URL + token-hint + honest --open -------
  // (паттерн bin-тестов mcp.test.ts: спавн dist/bin.js, сверка stdout)
  const binJs = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'bin.js');
  const boardEnv = { ...process.env, FLOCK_PORT: '7499', FLOCK_HOME: home };
  const boardOut = spawnSync(process.execPath, [binJs, 'board'], { env: boardEnv, encoding: 'utf8' });
  assert.strictEqual(boardOut.status, 0, 'flock board exits 0');
  assert.ok(boardOut.stdout.includes('http://127.0.0.1:7499/board'), 'stdout has the board URL from FLOCK_PORT');
  assert.ok(/login:/.test(boardOut.stdout) && boardOut.stdout.includes('token'), 'stdout has the token hint');
  assert.ok(boardOut.stdout.includes('board --token'), 'the hint points to flock board --token');
  // --token: изолированный home без core — честное сообщение, exit 0
  const boardTok = spawnSync(process.execPath, [binJs, 'board', '--token'], { env: boardEnv, encoding: 'utf8' });
  assert.strictEqual(boardTok.status, 0, 'flock board --token exits 0 without a running core');
  assert.ok(boardTok.stdout.includes('token:'), '--token prints the token line');
  // --open без GUI (headless: нет DISPLAY/WAYLAND_DISPLAY) — honest fallback:
  // exit 0, заметка «GUI не найден», без падения
  const boardOpen = spawnSync(process.execPath, [binJs, 'board', '--open'], {
    env: { ...boardEnv, DISPLAY: '', WAYLAND_DISPLAY: '' },
    encoding: 'utf8',
  });
  assert.strictEqual(boardOpen.status, 0, '--open does not fail without a GUI');
  assert.ok(/GUI не найден/.test(boardOpen.stdout), '--open headless prints an honest fallback note');
  assert.ok(boardOpen.stdout.includes('http://127.0.0.1:7499/board'), 'the URL is printed anyway (open it manually)');
}

// ---- 11) [U8] creation: New task (task_add) / New pod (pod_spawn) — the
//         op path the board's forms use (same as [U6] actions) ----

// 11a) task_add {role, title} on an existing pod → ok:true + task queued
const ta = await postOp({ type: 'task_add', role: 'dev', title: 'u8 new task' });
assert.strictEqual(ta.status, 200);
const taBody = (await ta.json()) as { ok: boolean; result: { id: string; status: string; pod_role: string } };
assert.strictEqual(taBody.ok, true, `task_add ok: ${JSON.stringify(taBody)}`);
assert.strictEqual(taBody.result.status, 'queued');
assert.strictEqual(taBody.result.pod_role, 'dev');
const tl11 = await postOp({ type: 'task_list', limit: 500 });
const tl11Body = (await tl11.json()) as { ok: boolean; result: { tasks: Array<{ id: string; status: string }> } };
assert.strictEqual(tl11.status, 200);
const added = tl11Body.result.tasks.find((t) => t.id === taBody.result.id);
assert.ok(added && added.status === 'queued', 'task_add: the task is visible in task_list (queued)');

// 11b) task_add on a non-existent pod → 404 ok:false «no pod» — core is alive
const taBad = await postOp({ type: 'task_add', role: 'ghost', title: 'x' });
assert.strictEqual(taBad.status, 404);
const taBadBody = (await taBad.json()) as { ok: boolean; error?: string };
assert.strictEqual(taBadBody.ok, false);
assert.ok(/no pod/.test(taBadBody.error ?? ''), `no-pod error text: ${JSON.stringify(taBadBody)}`);
const tl11b = await postOp({ type: 'task_list', limit: 5 });
assert.strictEqual(tl11b.status, 200, 'core is alive after a 404 task_add');

// 11c) pod_spawn {role, cmd} (plain cmd pod, no LLM) → ok:true + pod in the
//      store, state not closed. REAL spawn: isolated tmux session
//      (FLOCK_HOME → testHome, see board.test-env.js).
const ps = await postOp({ type: 'pod_spawn', role: 'u8p', cmd: 'sleep 3600' });
assert.strictEqual(ps.status, 200);
const psBody = (await ps.json()) as { ok: boolean; error?: string };
assert.strictEqual(psBody.ok, true, `pod_spawn ok: ${JSON.stringify(psBody)}`);
const pods11 = await get('/api/pods');
const pods11Body = (await pods11.json()) as { pods: Array<{ role: string; state: string }> };
const u8p = pods11Body.pods.find((p) => p.role === 'u8p');
assert.ok(u8p, 'pod_spawn: the pod is in /api/pods');
assert.notStrictEqual(u8p.state, 'closed', `pod state not closed: ${u8p.state}`);

// 11d) pod_spawn again on the live role → 409 ok:false «already» (honest)
const ps2 = await postOp({ type: 'pod_spawn', role: 'u8p', cmd: 'sleep 3600' });
assert.strictEqual(ps2.status, 409);
const ps2Body = (await ps2.json()) as { ok: boolean; error?: string };
assert.strictEqual(ps2Body.ok, false);
assert.ok(/already/.test(ps2Body.error ?? ''), `already error text: ${JSON.stringify(ps2Body)}`);

// [U9] teardown note: the isolated tmux session is killed at the very end
// (after §12) — both spawned cmd pods (u8p, u8t) live in it. The u8p
// pod-local unix socket is closed here (startPodSocket on spawn keeps the
// event loop alive; stopPodSocket — the same core path pod_close uses).
stopPodSocket(path.join(home, 'pods', 'u8p'));

// ---- 12) [U9] terminal: pod_capture (read-only, the board's Terminal) ----

// 12a) pod that echoes a marker into the pane → capture contains it.
//      REAL spawn (isolated tmux session, board.test-env.js).
const ps12 = await postOp({ type: 'pod_spawn', role: 'u8t', cmd: "bash -c 'echo FLOCK_BOARD_TERM_OK; sleep 300'" });
assert.strictEqual(ps12.status, 200);
const ps12Body = (await ps12.json()) as { ok: boolean; error?: string };
assert.strictEqual(ps12Body.ok, true, `pod_spawn u8t ok: ${JSON.stringify(ps12Body)}`);
await new Promise((r) => setTimeout(r, 700)); // окно поднялось, echo отработал
const cap = await postOp({ type: 'pod_capture', role: 'u8t', lines: 50 });
assert.strictEqual(cap.status, 200);
const capBody = (await cap.json()) as { ok: boolean; result: { role: string; text: string } };
assert.strictEqual(capBody.ok, true, `pod_capture ok: ${JSON.stringify(capBody).slice(0, 200)}`);
assert.ok(capBody.result.text.includes('FLOCK_BOARD_TERM_OK'), 'capture contains the pane marker line');

// 12b) closed pod → 404 «no live pod» (honest, not 500)
const capC = await postOp({ type: 'pod_capture', role: 'rev', lines: 50 }); // pod_rev seeded closed
assert.strictEqual(capC.status, 404);
const capCBody = (await capC.json()) as { ok: boolean; error?: string };
assert.strictEqual(capCBody.ok, false);
assert.ok(/no live pod/.test(capCBody.error ?? ''), `closed-pod capture error: ${JSON.stringify(capCBody)}`);

// 12c) non-existent pod → 404 ok:false
const capG = await postOp({ type: 'pod_capture', role: 'ghost9', lines: 50 });
assert.strictEqual(capG.status, 404);
const capGBody = (await capG.json()) as { ok: boolean; error?: string };
assert.strictEqual(capGBody.ok, false);
assert.ok(/no live pod/.test(capGBody.error ?? ''), `missing-pod capture error: ${JSON.stringify(capGBody)}`);

// teardown: the isolated tmux session (u8p: sleep 3600, u8t: sleep 300)
// + the u8t pod-local socket (keeps the event loop alive)
spawnSync('tmux', ['kill-session', '-t', TMUX_SESSION], { stdio: 'ignore' });
stopPodSocket(path.join(home, 'pods', 'u8t'));

fs.rmSync(home, { recursive: true, force: true });
console.log('board.test.js: all checks passed');
