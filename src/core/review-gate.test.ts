// review gate (owner → checker): hermetic op-level checks.
// The gate is enforced in the ops layer (queue/ops, not prompts):
//  - task_gate: active owner task + a SEPARATE checker pod; one transaction
//    sets the gate and enqueues the checker's review task;
//  - while the gate is pending, task_done/task_handoff refuse the task;
//  - only the designated checker pod (or the operator) may issue a verdict;
//  - verdict pass closes the task as done (finished); reject sends it back
//    to queued for rework (re-gate later).
// Run: node dist/core/review-gate.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { openStore, getTask, pendingGate, setTaskStatus } from './store.js';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-rv-'));
const db = openStore(home);
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  for (const role of ['dev', 'rev', 'other']) {
    raw.prepare("INSERT OR IGNORE INTO pods(id, role, dir, state, created_at) VALUES (?, ?, ?, 'live', ?)")
      .run('pod_' + role, role, path.join(home, 'pods', role), new Date().toISOString());
  }
  raw.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES ('t_main', 'main work', 'dev', 'active', ?)").run(new Date().toISOString());
  raw.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES ('t_free', 'no gate', 'dev', 'active', ?)").run(new Date().toISOString());
  raw.close();
}

const { apply } = await import('./ops.js');
const as = (role: string) => ({ store: db, ticks: {}, caller: { kind: 'pod', role } } as never);
const operator = { store: db, ticks: {} } as never;

// 1) owner sets the gate (own-pod op); gate + review task in one shot
{
  const r = (await apply({ type: 'task_gate', id: 't_main', checker: 'rev' }, as('dev'))) as { task: { gate: string }; reviewTask: { id: string; pod_role: string; status: string; body: string | null } };
  assert.ok(pendingGate(r.task), 'gate must be pending after task_gate');
  assert.ok(r.task.gate, 'gate json present on the task');
  assert.strictEqual(r.reviewTask.pod_role, 'rev');
  assert.strictEqual(r.reviewTask.status, 'queued');
  assert.match(r.reviewTask.body ?? '', /task verdict t_main (pass|reject)/);
}

// 2) double gate is refused
{
  let denied = false;
  try { await apply({ type: 'task_gate', id: 't_main', checker: 'rev' }, as('dev')); } catch (e) { denied = /already has a pending gate/.test(String((e as Error).message)); }
  assert.ok(denied, 'second task_gate on a gatted task is refused');
}

// 3) owner cannot close the gatted task (done OR handoff) — the gate is in the ops, not the prompt
for (const op of [
  { type: 'task_done', id: 't_main', reason: 'finished' },
  { type: 'task_handoff', id: 't_main', to: 'rev' },
] as const) {
  let refused = false;
  try { await apply({ ...op }, as('dev')); } catch (e) { refused = /review gate/.test(String((e as Error).message)); }
  assert.ok(refused, `owner ${op.type} on a gatted task must be refused`);
}
// the operator cannot bypass either (the verdict is the only close path)
let opRefused = false;
try { await apply({ type: 'task_done', id: 't_main', reason: 'finished' }, operator); } catch (e) { opRefused = /review gate/.test(String((e as Error).message)); }
assert.ok(opRefused, 'operator task_done on a gatted task is refused too');
// non-terminal reports stay allowed (hot-potato: the owner still holds it)
const blocked = (await apply({ type: 'task_blocked', id: 't_main', reason: 'waiting' }, as('dev'))) as { status: string };
assert.strictEqual(blocked.status, 'blocked', 'blocked/needs are non-terminal and not gated');
setTaskStatus(db, 't_main', 'active', { reason: 'back' });

// 4) verdict: a non-checker pod cannot vote
let denied = false;
try { await apply({ type: 'task_verdict', id: 't_main', verdict: 'pass' }, as('other')); } catch (e) { denied = /checker pod/.test(String((e as Error).message)); }
assert.ok(denied, 'a non-checker pod cannot issue the verdict');

// 5) checker rejects: the task goes back to queued, the gate is cleared with the verdict
{
  const t = (await apply({ type: 'task_verdict', id: 't_main', verdict: 'reject', reason: 'no tests' }, as('rev'))) as { status: string; gate: string | null };
  assert.strictEqual(t.status, 'queued');
  assert.ok(!pendingGate({ gate: t.gate }), 'gate cleared after reject');
  assert.match(t.gate ?? '', /"verdict":"rejected"/);
  assert.match(t.gate ?? '', /no tests/);
}

// 6) rework loop: re-gate (gate is cleared, task active again) then pass
{
  setTaskStatus(db, 't_main', 'active', { reason: 'rework' });
  await apply({ type: 'task_gate', id: 't_main', checker: 'rev' }, as('dev'));
  const t = (await apply({ type: 'task_verdict', id: 't_main', verdict: 'pass' }, as('rev'))) as { status: string; closed: string | null; gate: string | null };
  assert.strictEqual(t.status, 'done');
  assert.match(t.closed ?? '', /"reason":"finished"/);
  assert.match(t.gate ?? '', /"verdict":"passed"/);
  // terminal: a late second done is a no-op, not a re-open
  const again = (await apply({ type: 'task_done', id: 't_main', reason: 'finished' }, as('dev'))) as { status: string };
  assert.strictEqual(again.status, 'done');
}

// 7) a task without a gate closes normally (existing behavior unchanged)
{
  const t = (await apply({ type: 'task_done', id: 't_free', reason: 'finished' }, as('dev'))) as { status: string };
  assert.strictEqual(t.status, 'done');
}

// 8) gate preconditions: not for own pod, not on a non-active task, no unknown checker
{
  let denied = false;
  try { await apply({ type: 'task_gate', id: 't_free', checker: 'dev' }, operator); } catch (e) { denied = /separate pod/.test(String((e as Error).message)); }
  assert.ok(denied, 'checker must be a separate pod');
  let e2 = false;
  try { await apply({ type: 'task_gate', id: 't_free', checker: 'rev' }, operator); } catch (err) { e2 = /active task/.test(String((err as Error).message)); }
  assert.ok(e2, 'gate needs an active task');
  let e3 = false;
  try { await apply({ type: 'task_gate', id: 't_main', checker: 'ghost' }, operator); } catch (err) { e3 = /no pod: ghost|active task/.test(String((err as Error).message)); }
  assert.ok(e3, 'unknown checker pod is refused');
}

fs.rmSync(home, { recursive: true, force: true });
console.log('review-gate.test.js: all checks passed');
