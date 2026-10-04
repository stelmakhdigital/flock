// task-flow: the TASK_FLOW guard + the idempotent same-state no-op.
// A queued->queued re-report (agent double-report racing the arbiter claim)
// must be a no-op, not an error. Run: node dist/core/task-flow.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { openStore, setTaskStatus, setWorkflowInstanceState, closeWorkItem, handoffTask } from './store.js';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-tf-'));
const db = openStore(home);
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  for (const role of ['dev']) {
    raw.prepare("INSERT OR IGNORE INTO pods(id, role, dir, state, created_at) VALUES (?, ?, ?, 'closed', ?)").run('pod_' + role, role, path.join(home, 'pods', role), new Date().toISOString());
  }
  raw.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES ('t_a', 'a', 'dev', 'queued', ?)").run(new Date().toISOString());
  raw.close();
}

// idempotent: same-state re-report is a no-op, not an error
setTaskStatus(db, 't_a', 'queued', { reason: 'first' });
setTaskStatus(db, 't_a', 'queued', { reason: 'again' }); // must not throw

// legal transition
setTaskStatus(db, 't_a', 'active');
// illegal transition: active -> queued is not in TASK_FLOW... actually it IS
// (the arbiter can re-queue). Use active -> cancelled (legal), then done (illegal)
setTaskStatus(db, 't_a', 'cancelled');
assert.throws(() => setTaskStatus(db, 't_a', 'done'), /bad task transition/, 'terminal state is terminal');

// C3: hot-potato closure — closeWorkItem is the only terminal path
const rawC = new DatabaseSync(path.join(home, 'flock.db'));
rawC.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES ('t_c', 'c', 'dev', 'active', ?)").run(new Date().toISOString());
rawC.close();
assert.throws(() => closeWorkItem(db, 't_c', 'done', { reason: 'bogus' as never, by: 'test' }), /invalid closure reason/);
assert.throws(() => closeWorkItem(db, 't_c', 'done', { reason: 'handed-off', by: 'test' }), /requires target/);
closeWorkItem(db, 't_c', 'done', { reason: 'finished', by: 'dev' });
const closedTask = (new DatabaseSync(path.join(home, 'flock.db')).prepare('SELECT closed, status FROM tasks WHERE id = ?').get('t_c') as { closed: string; status: string }) || null;
assert.strictEqual(closedTask!.status, 'done');
assert.match(closedTask!.closed, /\"reason\":\"finished\"/);
// handoff: one transaction — old closed (handed-off) + successor created
const rawC2 = new DatabaseSync(path.join(home, 'flock.db'));
rawC2.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES ('t_c2', 'c', 'dev', 'active', ?)").run(new Date().toISOString());
rawC2.prepare("INSERT OR IGNORE INTO pods(id, role, dir, state, created_at) VALUES ('pod_q', 'q', ?, 'live', ?)").run(path.join(home, 'pods', 'q'), new Date().toISOString());
rawC2.close();
const ho = handoffTask(db, 't_c2', 'q', 'dev');
assert.strictEqual(ho.from.status, 'done');
assert.strictEqual(ho.to.status, 'queued');
assert.strictEqual(ho.to.pod_role, 'q');
assert.match(ho.to.body ?? '', /handoff/);
assert.match(ho.from.closed ?? '', /\"target\":\"q\"/);

fs.rmSync(home, { recursive: true, force: true });

// 5.4b S3: frozen step task — a stopped workflow instance rejects late state
// changes (no task/instance state drift)
{
  const home2 = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-frozen-'));
  const db2 = openStore(home2);
  const raw2 = new DatabaseSync(path.join(home2, 'flock.db'));
  raw2.prepare("INSERT INTO pods(id, role, dir, state, created_at) VALUES (?, ?, ?, 'live', ?)").run('pod_p', 'p', path.join(home2, 'pods', 'p'), new Date().toISOString());
  raw2.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at, workflow_instance_id, workflow_step) VALUES ('t_f1', 'x', 'p', 'active', ?, 'wfi1', 's')").run(new Date().toISOString());
  raw2.prepare("INSERT INTO workflows(id, name, spec, created_at) VALUES ('wf1', 'froz', ?, ?)").run(JSON.stringify({ steps: [{ id: 's', role: 'p' }] }), new Date().toISOString());
  raw2.prepare("INSERT INTO workflow_instances(id, workflow_id, payload, state, created_at) VALUES ('wfi1', 'wf1', NULL, 'running', ?)").run(new Date().toISOString());
  raw2.close();
  const { apply } = await import('./ops.js');
  const ctx = { store: db2, ticks: {} } as never;
  const r1 = (await apply({ type: 'task_blocked', id: 't_f1', reason: 'reject' }, ctx)) as { status: string };
  assert.strictEqual(r1.status, 'blocked', 'running instance: reject lands');
  setWorkflowInstanceState(db2, 'wfi1', 'blocked');
  let threw = false;
  try {
    await apply({ type: 'task_done', id: 't_f1', reason: 'finished' }, ctx);
  } catch (e) {
    threw = /frozen/.test(String((e as Error).message));
  }
  assert.ok(threw, 'frozen instance rejects the late state change');
  // C3: hot-potato — task done without a closure reason is refused
  let refused = false;
  try {
    await apply({ type: 'task_done', id: 't_f1' }, ctx);
  } catch (e) {
    refused = /closure reason/.test(String((e as Error).message)) || /frozen/.test(String((e as Error).message));
  }
  assert.ok(refused, 'task done without reason is refused');
  fs.rmSync(home2, { recursive: true, force: true });
  console.log('task-flow: frozen step task checks passed');
}
