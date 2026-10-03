// 5.4a: workflow priority, timeout/TTL, retry — hermetic (no tmux, no network).
// Run: node dist/core/wf.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import * as storemod from './store.js';
const { openStore, insertTask, oldestQueuedTask, setTaskStatus, wfStepAttempts, bumpWfStepAttempts, listWfStepStates, getTask, getWorkflowInstance } = storemod;
import { advanceWorkflow, checkWorkflowTimeouts } from './ops.js';
import type { CoreCtx } from './ops.js';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-wf-'));
const store = openStore(home) as unknown as storemod.Store;
const raw = () => new DatabaseSync(path.join(home, 'flock.db'));

// pods
{
  const r = raw();
  for (const role of ['dev', 'rev']) {
    r.prepare("INSERT INTO pods(id, role, dir, state, created_at) VALUES (?, ?, ?, 'live', ?)").run('pod_' + role, role, path.join(home, 'pods', role), new Date().toISOString());
  }
  r.close();
}

// ctx with a no-op tick/emit
const ctx: CoreCtx = { store: store as never, ticks: { register() {}, all: () => [] } as never, startedAt: new Date().toISOString(), emit: () => {} };

// ---- priority -------------------------------------------------------------
{
  const mk = (id: string, prio: number) => {
    insertTask(store, { id, title: id, body: null, podRole: 'dev', priority: prio });
  };
  mk('t_low', 0);
  mk('t_high', 5);
  mk('t_mid', 2);
  const first = oldestQueuedTask(store, 'dev')!;
  assert.strictEqual(first.id, 't_high', 'highest priority claimed first');
  // same priority: FIFO by created_at
  insertTask(store, { id: 't_old', title: 'old', body: null, podRole: 'rev', priority: 1 });
  // backdate
  const r = raw();
  r.prepare("UPDATE tasks SET created_at = datetime('now', '-5 minutes') WHERE id = 't_old'").run();
  r.close();
  insertTask(store, { id: 't_new', title: 'new', body: null, podRole: 'rev', priority: 1 });
  assert.strictEqual(oldestQueuedTask(store, 'rev')!.id, 't_old', 'same priority: FIFO');
}

// ---- retry budget ---------------------------------------------------------
// workflow: one step with retry=1; blocking it twice must exhaust the budget
{
  const r = raw();
  r.prepare("INSERT INTO workflows(id, name, spec, created_at) VALUES ('wf1', 'retry-wf', ?, ?)").run(
    JSON.stringify({ steps: [{ id: 's1', role: 'dev', retry: 1 }] }),
    new Date().toISOString(),
  );
  r.prepare("INSERT INTO workflow_instances(id, workflow_id, payload, state, created_at, priority) VALUES ('wfi1', 'wf1', NULL, 'running', ?, 0)").run(new Date().toISOString());
  r.prepare("INSERT INTO tasks(id, title, body, pod_role, status, created_at, workflow_instance_id, workflow_step, priority) VALUES ('w1', 'step1', NULL, 'dev', 'active', ?, 'wfi1', 's1', 0)").run(new Date().toISOString());
  r.close();

  assert.strictEqual(wfStepAttempts(store, 'wfi1', 's1'), 0);

  // first failure: attempts 0 < retry 1 -> re-queue (a new active->queued is
  // not a legal transition, so simulate the arbiter's re-claim: the retried
  // step task is a NEW task; here we just verify the budget math and the
  // instance state via advanceWorkflow on a blocked task)
  setTaskStatus(store, 'w1', 'blocked', { reason: 'first failure' });
  advanceWorkflow(ctx, 'w1');
  assert.strictEqual(wfStepAttempts(store, 'wfi1', 's1'), 1, 'attempt counted');
  assert.strictEqual(getWorkflowInstance(store, 'wfi1')!.state, 'running', 'instance stays running on retry');
  // the retry enqueued a fresh task for the same step
  const retried = (raw().prepare("SELECT * FROM tasks WHERE workflow_instance_id = 'wfi1' AND id != 'w1'").all() as { id: string; status: string; workflow_step: string }[]);
  assert.strictEqual(retried.length, 1, 'retried task enqueued');
  assert.strictEqual(retried[0].workflow_step, 's1');

  // exhaust: block the retry task -> attempts 1 = retry 1 -> instance blocked
  const r2 = raw();
  r2.prepare("UPDATE tasks SET status = 'active' WHERE workflow_instance_id = 'wfi1' AND id != 'w1'").run();
  r2.close();
  const retryTaskId = retried[0].id;
  setTaskStatus(store, retryTaskId, 'blocked', { reason: 'second failure' });
  advanceWorkflow(ctx, retryTaskId);
  assert.strictEqual(getWorkflowInstance(store, 'wfi1')!.state, 'blocked', 'instance blocked after budget exhausted');
}

// ---- timeout/TTL ------------------------------------------------------------
// step with timeoutMin=1, claimed 2 minutes ago -> checkWorkflowTimeouts blocks it
{
  const r = raw();
  r.prepare("INSERT INTO workflows(id, name, spec, created_at) VALUES ('wf2', 'timeout-wf', ?, ?)").run(
    JSON.stringify({ steps: [{ id: 't1', role: 'dev', timeoutMin: 1 }] }),
    new Date().toISOString(),
  );
  r.prepare("INSERT INTO workflow_instances(id, workflow_id, payload, state, created_at, priority) VALUES ('wfi2', 'wf2', NULL, 'running', ?, 0)").run(new Date().toISOString());
  r.prepare("INSERT INTO tasks(id, title, body, pod_role, status, created_at, claimed_at, workflow_instance_id, workflow_step, priority) VALUES ('w2', 'slow step', NULL, 'dev', 'active', ?, ?, 'wfi2', 't1', 0)").run(
    new Date(Date.now() - 2 * 60_000).toISOString(),
    new Date(Date.now() - 2 * 60_000).toISOString(),
  );
  r.close();

  checkWorkflowTimeouts(ctx);
  const t = getTask(store, 'w2')!;
  assert.strictEqual(t.status, 'blocked', 'timed-out step task is blocked');
  assert.ok((t.result ?? '').includes('timeout'), 'reason says timeout');
  assert.strictEqual(getWorkflowInstance(store, 'wfi2')!.state, 'blocked', 'instance blocked (no retry budget)');

  // a FRESH task (claimed now) must not be touched
  insertTask(store, { id: 'w3', title: 'fresh', body: null, podRole: 'dev' });
  const r2 = raw();
  r2.prepare("UPDATE tasks SET workflow_instance_id = 'wfi2', workflow_step = 't1', status = 'active', claimed_at = ? WHERE id = 'w3'").run(new Date().toISOString());
  r2.close();
  // unblock the instance first (wfi2 is terminal) — use a fresh instance
  const r3 = raw();
  r3.prepare("UPDATE workflow_instances SET state = 'running' WHERE id = 'wfi2'").run();
  r3.close();
  checkWorkflowTimeouts(ctx);
  assert.strictEqual(getTask(store, 'w3')!.status, 'active', 'fresh task not touched by timeout');
}

// ---- bump helper idempotence ------------------------------------------------
{
  assert.strictEqual(bumpWfStepAttempts(store, 'wfi9', 'x'), 1);
  assert.strictEqual(bumpWfStepAttempts(store, 'wfi9', 'x'), 2);
  const states = listWfStepStates(store, 'wfi9');
  assert.strictEqual(states.length, 1);
  assert.strictEqual(states[0].step, 'x');
  assert.strictEqual(states[0].attempts, 2);
}

// ---- DAG: dependency frontier (5.4d) ---------------------------------------
// diamond: a -> (b, c) -> d. b and c start in PARALLEL (both frontier at
// start after a), d starts only when BOTH b and c are done.
{
  const r = raw();
  r.prepare("INSERT INTO workflows(id, name, spec, created_at) VALUES ('wfd', 'dag-wf', ?, ?)").run(
    JSON.stringify({ steps: [
      { id: 'a', role: 'dev' },
      { id: 'b', role: 'rev', deps: ['a'] },
      { id: 'c', role: 'dev', deps: ['a'] },
      { id: 'd', role: 'rev', deps: ['b', 'c'] },
    ] }),
    new Date().toISOString(),
  );
  r.prepare("INSERT INTO workflow_instances(id, workflow_id, payload, state, created_at, priority) VALUES ('wfid', 'wfd', NULL, 'running', ?, 0)").run(new Date().toISOString());
  r.prepare("INSERT INTO tasks(id, title, body, pod_role, status, created_at, workflow_instance_id, workflow_step, priority) VALUES ('da', 'a', NULL, 'dev', 'active', ?, 'wfid', 'a', 0)").run(new Date().toISOString());
  r.close();

  // a done -> b AND c become ready in the same advance (parallel frontier)
  setTaskStatus(store, 'da', 'done');
  advanceWorkflow(ctx, 'da');
  const tasksAfterA = raw().prepare("SELECT workflow_step FROM tasks WHERE workflow_instance_id = 'wfid' AND status = 'queued'").all().map((x) => x.workflow_step).sort();
  assert.deepStrictEqual(tasksAfterA, ['b', 'c'], 'b and c enqueued in parallel');
  assert.strictEqual(getTask(store, 'da')!.status, 'done');
  const instD = getWorkflowInstance(store, 'wfid')!;
  assert.strictEqual(instD.state, 'running', 'instance still running (d not ready)');

  // b done alone: d is NOT ready (c still pending)
  const tb = raw().prepare("SELECT id FROM tasks WHERE workflow_instance_id = 'wfid' AND workflow_step = 'b' AND status = 'queued'").get() as { id: string };
  setTaskStatus(store, tb.id, 'active');
  setTaskStatus(store, tb.id, 'done');
  advanceWorkflow(ctx, tb.id);
  const queuedMid = raw().prepare("SELECT workflow_step FROM tasks WHERE workflow_instance_id = 'wfid' AND status = 'queued'").all().map((x) => x.workflow_step);
  assert.ok(!queuedMid.includes('d'), 'd not enqueued until c is done too');
  assert.ok(queuedMid.includes('c'), 'c is still in flight (queued, awaiting the arbiter)');

  // c done -> d ready
  const tc = raw().prepare("SELECT id FROM tasks WHERE workflow_instance_id = 'wfid' AND workflow_step = 'c' AND status = 'queued'").get() as { id: string };
  setTaskStatus(store, tc.id, 'active');
  setTaskStatus(store, tc.id, 'done');
  advanceWorkflow(ctx, tc.id);
  const queuedAfterC = raw().prepare("SELECT workflow_step FROM tasks WHERE workflow_instance_id = 'wfid' AND status = 'queued'").all().map((x) => x.workflow_step);
  assert.deepStrictEqual(queuedAfterC, ['d'], 'd enqueued when all deps are done');

  // d done -> instance done
  const td = raw().prepare("SELECT id FROM tasks WHERE workflow_instance_id = 'wfid' AND workflow_step = 'd' AND status = 'queued'").get() as { id: string };
  setTaskStatus(store, td.id, 'active');
  setTaskStatus(store, td.id, 'done');
  advanceWorkflow(ctx, td.id);
  assert.strictEqual(getWorkflowInstance(store, 'wfid')!.state, 'done', 'instance done when the last step is done');
}

// ---- DAG validation: cycle / unknown dep / self-dep are rejected ----------
{
  const { OP_REGISTRY } = await import('./ops.js');
  const runDef = (steps: unknown): Promise<unknown> => Promise.resolve(OP_REGISTRY.workflow_define.run({ name: 'dagx', steps }, ctx));
  await assert.rejects(() => runDef([
    { id: 'a', role: 'dev', deps: ['b'] },
    { id: 'b', role: 'dev', deps: ['a'] },
  ]), /cycle/, 'dependency cycle rejected');
  await assert.rejects(() => runDef([{ id: 'a', role: 'dev', deps: ['nope'] }]), /unknown dep/, 'unknown dep rejected');
  await assert.rejects(() => runDef([{ id: 'a', role: 'dev', deps: ['a'] }]), /itself/, 'self-dep rejected');
  // a valid DAG defines fine
  const ok = (await runDef([{ id: 'a', role: 'dev' }, { id: 'b', role: 'dev', deps: ['a'] }])) as { name: string };
  assert.strictEqual(ok.name, 'dagx', 'valid DAG accepted');
}

// ---- DAG start: the frontier (all no-dep steps) is enqueued at once --------
{
  const { OP_REGISTRY } = await import('./ops.js');
  const res = (await OP_REGISTRY.workflow_start.run({ name: 'dagx', priority: 0 }, ctx)) as { instance: { id: string; state: string } };
  const started = raw().prepare("SELECT workflow_step FROM tasks WHERE workflow_instance_id = ? AND status = 'queued'").all(res.instance.id).map((x) => x.workflow_step);
  assert.deepStrictEqual(started, ['a'], 'at start only the no-dep step is queued');
  // close it so the rest of the suite is not polluted
  const one = raw().prepare("SELECT id FROM tasks WHERE workflow_instance_id = ? LIMIT 1").get(res.instance.id) as { id: string };
  setTaskStatus(store, one.id, 'active');
}

fs.rmSync(home, { recursive: true, force: true });
console.log('wf: all checks passed');
