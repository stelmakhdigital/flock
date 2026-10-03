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

fs.rmSync(home, { recursive: true, force: true });
console.log('wf: all checks passed');
