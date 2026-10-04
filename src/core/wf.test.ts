// C7: the scribe model — no priority/retry/TTL knobs; stuck detection is
// the watchdog's job. The DAG frontier (5.4d) stays: scribe and DAG are
// orthogonal (scribe = who closes/records, DAG = who is eligible).
// Run: node dist/core/wf.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import * as storemod from './store.js';
const { openStore, insertTask, oldestQueuedTask, setTaskStatus, getTask, getWorkflowInstance } = storemod;
import { advanceWorkflow } from './ops.js';
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

// ---- queue ordering: FIFO (no priority knob in C7) -------------------------
{
  insertTask(store, { id: 't1', title: 'first', body: null, podRole: 'dev' });
  insertTask(store, { id: 't2', title: 'second', body: null, podRole: 'dev' });
  assert.strictEqual(oldestQueuedTask(store, 'dev')!.id, 't1', 'FIFO by created_at (ties broken by rowid)');
}

// ---- failure: the scribe records and stops (no retry budget) --------------
// workflow: one step; blocking it must block the instance immediately
{
  const r = raw();
  r.prepare("INSERT INTO workflows(id, name, spec, created_at) VALUES ('wf1', 'fail-wf', ?, ?)").run(
    JSON.stringify({ steps: [{ id: 's1', role: 'dev' }] }),
    new Date().toISOString(),
  );
  r.prepare("INSERT INTO workflow_instances(id, workflow_id, payload, state, created_at) VALUES ('wfi1', 'wf1', NULL, 'running', ?)").run(new Date().toISOString());
  r.prepare("INSERT INTO tasks(id, title, body, pod_role, status, created_at, workflow_instance_id, workflow_step) VALUES ('w1', 'step1', NULL, 'dev', 'active', ?, 'wfi1', 's1')").run(new Date().toISOString());
  r.close();

  setTaskStatus(store, 'w1', 'blocked', { reason: 'first failure' });
  advanceWorkflow(ctx, 'w1');
  assert.strictEqual(getWorkflowInstance(store, 'wfi1')!.state, 'blocked', 'C7: a failed step blocks the instance (no auto-retry)');
  const others = raw().prepare("SELECT COUNT(*) AS n FROM tasks WHERE workflow_instance_id = 'wfi1' AND id != 'w1'").get() as { n: number };
  assert.strictEqual(others.n, 0, 'no retried task was enqueued');
  assert.strictEqual(getTask(store, 'w1')!.closed === undefined || !String(getTask(store, 'w1')!.closed ?? '').includes('retry'), true, 'no retry metadata');
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
  r.prepare("INSERT INTO workflow_instances(id, workflow_id, payload, state, created_at) VALUES ('wfid', 'wfd', NULL, 'running', ?)").run(new Date().toISOString());
  r.prepare("INSERT INTO tasks(id, title, body, pod_role, status, created_at, workflow_instance_id, workflow_step) VALUES ('da', 'a', NULL, 'dev', 'active', ?, 'wfid', 'a')").run(new Date().toISOString());
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
  // C7: the economy knobs are gone — retry/timeoutMin/priority are not part
  // of the step contract anymore (they are simply ignored at the spec level,
  // validation no longer mentions them)
  const ok = (await runDef([{ id: 'a', role: 'dev' }, { id: 'b', role: 'dev', deps: ['a'] }])) as { name: string };
  assert.strictEqual(ok.name, 'dagx', 'valid DAG accepted');
}

// ---- DAG start: the frontier (all no-dep steps) is enqueued at once --------
{
  const { OP_REGISTRY } = await import('./ops.js');
  const res = (await OP_REGISTRY.workflow_start.run({ name: 'dagx' }, ctx)) as { instance: { id: string; state: string } };
  const started = raw().prepare("SELECT workflow_step FROM tasks WHERE workflow_instance_id = ? AND status = 'queued'").all(res.instance.id).map((x) => x.workflow_step);
  assert.deepStrictEqual(started, ['a'], 'at start only the no-dep step is queued');
}

fs.rmSync(home, { recursive: true, force: true });
console.log('wf: all checks passed');
