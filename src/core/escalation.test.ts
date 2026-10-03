// 5.4c durable escalation ladder — hermetic: the trigger that was
// fire-and-forget is now a BDD row that survives a pm-less core, walks
// open -> escalated -> operator alert, auto-resolves when healed, and is
// acknowledged by the operator. Plus: worktree GC keeps UNMERGED branches.
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import * as store from './store.js';
import { openEscalation, runEscalationTick, escOptsFromEnv } from './escalation.js';
import { runRetentionSweep } from './retention.js';
import { worktreeAttach } from './gitops.js';
import { execFile as execFileCb } from 'node:child_process';
import { promisify } from 'node:util';
const execFile = promisify(execFileCb);
const git = (dir: string, ...args: string[]) =>
  execFile('git', ['-C', dir, ...args], { env: { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null' } }).then((r) => (r.stdout as string).trim());
import type { CoreCtx } from './ops.js';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-esc-'));
const storeDb = store.openStore(home);
const events: Record<string, unknown>[] = [];
const ctx = {
  store: storeDb,
  ticks: { register: () => {} },
  startedAt: store.nowIso(),
  emit: (e: Record<string, unknown>) => { events.push(e); },
} as unknown as CoreCtx;

// 1) open: pm not live -> the trigger is DURABLE (a row exists before any
//    delivery attempt) and the tick escalates straight to the operator
openEscalation(ctx, { key: 'pod:dev:crashed', kind: 'pod_crashed', detail: 'под dev: runner умер', severity: 'critical' });
let rows = store.listEscalations(storeDb, true);
assert.strictEqual(rows.length, 1, 'durable row exists');
assert.strictEqual(rows[0].state, 'open');
assert.ok(rows[0].id.startsWith('esc_'), 'BDD id');

await runEscalationTick(ctx);
rows = store.listEscalations(storeDb, true);
assert.strictEqual(rows[0].state, 'escalated', 'no live pm -> operator rung');
assert.ok(events.some((e) => e.type === 'escalation_escalated'), 'escalation event emitted (audit)');
const alert = storeDb.db.prepare('SELECT * FROM health_alerts WHERE pod_role = ? AND kind = ?').get('escalation', 'ladder') as { note: string } | undefined;
assert.ok(alert, 'operator health alert is visible');

// 2) dedupe: a second crash of the same pod absorbs into the same row
openEscalation(ctx, { key: 'pod:dev:crashed', kind: 'pod_crashed', detail: 'под dev: runner умер (again)' });
rows = store.listEscalations(storeDb, true);
assert.strictEqual(rows.length, 1, 'no duplicate row for the same key');
assert.match(rows[0].subject, /again/, 'audit note refreshed');

// 3) heal: the pod run is alive again -> auto-resolved
store.openPod(storeDb, { id: 'p_dev', role: 'dev', dir: path.join(home, 'pods', 'dev'), terminalTarget: 'none', model: null });
store.insertRun(storeDb, { id: 'run_esc1', podRole: 'dev', pid: null, meta: { kind: 'test' } });
await runEscalationTick(ctx);
rows = store.listEscalations(storeDb, false);
assert.strictEqual(rows.find((r) => r.key === 'pod:dev:crashed')!.state, 'resolved');
assert.match(rows.find((r) => r.key === 'pod:dev:crashed')!.resolved_reason!, /alive/);

// 4) task_blocked: heals when the task leaves blocked
openEscalation(ctx, { key: 'task:t_esc2', kind: 'task_blocked', detail: 'таск t_esc2 blocked' });
store.insertTask(storeDb, { id: 't_esc2', title: 'esc', body: null, podRole: 'dev' });
store.setTaskStatus(storeDb, 't_esc2', 'active');
store.setTaskStatus(storeDb, 't_esc2', 'blocked');
await runEscalationTick(ctx);
assert.strictEqual(store.getEscalation(storeDb, store.listEscalations(storeDb, true)[0].id)!.state, 'escalated');
store.setTaskStatus(storeDb, 't_esc2', 'done');
await runEscalationTick(ctx);
const esc2 = store.listEscalations(storeDb, false).find((r) => r.key === 'task:t_esc2')!;
assert.strictEqual(esc2.state, 'resolved', 'auto-resolved when the task healed');
assert.match(esc2.resolved_reason!, /done/);

// 5) operator ack closes the ladder (no more reminders)
openEscalation(ctx, { key: 'wf:wfi_esc3', kind: 'workflow_blocked', detail: 'instance wfi_esc3 blocked' });
store.insertWorkflow(storeDb, { id: 'wf_esc3', name: 'esc3', spec: '{"steps":[]}' });
store.insertWorkflowInstance(storeDb, { id: 'wfi_esc3', workflowId: 'wf_esc3', payload: '[]' });
store.setWorkflowInstanceState(storeDb, 'wfi_esc3', 'blocked');
await runEscalationTick(ctx);
const esc3 = store.listEscalations(storeDb, true)[0];
assert.strictEqual(esc3.state, 'escalated');
store.setEscalationState(storeDb, esc3.id, 'acknowledged', { resolvedReason: 'operator ack' });
assert.strictEqual(store.listEscalations(storeDb, true).length, 0, 'acknowledged = no longer active');
assert.ok(store.listEscalations(storeDb, false).some((r) => r.id === esc3.id && r.state === 'acknowledged'), 'audit row kept');

// 6) pm-silence timer: pm_notified older than the timeout escalates
const r6 = store.upsertEscalation(storeDb, { key: 'task:t_silent', kind: 'task_blocked', subject: 'silent pm' });
const silentIso = new Date(Date.now() - (escOptsFromEnv().pmTimeoutMs + 1000)).toISOString();
store.setEscalationState(storeDb, r6.id, 'pm_notified', { pmNotifiedAt: silentIso, attempts: 1 });
store.insertTask(storeDb, { id: 't_silent', title: 'silent', body: null, podRole: 'dev' });
store.setTaskStatus(storeDb, 't_silent', 'active');
store.setTaskStatus(storeDb, 't_silent', 'blocked');
await runEscalationTick(ctx);
assert.strictEqual(store.getEscalation(storeDb, r6.id)!.state, 'escalated', 'pm silent past timeout -> operator');

storeDb.db.close();
fs.rmSync(home, { recursive: true, force: true });

// ---- worktree GC: unmerged branches are KEPT, merged worktrees cleaned ----
const home2 = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-gc-'));
const repo = path.join(home2, 'repo');
fs.mkdirSync(repo, { recursive: true });
await git(repo, 'init', '-q', '-b', 'main');
await git(repo, 'config', 'user.email', 't@t');
await git(repo, 'config', 'user.name', 't');
fs.writeFileSync(path.join(repo, 'base.txt'), 'base');
await git(repo, 'add', '.');
await git(repo, 'commit', '-q', '-m', 'base');
const storeDb2 = store.openStore(home2);
const ctx2 = { store: storeDb2, ticks: { register: () => {} }, startedAt: store.nowIso() } as unknown as CoreCtx;
// pod A: closed, UNMERGED commit on its branch -> branch must be KEPT
store.openPod(storeDb2, { id: 'p_gca', role: 'gca', dir: path.join(home2, 'pods', 'gca'), terminalTarget: 'none', model: null, agent: 'bash', mergePolicy: 'squash' });
store.setPodState(storeDb2, 'gca', 'closed');
store.setPodRepo(storeDb2, 'gca', repo, 'main', `flock/gca`);
await worktreeAttach(repo, path.join(home2, 'pods', 'gca', 'work'), 'flock/gca', 'main');
fs.writeFileSync(path.join(home2, 'pods', 'gca', 'work', 'work.txt'), 'unmerged work');
await git(path.join(home2, 'pods', 'gca', 'work'), 'add', '.');
await git(path.join(home2, 'pods', 'gca', 'work'), 'commit', '-q', '-m', 'unmerged');
// pod B: closed, MERGED (ahead=0) -> branch deleted, worktree gone
store.openPod(storeDb2, { id: 'p_gcb', role: 'gcb', dir: path.join(home2, 'pods', 'gcb'), terminalTarget: 'none', model: null, agent: 'bash', mergePolicy: 'squash' });
store.setPodState(storeDb2, 'gcb', 'closed');
store.setPodRepo(storeDb2, 'gcb', repo, 'main', `flock/gcb`);
await worktreeAttach(repo, path.join(home2, 'pods', 'gcb', 'work'), 'flock/gcb', 'main');

const report = await runRetentionSweep({ store: storeDb2, home: home2 });
assert.ok(report.gcWorktrees.includes('gcb'), 'closed-pod worktree removed (re-spawn re-attaches)');
assert.ok(!fs.existsSync(path.join(home2, 'pods', 'gcb', 'work')), 'worktree gone from disk');
assert.ok(!fs.existsSync(path.join(home2, 'pods', 'gca', 'work')), 'unmerged pod worktree also removed (branch keeps the work)');
assert.ok(report.gcBranches.some((b) => b.includes('gcb')), 'merged branch (ahead=0) deleted');
assert.ok(report.gcKept.some((k) => k.includes('gca') && k.includes('ahead=1')), 'UNMERGED branch kept with reason');
// the work is intact on the kept branch
const showOut = await git(repo, 'show', 'flock/gca:work.txt');
assert.match(showOut, /unmerged work/);
// re-spawn re-attaches idempotently (the design promise)
await worktreeAttach(repo, path.join(home2, 'pods', 'gca', 'work'), 'flock/gca', 'main');
assert.ok(fs.existsSync(path.join(home2, 'pods', 'gca', 'work', 'work.txt')), 're-attach restores the checkout');

storeDb2.db.close();
fs.rmSync(home2, { recursive: true, force: true });
console.log('escalation+gc.test.ts: passed');
