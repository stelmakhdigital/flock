// Hermetic checks for pm intent validation. Run: node dist/core/pm.test.js
import assert from 'node:assert';
import { validateIntent } from './pm.js';

// whitelisted ops pass
assert.strictEqual(validateIntent({ op: 'task_done', id: 't_1' }), null);
assert.strictEqual(validateIntent({ op: 'task_add', title: 'x' }), null);
assert.strictEqual(validateIntent({ op: 'task_add', title: 'x', pod_role: 'dev' }), null);
assert.strictEqual(validateIntent({ op: 'task_needs', id: 't_1', reason: 'r' }), null);
assert.strictEqual(validateIntent({ op: 'pod_send', role: 'dev', text: 'hi' }), null);
assert.strictEqual(validateIntent({ op: 'pod_relaunch', role: 'dev' }), null);
assert.strictEqual(validateIntent({ op: 'pod_spawn', role: 'new' }), null);
assert.strictEqual(validateIntent({ op: 'pod_close', role: 'dev' }), null);
assert.strictEqual(validateIntent({ op: 'workflow_start', name: 'wf' }), null);
assert.strictEqual(validateIntent({ op: 'task_cancel', id: 't_1' }), null);
assert.strictEqual(validateIntent({ op: 'task_unblock', id: 't_1' }), null);

// not in the whitelist: core ops, pm recursion, unknown
assert.ok(validateIntent({ op: 'pod_relaunch_all', role: 'x' })?.includes('whitelist'));
assert.ok(validateIntent({ op: 'core_down' })?.includes('whitelist'));
assert.ok(validateIntent({ op: 'pm_up' })?.includes('whitelist'));
assert.ok(validateIntent({ op: 'watchdog_register' })?.includes('whitelist'));
assert.ok(validateIntent({})?.includes('whitelist'));

// required fields
assert.ok(validateIntent({ op: 'task_add' })?.includes('title'));
assert.ok(validateIntent({ op: 'task_add', title: '', pod_role: 'dev' })?.includes('title'));
assert.ok(validateIntent({ op: 'task_done' })?.includes('id'));
assert.ok(validateIntent({ op: 'task_needs', id: 't_1' })?.includes('reason'));
assert.ok(validateIntent({ op: 'task_blocked', id: 't_1' })?.includes('reason'));
assert.ok(validateIntent({ op: 'task_cancel', id: 't_1' }) === null); // no reason for cancel
assert.ok(validateIntent({ op: 'pod_send', role: 'dev' })?.includes('text'));
assert.ok(validateIntent({ op: 'pod_send', role: 'DEV!', text: 'x' })?.includes('role'));
assert.ok(validateIntent({ op: 'pod_relaunch', role: 'Bad Role' })?.includes('role'));
assert.ok(validateIntent({ op: 'workflow_start' })?.includes('name'));

console.log('pm: all checks passed');
