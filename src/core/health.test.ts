// Hermetic checks for health detection (pure parts). Run: node dist/core/health.test.js
import assert from 'node:assert';
import { parseActivity, detectGate, detectIdle, healthOptsFromEnv } from './health.js';

const opts = healthOptsFromEnv({});
assert.strictEqual(opts.idleMin, 15);
assert.strictEqual(opts.gateDetectMin, 2);
// env overrides
const o2 = healthOptsFromEnv({ FLOCK_HEALTH_IDLE_MIN: '7' });
assert.strictEqual(o2.idleMin, 7);
assert.strictEqual(o2.gateDetectMin, 2);

// parseActivity: skips non-JSON, keeps order
const act = parseActivity('{"a":1}\ngarbage\n{"b":2}\n');
assert.deepStrictEqual(act, [{ a: 1 }, { b: 2 }]);

// detectGate: open dialog = newest unanswered with no answered for the same id after
const now = '2026-10-02T23:00:00.000Z';
assert.strictEqual(detectGate([]), null);
const one = [
  { event: 'ext_dialog_unanswered', id: 'd1', at: now, method: 'select', title: 't' },
];
assert.ok(detectGate(one)?.id === 'd1');
// answered closes it
assert.strictEqual(detectGate([...one, { event: 'ext_dialog_answered', id: 'd1', via: 'operator' }]), null);
// a different id does not close it
assert.ok(detectGate([...one, { event: 'ext_dialog_answered', id: 'other' }])?.id === 'd1');
// a newer open dialog wins
const two = [
  ...one,
  { event: 'ext_dialog_unanswered', id: 'd2', at: now, method: 'confirm', title: 't2' },
];
assert.ok(detectGate(two)?.id === 'd2');
// answered d2 -> d1 is open again (FIFO of the open set)
const closed2 = [...two, { event: 'ext_dialog_answered', id: 'd2', via: 'operator' }];
assert.ok(detectGate(closed2)?.id === 'd1');

// detectIdle: ready + not streaming + lastPrompt old enough
const nowMs = Date.parse(now);
assert.ok(detectIdle({ ready: true, streaming: false, lastPromptAt: '2026-10-02T22:00:00.000Z' }, nowMs, opts));
assert.ok(!detectIdle({ ready: true, streaming: true, lastPromptAt: '2026-10-02T22:00:00.000Z' }, nowMs, opts)); // still working
assert.ok(!detectIdle({ ready: true, streaming: false, lastPromptAt: now }, nowMs, opts)); // fresh
assert.ok(!detectIdle({ ready: false, streaming: false, lastPromptAt: '2026-10-02T22:00:00.000Z' }, nowMs, opts)); // not ready
assert.ok(!detectIdle({ ready: true }, nowMs, opts)); // no lastPrompt

console.log('health: all checks passed');
