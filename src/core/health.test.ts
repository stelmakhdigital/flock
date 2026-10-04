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

// detectGate: C10 — the gate is an ext_dialog_auto_denied (the operator
// answer channel is gone; the auto-deny is the observability signal, the
// most recent one wins)
const now = '2026-10-02T23:00:00.000Z';
assert.strictEqual(detectGate([]), null);
const one = [
  { event: 'ext_dialog_auto_denied', id: 'd1', at: now, method: 'select', title: 't' },
];
assert.ok(detectGate(one)?.id === 'd1');
// a newer auto-deny wins
const two = [
  ...one,
  { event: 'ext_dialog_auto_denied', id: 'd2', at: now, method: 'confirm', title: 't2' },
];
assert.ok(detectGate(two)?.id === 'd2');
// unrelated events do not affect the gate
assert.ok(detectGate([...one, { event: 'prompt_sent', id: 'x' }])?.id === 'd1');

// detectIdle: ready + not streaming + lastPrompt old enough
const nowMs = Date.parse(now);
assert.ok(detectIdle({ ready: true, streaming: false, lastPromptAt: '2026-10-02T22:00:00.000Z' }, nowMs, opts));
assert.ok(!detectIdle({ ready: true, streaming: true, lastPromptAt: '2026-10-02T22:00:00.000Z' }, nowMs, opts)); // still working
assert.ok(!detectIdle({ ready: true, streaming: false, lastPromptAt: now }, nowMs, opts)); // fresh
assert.ok(!detectIdle({ ready: false, streaming: false, lastPromptAt: '2026-10-02T22:00:00.000Z' }, nowMs, opts)); // not ready
assert.ok(!detectIdle({ ready: true }, nowMs, opts)); // no lastPrompt

console.log('health: all checks passed');
