// C6: strict honest resume — a failed resume fails loudly and stays failed;
// fresh is an explicit operator choice (--fresh). Hermetic: no tmux.
// Run: node dist/core/resume.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// 1) the pure decision layer: an invalid token is attention_required (no
// retry_fresh left in the vocabulary)
const { resolveLaunchMode } = await import('./bridge-protocol.js');
const bad = resolveLaunchMode({ resumeToken: 'nope' });
assert.strictEqual(bad.mode, 'error');
assert.strictEqual(bad.recovery, 'attention_required', 'invalid token: attention_required, never retry_fresh');
assert.ok(!('retry_fresh' in (bad as object)), 'retry_fresh is gone from the contract');

// 2) the adapter contract type no longer offers retry_fresh
const { getAdapter } = await import('./runtime-adapter.js');
const piManifest = { name: 't', runtime: 'pi', command: 'pi' };
const adapter = getAdapter(piManifest as never, { home: os.tmpdir(), token: 'x', runnerPath: '/x/runner.js' } as never)!;
assert.strictEqual(adapter.runtime, 'pi');

// 3) probe helpers: codex rollout lookup (real on-disk shape from 5.5)
const { codexRolloutForThread } = await import('./codex-protocol.js');
const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-resume-'));
const sessions = path.join(home, 'sessions');
const dayDir = path.join(sessions, '2026', '10', '4');
fs.mkdirSync(dayDir, { recursive: true });
const thread = '01a105ae-7d32-70b2-81e1-b6407d9d9ec2';
const rollout = path.join(dayDir, `rollout-2026-10-04T11-51-23-${thread}.jsonl`);
fs.writeFileSync(rollout, '[]\n');
assert.strictEqual(codexRolloutForThread(sessions, thread), rollout, 'finds the thread rollout in the date tree');
assert.strictEqual(codexRolloutForThread(sessions, '00000000-0000-7000-8000-000000000000'), null, 'unknown thread: null');
assert.strictEqual(codexRolloutForThread(path.join(home, 'missing'), thread), null, 'missing sessions dir: null');

fs.rmSync(home, { recursive: true, force: true });
console.log('resume: all checks passed');
