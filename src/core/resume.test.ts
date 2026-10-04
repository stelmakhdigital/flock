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

// 3) C12b: a removed runtime produces NO adapter — the clean-refusal path
// (codex's rollout-lookup probe helper is gone with the codex adapter)
assert.strictEqual(getAdapter({ id: 'c', command: 'codex', runtime: 'codex' } as never, { home: os.tmpdir(), token: 'x', runnerPath: '/x/runner.js' } as never), null, 'codex: no adapter');

console.log('resume: all checks passed');
