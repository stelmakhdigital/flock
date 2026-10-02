// Hermetic checks for runtime-adapter pure logic + managed blocks.
// Run: node dist/core/runtime-adapter.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { resolveTrust, validateResumeToken, resolveLaunchMode } from './runner-protocol.js';
import { mergeManagedBlock } from './runtime-adapter.js';

// ── resolveTrust: posture is authoritative ──
assert.strictEqual(resolveTrust(undefined, undefined), 'approve'); // flock default
assert.strictEqual(resolveTrust('no-approve', 'floor'), 'no-approve'); // configured wins on floor
assert.strictEqual(resolveTrust('no-approve', 'full_bypass'), 'approve'); // full_bypass forces
assert.strictEqual(resolveTrust('approve', 'full_bypass'), 'approve');

// ── validateResumeToken: absolute path, no .., charset, .jsonl ──
assert.deepStrictEqual(validateResumeToken('/home/u/.flock/pods/dev/.pi/sessions/abc_dev.jsonl'), {
  ok: true,
  token: '/home/u/.flock/pods/dev/.pi/sessions/abc_dev.jsonl',
});
assert.strictEqual(validateResumeToken('relative/x.jsonl').ok, false);
assert.strictEqual(validateResumeToken('/a/../b.jsonl').ok, false);
assert.strictEqual(validateResumeToken('').ok, false);
assert.strictEqual(validateResumeToken(null).ok, false);
assert.strictEqual(validateResumeToken('/a/b c.jsonl').ok, false); // space
assert.strictEqual(validateResumeToken('/a/b.json').ok, false); // suffix
assert.strictEqual(validateResumeToken('/' + 'a'.repeat(600) + '.jsonl').ok, false); // too long

// ── resolveLaunchMode: fresh / resume / fork / mutual exclusion ──
assert.deepStrictEqual(resolveLaunchMode({}), { mode: 'fresh' });
const res = resolveLaunchMode({ resumeToken: '/s/f.jsonl' });
assert.deepStrictEqual(res, { mode: 'resume', sessionFile: '/s/f.jsonl' });
const bad = resolveLaunchMode({ resumeToken: 'nope' });
assert.strictEqual(bad.mode, 'error');
assert.strictEqual(bad.recovery, 'retry_fresh');
const fk = resolveLaunchMode({ forkSource: { kind: 'native_id', value: '/s/parent.jsonl' } });
assert.deepStrictEqual(fk, { mode: 'fork', forkRef: '/s/parent.jsonl' });
assert.strictEqual(resolveLaunchMode({ forkSource: { kind: 'name', value: 'x' } }).mode, 'error');
assert.strictEqual(
  resolveLaunchMode({ resumeToken: '/s/f.jsonl', forkSource: { kind: 'native_id', value: '/s/p.jsonl' } }).mode,
  'error',
); // mutual exclusion

// ── mergeManagedBlock: create / replace / idempotent / multi-block / legacy ──
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-mb-'));
try {
  const f = path.join(tmp, 'AGENTS.md');

  // create
  mergeManagedBlock(f, 'b1', 'one');
  assert.ok(fs.readFileSync(f, 'utf8').includes('<!-- BEGIN MANAGED BLOCK: b1 -->\none\n<!-- END MANAGED BLOCK: b1 -->'));

  // idempotent (same content -> file unchanged)
  const before = fs.readFileSync(f, 'utf8');
  mergeManagedBlock(f, 'b1', 'one');
  assert.strictEqual(fs.readFileSync(f, 'utf8'), before);

  // replace (content updated, no duplicate)
  mergeManagedBlock(f, 'b1', 'one-v2');
  let text = fs.readFileSync(f, 'utf8');
  assert.ok(text.includes('one-v2') && !text.includes('\none\n'));
  assert.strictEqual(text.split('BEGIN MANAGED BLOCK: b1').length - 1, 1);

  // second block coexists, user text between blocks survives
  fs.writeFileSync(f, text + '\nuser text here\n');
  mergeManagedBlock(f, 'b2', 'two');
  text = fs.readFileSync(f, 'utf8');
  assert.ok(text.includes('user text here'));
  assert.ok(text.includes('BEGIN MANAGED BLOCK: b2'));
  assert.strictEqual(text.split('BEGIN MANAGED BLOCK: b1').length - 1, 1);
  assert.strictEqual(text.split('BEGIN MANAGED BLOCK: b2').length - 1, 1);

  // replaceBlockIds: legacy block stripped, no residue
  mergeManagedBlock(f, 'b1', 'one-v3', { replaceBlockIds: ['b1-legacy'] });
  text = fs.readFileSync(f, 'utf8');
  assert.ok(!text.includes('b1-legacy'));

  // explicit legacy strip when both present
  fs.writeFileSync(f, '<!-- BEGIN MANAGED BLOCK: old -->\nold\n<!-- END MANAGED BLOCK: old -->\n');
  mergeManagedBlock(f, 'new', 'fresh', { replaceBlockIds: ['old'] });
  text = fs.readFileSync(f, 'utf8');
  assert.ok(text.includes('BEGIN MANAGED BLOCK: new'));
  assert.ok(!text.includes('BEGIN MANAGED BLOCK: old'));
  assert.ok(!text.includes('\nold\n'));
} finally {
  fs.rmSync(tmp, { recursive: true, force: true });
}

console.log('runtime-adapter: all checks passed');
