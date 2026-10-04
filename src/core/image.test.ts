// C13: agent images — hermetic checks (filesystem, no tmux, no live core).
// Run: node dist/core/image.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { validateImageName, listImages, readImage } from './ops.js';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-img-'));
const home = tmp;

// 1) name validation
assert.strictEqual(validateImageName('known-good'), true);
assert.strictEqual(validateImageName('a.b_c-9'), true);
assert.strictEqual(validateImageName('../evil'), false, 'traversal rejected');
assert.strictEqual(validateImageName(''), false);
assert.strictEqual(validateImageName('a'.repeat(65)), false, 'too long');
assert.strictEqual(validateImageName('has space'), false);

// 2) empty list
assert.deepStrictEqual(listImages(home), []);

// 3) a well-formed image round-trips
{
  const imgDir = path.join(home, 'images', 'dev-good');
  fs.mkdirSync(imgDir, { recursive: true });
  const sessionFile = path.join(imgDir, 'session-dev-good.jsonl');
  fs.writeFileSync(sessionFile, '{"type":"message"}\n');
  const meta = {
    name: 'dev-good',
    sourceRole: 'dev',
    agentId: 'pi',
    manifestId: 'pi',
    profile: null,
    model: 'cat-vllm/qwen3.8-27b-fp8',
    savedAt: '2026-10-04T12:00:00.000Z',
    sessionFile,
    restore: { id: 'pi', command: 'pi', runtime: 'pi', thinking: 'medium' },
  };
  fs.writeFileSync(path.join(imgDir, 'image.json'), JSON.stringify(meta, null, 2));

  const got = readImage(home, 'dev-good');
  assert.strictEqual(got.name, 'dev-good');
  assert.strictEqual(got.sourceRole, 'dev');
  assert.strictEqual(got.restore.thinking, 'medium');
  assert.ok(fs.existsSync(got.sessionFile), 'session copy on disk');

  const rows = listImages(home);
  assert.strictEqual(rows.length, 1);
  assert.strictEqual(rows[0].name, 'dev-good');
  assert.strictEqual(rows[0].agent, 'pi');
  assert.strictEqual(rows[0].source, 'dev');
}

// 4) missing image: loud 404-shape error
assert.throws(() => readImage(home, 'nope'), /no image: nope/);

// 5) broken image (missing session file): loud, ls stays tolerant
{
  const imgDir = path.join(home, 'images', 'broken');
  fs.mkdirSync(imgDir, { recursive: true });
  fs.writeFileSync(path.join(imgDir, 'image.json'), JSON.stringify({
    name: 'broken', sourceRole: 'x', agentId: 'pi', manifestId: 'pi', profile: null,
    model: null, savedAt: '2026-10-04T12:00:00.000Z',
    sessionFile: path.join(imgDir, 'missing.jsonl'),
    restore: { id: 'pi', command: 'pi' },
  }));
}
assert.throws(() => readImage(home, 'broken'), /session file missing/);
assert.strictEqual(listImages(home).length, 1, 'ls skips the unreadable one, still shows the good one');
assert.strictEqual(listImages(home)[0].name, 'dev-good');

// 6) corrupt image.json: loud on read, tolerant in ls
{
  const imgDir = path.join(home, 'images', 'corrupt');
  fs.mkdirSync(imgDir, { recursive: true });
  fs.writeFileSync(path.join(imgDir, 'image.json'), '{not json');
}
assert.throws(() => readImage(home, 'corrupt'), /corrupt image/);
assert.strictEqual(listImages(home).length, 1);
assert.strictEqual(listImages(home)[0].name, 'dev-good');

fs.rmSync(tmp, { recursive: true, force: true });
console.log('image.test.ts: all checks passed');
