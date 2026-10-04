// C12: content layer — packs (filesystem-canonical, assembly), workspace
// refs, plugins list parsing (pi list output).
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { listPacks, readPackMeta, buildPackBundle, validatePackName } from './packs.js';
import { readWorkspace, resolveWorkspaceRef, workspacePath } from './workspace.js';
import { parsePiList } from './plugins.js';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-content-'));
try {
  // ---- packs -----------------------------------------------------------
  const packs = path.join(tmp, 'packs');
  fs.mkdirSync(path.join(packs, 'style'), { recursive: true });
  fs.writeFileSync(path.join(packs, 'style', 'pack.json'), JSON.stringify({ id: 'style', files: ['rules.md', 'nested/notes.md'] }));
  fs.mkdirSync(path.join(packs, 'style', 'nested'), { recursive: true });
  fs.writeFileSync(path.join(packs, 'style', 'rules.md'), 'пиши на русском\n');
  fs.writeFileSync(path.join(packs, 'style', 'nested/notes.md'), 'коммиты — конвенциональные\n');

  assert.strictEqual(validatePackName('style'), null, 'good name');
  assert.ok(validatePackName('Style'), 'capital rejected');
  assert.ok(validatePackName('a/b'), 'slash rejected');

  const ls = listPacks(tmp);
  assert.deepStrictEqual(ls, [{ name: 'style', files: 2 }], 'listPacks');

  const meta = readPackMeta(tmp, 'style');
  assert.strictEqual(meta.id, 'style');
  assert.deepStrictEqual(meta.files, ['rules.md', 'nested/notes.md']);

  const bundle = buildPackBundle(tmp, 'style');
  assert.ok(bundle.startsWith('# pack: style'), 'bundle header');
  assert.ok(bundle.includes('## rules.md'), 'file section');
  assert.ok(bundle.includes('## nested/notes.md'), 'nested file section');
  assert.ok(bundle.includes('пиши на русском'), 'file content');

  // missing file = visible placeholder, not a silent shrink
  fs.rmSync(path.join(packs, 'style', 'nested'), { recursive: true });
  fs.writeFileSync(path.join(packs, 'style', 'pack.json'), JSON.stringify({ id: 'style', files: ['rules.md', 'gone.md'] }));
  const bundle2 = buildPackBundle(tmp, 'style');
  assert.ok(bundle2.includes('(MISSING FILE: gone.md)'), 'missing file placeholder');

  // .. traversal is a broken pack (loud), not a read outside the dir
  fs.writeFileSync(path.join(packs, 'style', 'pack.json'), JSON.stringify({ id: 'style', files: ['../outside.md'] }));
  assert.throws(() => readPackMeta(tmp, 'style'), /escapes the pack dir/, 'traversal rejected');

  // non-pack dir (no pack.json) is skipped, not an error; a broken pack
  // (bad files) is skipped too — list is tolerant, reads are loud
  fs.mkdirSync(path.join(packs, 'stray'), { recursive: true });
  fs.writeFileSync(path.join(packs, 'style', 'pack.json'), JSON.stringify({ id: 'style', files: ['rules.md'] }));
  assert.deepStrictEqual(listPacks(tmp), [{ name: 'style', files: 1 }], 'stray skipped, broken pack skipped, good pack listed');

  // unknown pack = loud
  assert.throws(() => buildPackBundle(tmp, 'nope'), /cannot read pack\.json/, 'unknown pack loud');

  // ---- workspace ---------------------------------------------------------
  assert.deepStrictEqual(readWorkspace(tmp), { repos: {} }, 'missing workspace = empty');

  const ws = { root: '/home/u/Code', repos: { flock: '/home/u/Code/PROJECTS/flock', web: '/home/u/Code/web' }, knowledge: '/home/u/notes' };
  fs.writeFileSync(workspacePath(tmp), JSON.stringify(ws));
  assert.deepStrictEqual(readWorkspace(tmp), ws, 'workspace round-trip');

  assert.strictEqual(resolveWorkspaceRef(tmp, 'ws:flock'), '/home/u/Code/PROJECTS/flock', 'ws:<name>');
  assert.strictEqual(resolveWorkspaceRef(tmp, 'ws:root'), '/home/u/Code', 'ws:root');
  assert.throws(() => resolveWorkspaceRef(tmp, 'ws:nope'), /workspace repo not found/, 'unknown repo loud');
  assert.throws(() => resolveWorkspaceRef(tmp, '/abs/path'), /not a workspace ref/, 'plain path is not a ref');

  // bad JSON = loud (a broken declaration must fail, not silently shrink)
  fs.writeFileSync(workspacePath(tmp), '{not json');
  assert.throws(() => readWorkspace(tmp), /not valid JSON/, 'bad json loud');

  // ---- plugins (pi list output) -------------------------------------------
  const sample = `User packages:
  git:github.com/user/one
    /home/u/.pi/agent/git/github.com/user/one
  git:github.com/user/two (filtered)
    /home/u/.pi/agent/git/github.com/user/two
`;
  const entries = parsePiList(sample);
  assert.strictEqual(entries.length, 2, 'two entries');
  assert.deepStrictEqual(entries[0], { source: 'git:github.com/user/one', path: '/home/u/.pi/agent/git/github.com/user/one', kind: 'user' }, 'entry 1');
  assert.strictEqual(entries[1].source, 'git:github.com/user/two', 'filtered marker stripped from source');
  assert.strictEqual(entries[1].filtered, true, 'filtered preserved');

  const builtin = `Builtin extensions:
  builtin:web
  builtin:browser
`;
  const be = parsePiList(builtin);
  assert.strictEqual(be.length, 2, 'builtin entries');
  assert.strictEqual(be[0].kind, 'builtin', 'builtin kind');
  assert.strictEqual(be[0].source, 'builtin:web');
  assert.strictEqual(be[0].path, undefined, 'no path line = no path');

  console.log('content.test.ts: all checks passed');
} finally {
  fs.rmSync(tmp, { recursive: true, force: true });
}
