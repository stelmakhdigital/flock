// gitops — hermetic checks in a tmp repo (no network). Run: node dist/core/gitops.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import {
  branchName,
  ffMerge,
  isGitRepo,
  isGitWorkdir,
  worktreeAttach,
  worktreeRemove,
  worktreeStatus,
} from './gitops.js';

const execFileP = promisify(execFile);
const git = (dir: string, ...args: string[]) =>
  execFileP('git', ['-C', dir, ...args], { env: { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null' } }).then((r) => r.stdout.trim());

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-gitops-'));
const base = path.join(tmp, 'base');
const wt = path.join(tmp, 'wt-dev');
fs.mkdirSync(base, { recursive: true });
await git(base, 'init', '-q', '-b', 'main');
await git(base, 'config', 'user.email', 't@t');
await git(base, 'config', 'user.name', 't');
fs.writeFileSync(path.join(base, 'a.txt'), '1\n');
await git(base, 'add', '.');
await git(base, 'commit', '-q', '-m', 'base');

assert.ok(isGitRepo(base), 'base is a git repo');
assert.ok(!isGitRepo(tmp), 'plain dir is not a repo');
assert.strictEqual(branchName('dev'), 'flock/dev');

// 1) fresh attach: creates branch + worktree
{
  const r = await worktreeAttach(base, wt, 'flock/dev');
  assert.ok(r.created, 'first attach creates');
  assert.ok(isGitWorkdir(wt), 'worktree materialised');
  const st = await worktreeStatus(base, wt, 'flock/dev', 'main');
  assert.deepStrictEqual({ dirty: st.dirty, ahead: st.ahead, behind: st.behind }, { dirty: false, ahead: 0, behind: 0 });
}

// 2) idempotent re-attach (relaunch): no new branch, worktree kept
{
  fs.writeFileSync(path.join(wt, 'memory.txt'), 'pod memory\n');
  const r = await worktreeAttach(base, wt, 'flock/dev');
  assert.ok(!r.created, 'second attach is a no-op');
  assert.strictEqual(fs.readFileSync(path.join(wt, 'memory.txt'), 'utf8'), 'pod memory\n', 'worktree content survives');
}

// 3) one commit ahead -> ff-merge moves main forward
{
  fs.writeFileSync(path.join(wt, 'a.txt'), '2\n');
  await git(wt, 'add', '.');
  await git(wt, 'commit', '-q', '-m', 'pod work');
  const st = await worktreeStatus(base, wt, 'flock/dev', 'main');
  assert.strictEqual(st.ahead, 1, 'one commit ahead');
  assert.strictEqual(st.behind, 0);
  const m = await ffMerge(base, 'flock/dev', 'main');
  assert.ok(m.ok, `ff merge ok (${m.error ?? ''})`);
  assert.strictEqual(fs.readFileSync(path.join(base, 'a.txt'), 'utf8'), '2\n', 'base moved forward');
  const st2 = await worktreeStatus(base, wt, 'flock/dev', 'main');
  assert.strictEqual(st2.ahead, 0, 'no ahead after merge');
}

// 4) base moved + branch has its own commit -> NOT fast-forward -> refused
{
  // base advances
  fs.writeFileSync(path.join(base, 'b.txt'), 'base work\n');
  await git(base, 'add', '.');
  await git(base, 'commit', '-q', '-m', 'base moves');
  // branch commits too
  fs.writeFileSync(path.join(wt, 'c.txt'), 'branch work\n');
  await git(wt, 'add', '.');
  await git(wt, 'commit', '-q', '-m', 'branch moves');
  const st = await worktreeStatus(base, wt, 'flock/dev', 'main');
  assert.strictEqual(st.ahead, 1);
  assert.strictEqual(st.behind, 1, 'base moved -> behind');
  const m = await ffMerge(base, 'flock/dev', 'main');
  assert.ok(!m.ok, 'diverged history is NOT fast-forward');
  assert.match(m.error ?? '', /ff|non-fast|fast/i);
  // base is untouched
  assert.ok(!fs.existsSync(path.join(base, 'c.txt')), 'refused merge touched nothing');
}

// 5) dirty worktree is reported (tracked change only: untracked files are
// pod infrastructure by design and must NOT block the S0 merge)
{
  fs.writeFileSync(path.join(wt, 'dirty-untracked.txt'), 'x\n');
  const stU = await worktreeStatus(base, wt, 'flock/dev', 'main');
  assert.ok(!stU.dirty, 'untracked-only is not dirty (pod files never block the merge)');
  fs.appendFileSync(path.join(wt, 'a.txt'), 'modified\n');
  const st = await worktreeStatus(base, wt, 'flock/dev', 'main');
  assert.ok(st.dirty, 'tracked change -> dirty');
  fs.rmSync(path.join(wt, 'dirty-untracked.txt'));
}

// 6) worktree remove: checkout gone, branch survives
{
  await worktreeRemove(base, wt);
  assert.ok(!fs.existsSync(wt), 'worktree removed');
  const branches = await git(base, 'branch', '--list', 'flock/dev');
  assert.ok(branches.includes('flock/dev'), 'branch survives purge');
}

fs.rmSync(tmp, { recursive: true, force: true });
console.log('gitops: all checks passed');
