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
  mergeTreeCheck,
  squashMerge,
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

// 7) S1 squash merge (5.4b): clean squash -> one flock(<task>): commit on
// base, wip history NOT in main; conflict -> dry-run detects it, the real
// merge fails AND the repo is left clean (reset --hard after the pre-check);
// dirty base repo is refused (operator work protected)
{
  const tmp2 = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-squash-'));
  const b = path.join(tmp2, 'base');
  const w = path.join(tmp2, 'wt');
  fs.mkdirSync(b, { recursive: true });
  await git(b, 'init', '-q', '-b', 'main');
  await git(b, 'config', 'user.email', 't@t');
  await git(b, 'config', 'user.name', 't');
  fs.writeFileSync(path.join(b, 'a.txt'), 'l1\nl2\nl3\n');
  await git(b, 'add', '.');
  await git(b, 'commit', '-q', '-m', 'base');
  await worktreeAttach(b, w, 'flock/sq', 'main');
  fs.writeFileSync(path.join(w, 'a.txt'), 'l1\nCHANGED\nl3\n');
  fs.writeFileSync(path.join(w, 'b.txt'), 'new\n');
  await git(w, 'add', '.');
  await git(w, 'commit', '-q', '-m', 'wip1');
  await git(w, 'commit', '-q', '--allow-empty', '-m', 'wip2');

  const dry = await mergeTreeCheck(b, 'main', 'flock/sq');
  assert.ok(dry.clean, 'dry-run: clean when base did not move');
  const sm = await squashMerge(b, 'flock/sq', 'main', 'flock(t1): test squash', w);
  assert.ok(sm.ok, 'squash merge ok');
  const log = (await git(b, 'log', '--oneline', '-3')).split('\n');
  assert.strictEqual(log[0].endsWith('flock(t1): test squash'), true, 'one squash commit with the flock(task) message');
  assert.strictEqual(log.length, 2, 'wip commits did NOT land in main');
  assert.strictEqual(fs.readFileSync(path.join(b, 'a.txt'), 'utf8'), 'l1\nCHANGED\nl3\n', 'squashed content applied');
  assert.ok(fs.existsSync(path.join(b, 'b.txt')), 'new file applied');
  // the branch was advanced onto the squash commit: no add/add trap on the
  // next merge, the worktree is clean and in sync with base
  const ahead = (await git(b, 'rev-list', '--count', 'main..flock/sq')).split('\n')[0];
  assert.strictEqual(ahead, '0', 'branch advanced (ahead=0) after the squash');
  assert.strictEqual(await git(w, 'status', '--porcelain', '--untracked-files=no'), '', 'worktree clean after the branch advance');

  // conflict: the branch gets NEW work on line 2, main moves with a
  // conflicting change on the same line
  fs.writeFileSync(path.join(w, 'a.txt'), 'l1\nBR-CHANGED\nl3\n');
  await git(w, 'add', '.');
  await git(w, 'commit', '-q', '-m', 'new work on line 2');
  fs.writeFileSync(path.join(b, 'a.txt'), 'l1\nMAIN\nl3\n');
  await git(b, 'add', '.');
  await git(b, 'commit', '-q', '-m', 'main moved');
  const dry2 = await mergeTreeCheck(b, 'main', 'flock/sq');
  assert.ok(!dry2.clean, 'dry-run: conflict detected after base moved');
  assert.match(dry2.info, /CONFLICT/i, 'conflict info names the conflict');
  const sm2 = await squashMerge(b, 'flock/sq', 'main', 'flock(t2): should conflict');
  assert.ok(!sm2.ok, 'conflicted squash fails');
  assert.strictEqual(await git(b, 'status', '--porcelain', '--untracked-files=no'), '', 'repo left CLEAN after aborted squash (reset --hard)');

  // dirty base repo is refused even when the merge would be clean
  const w2 = path.join(tmp2, 'wt2');
  await worktreeAttach(b, w2, 'flock/sq2', 'main');
  fs.writeFileSync(path.join(w2, 'c.txt'), 'x\n');
  await git(w2, 'add', '.');
  await git(w2, 'commit', '-q', '-m', 'wip');
  fs.appendFileSync(path.join(b, 'a.txt'), 'operator-uncommitted\n');
  const sm3 = await squashMerge(b, 'flock/sq2', 'main', 'flock(t3): dirty base');
  assert.ok(!sm3.ok, 'dirty base refused');
  assert.match(sm3.error ?? '', /uncommitted/, 'refusal names the operator protection');
  assert.match(await git(b, 'status', '--porcelain', '--untracked-files=no'), /a\.txt/, 'operator work untouched');
  fs.rmSync(tmp2, { recursive: true, force: true });
}

// 8) S2 quality gate (5.4b): runQualityGate — green/red/timeout
{
  const { runQualityGate } = await import('./ops.js');
  const g = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-gate-'));
  const okG = await runQualityGate(g, 'echo fine && exit 0', 5000);
  assert.ok(okG.ok && okG.code === 0, 'green gate passes');
  const badG = await runQualityGate(g, 'echo boom >&2 && exit 3', 5000);
  assert.ok(!badG.ok && badG.code === 3, 'red gate: exit 3');
  assert.match(badG.tail, /boom/, 'red gate keeps the log tail');
  const slowG = await runQualityGate(g, 'sleep 5', 300);
  assert.ok(!slowG.ok && slowG.timedOut, 'timeout detected');
  fs.rmSync(g, { recursive: true, force: true });
}

console.log('gitops: S1+S2 checks passed');
