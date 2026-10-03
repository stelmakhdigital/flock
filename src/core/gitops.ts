// gitops — thin git wrapper for per-pod worktrees (stage 5.1).
// A worktree pod gets its own checkout + branch (flock/<role>); the task
// result is a merge candidate (S0: auto fast-forward only, everything else
// is a human button). All ops are short-lived execFile calls; git's own
// guards (dirty tree, non-ff) are the safety net.
import { execFile } from 'node:child_process';
import path from 'node:path';
import { promisify } from 'node:util';
import fs from 'node:fs';

const execFileP = promisify(execFile);

export class GitError extends Error {
  constructor(message: string, public code?: number) {
    super(message);
  }
}

export async function git(repo: string, ...args: string[]): Promise<string> {
  try {
    const raw = await execFileP('git', ['-C', repo, ...args], { timeout: 15_000 });
    return raw.stdout.replace(/\n$/, '');
  } catch (e) {
    const err = e as { code?: number; stderr?: string; message?: string };
    const raw = String(err.stderr ?? err.message ?? e);
    const fatal = raw.split('\n').filter(Boolean).find((l) => l.startsWith('fatal:')) ?? raw.trim().split('\n').pop() ?? '';
    throw new GitError(`git ${args[0]}: ${fatal.trim()}`, err.code);
  }
}

export function isGitRepo(p: string): boolean {
  try {
    return fs.statSync(p).isDirectory() && fs.existsSync(path.join(p, '.git'));
  } catch {
    return false;
  }
}
// a worktree's .git is a file pointing to the main repo; the main checkout
// has a .git directory — both are "git workdirs", but only the latter has
// .git as a directory
export function isGitWorkdir(p: string): boolean {
  try {
    return fs.existsSync(path.join(p, '.git'));
  } catch {
    return false;
  }
}

export function branchName(role: string): string {
  return `flock/${role}`;
}

// Attach (or create) the pod's worktree. Idempotent:
//   worktree exists         -> leave it (the worktree IS the pod's memory)
//   branch exists, no wt    -> `worktree add` on the existing branch
//   neither                 -> `worktree add -b <branch> <wt> <base|HEAD>`
export async function worktreeAttach(repo: string, wtPath: string, branch: string, baseRef?: string): Promise<{ created: boolean; branch: string }> {
  if (isGitWorkdir(wtPath)) return { created: false, branch };
  fs.mkdirSync(wtPath, { recursive: true });
  const branches = await git(repo, 'branch', '--list', branch);
  const hasBranch = branches.split('\n').some((l) => l.trim().replace(/^[*+]/, '').trim() === branch);
  if (hasBranch) {
    await git(repo, 'worktree', 'add', wtPath, branch);
  } else {
    await git(repo, 'worktree', 'add', '-b', branch, wtPath, baseRef ?? 'HEAD');
  }
  // pod infrastructure files (context, cli shim, socket) live inside the
  // checkout — keep them out of the agent's git status (untracked is fine:
  // S0's dirty check ignores untracked)
  const ignore = path.join(wtPath, '.gitignore');
  if (!fs.existsSync(ignore)) {
    fs.writeFileSync(ignore, '/AGENTS.md\n/CLAUDE.md\n/.flock-cli/\n/.pi/\n/.claude/\n/core.sock\n');
  }
  return { created: true, branch };
}

export async function worktreeRemove(repo: string, wtPath: string): Promise<void> {
  if (!fs.existsSync(wtPath)) return;
  try {
    await git(repo, 'worktree', 'remove', '--force', wtPath);
  } catch (e) {
    // pruned if the path is already gone from git's view
    await git(repo, 'worktree', 'prune').catch(() => {});
    if (!fs.existsSync(wtPath)) return;
    throw e;
  }
}

export interface WtStatus {
  dirty: boolean;
  ahead: number; // branch commits not in base
  behind: number; // base commits not in branch
  head: string | null;
}

// branch vs base: ahead = commits the merge would bring, behind = base moved
// (a non-zero behind breaks the S0 fast-forward precondition). dirty counts
// TRACKED changes only (--untracked-files=no): pod infrastructure (AGENTS.md,
// .flock-cli) and agent drafts are untracked by design and must not block
// the merge — uncommitted work stays in the worktree for the next task.
export async function worktreeStatus(repo: string, wtPath: string, branch: string, baseRef: string): Promise<WtStatus> {
  const dirty = (await git(wtPath, 'status', '--porcelain', '--untracked-files=no')).length > 0;
  const head = (await git(wtPath, 'rev-parse', '--short', 'HEAD')).split('\n')[0] ?? null;
  const revList = async (spec: string): Promise<number> =>
    Number((await git(repo, 'rev-list', '--count', spec)) || 0);
  const [ahead, behind] = await Promise.all([
    revList(`${baseRef}..${branch}`),
    revList(`${branch}..${baseRef}`),
  ]);
  return { dirty, ahead, behind, head };
}

// S0 merge: fast-forward only, into the base branch. Preconditions keep the
// risk at zero: the base branch must be checked out at the base repo (we
// never move a ref under a different checkout) and git's own dirty-tree
// guard protects the user's work. Everything else -> skip with a reason
// (the branch stays a merge candidate for a human).
export async function ffMerge(repo: string, branch: string, baseRef: string): Promise<{ ok: boolean; error?: string }> {
  try {
    const headRef = (await git(repo, 'rev-parse', '--abbrev-ref', 'HEAD')).split('\n')[0];
    const probe = await git(repo, 'rev-parse', '--symbolic', '--quiet', '--verify', baseRef).catch(() => '');
    const baseBranch = (probe.split('\n')[0] || '').trim();
    if (!baseBranch) {
      if (baseRef === 'HEAD') throw new GitError('base is HEAD (detached or default) — spawn with --base <branch>');
      throw new GitError(`base ${baseRef} not found`);
    }
    const baseName = baseBranch.split('/').pop()!;
    if (headRef !== baseName) {
      throw new GitError(`base branch ${baseName} not checked out (HEAD is ${headRef})`);
    }
    await git(repo, 'merge', '--ff-only', branch);
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  }
}

export async function resolveBaseRef(repo: string, baseRef: string): Promise<string> {
  return (await git(repo, 'rev-parse', '--verify', baseRef)).split('\n')[0];
}

// the branch the base checkout currently has checked out (S0 default target)
export async function currentBranch(repo: string): Promise<string> {
  const r = await git(repo, 'rev-parse', '--abbrev-ref', 'HEAD');
  const b = r.split('\n')[0];
  return b === 'HEAD' ? 'HEAD' : b; // detached -> 'HEAD' (S0 will refuse honestly)
}
