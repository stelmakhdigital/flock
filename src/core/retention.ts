// retention — the 24h sweep: runs older than N days -> runs_archive,
// activity.jsonl head-trim (a file over the high-water mark is trimmed to
// the tail window), core.log rotated when it grows past the size cap.
// Archive, not delete: the operator keeps an audit trail; deletion is a
// separate explicit op (retention_purge, stage 5.3+ if it earns its keep).
import fs from 'node:fs';
import path from 'node:path';
import * as store from './store.js';
import { seatPaths } from './runner-protocol.js';
import { gitRaw, worktreeRemove } from './gitops.js';

const env = (k: string, dflt: number): number => {
  const v = Number(process.env[k]);
  return Number.isFinite(v) && v > 0 ? v : dflt;
};

function retentionOpts(): {
  runsDays: number;
  activityHighWater: number;
  activityTrimTo: number;
  coreLogBytes: number;
} {
  return {
    runsDays: env('FLOCK_RETENTION_RUNS_DAYS', 14),
    activityHighWater: env('FLOCK_RETENTION_ACTIVITY_LINES', 20_000),
    activityTrimTo: env('FLOCK_RETENTION_ACTIVITY_KEEP', 5_000),
    coreLogBytes: env('FLOCK_RETENTION_CORE_LOG_BYTES', 10 * 1024 * 1024),
  };
}

export interface RetentionReport {
  archivedRuns: number;
  trimmedActivity: string[]; // roles
  coreLogRotated: boolean;
  // worktree GC (5.4c): closed pods' worktrees + dead branches
  gcWorktrees: string[]; // roles whose worktree was removed
  gcBranches: string[]; // branches deleted (merged, ahead=0)
  gcKept: string[]; // closed-pod branches kept (UNMERGED work — merge candidates)
}

export async function runRetentionSweep(ctx: { store: store.Store; home: string }): Promise<RetentionReport> {
  const R = retentionOpts();
  const cutoff = new Date(Date.now() - R.runsDays * 86_400_000).toISOString();
  const archivedRuns = store.archiveOldRuns(ctx.store, cutoff);

  const trimmedActivity: string[] = [];
  // every seat dir on disk (closed pods may be missing from listPods)
  let seatRoles: string[] = [];
  try {
    seatRoles = fs.readdirSync(path.join(ctx.home, 'pods'));
  } catch {
    /* no pods dir */
  }
  for (const role of seatRoles) {
    const p = seatPaths(ctx.home, role);
    try {
      const stat = fs.statSync(p.activityPath);
      if (stat.size < 1024) continue; // tiny: don't touch
      const raw = fs.readFileSync(p.activityPath, 'utf8');
      const lines = raw.split('\n');
      if (lines.length <= R.activityHighWater) continue;
      // head-trim: keep the TAIL (the recent window); the usage cursor file
      // holds a byte offset that may now exceed the file -> ingestUsage
      // resets to 0 on shrink and the dedupe key absorbs the overlap.
      const kept = lines.slice(-R.activityTrimTo).join('\n') + '\n';
      fs.writeFileSync(p.activityPath, kept);
      trimmedActivity.push(role);
    } catch {
      /* missing/unreadable: skip */
    }
  }

  // core.log: rotate (rename to .1) when over the cap. The core holds the
  // file open; a renamed fd keeps writing to the .1 file until the next
  // restart reopens core.log — acceptable for a 24h sweep (the log is for
  // post-mortems, not real-time), and it caps the disk growth to 2x the cap.
  let coreLogRotated = false;
  const logPath = path.join(ctx.home, 'core.log');
  try {
    const stat = fs.statSync(logPath);
    if (stat.size > R.coreLogBytes) {
      fs.renameSync(logPath, logPath + '.1');
      coreLogRotated = true;
    }
  } catch {
    /* no log yet */
  }

  return {
    archivedRuns, trimmedActivity, coreLogRotated,
    ...(await gcWorktreesAndBranches(ctx)),
  };
}

// ---- worktree GC (5.4c) --------------------------------------------------------
// Closed worktree pods leak checkouts and branches: `pod close --purge` is
// explicit, but auto-GC belongs here. Rules (work is never lost):
//  - worktree on disk of a CLOSED pod -> worktreeRemove (the branch keeps
//    the commits; re-spawn re-attaches the checkout idempotently)
//  - closed-pod branch with ahead=0 (fully merged) -> branch deleted
//  - closed-pod branch with ahead>0 (UNMERGED) -> KEPT: it is a merge
//    candidate (the arbiter re-queue path depends on it)
//  - git worktree prune on every known repo (stale git metadata)
async function gcWorktreesAndBranches(ctx: { store: store.Store; home: string }): Promise<{
  gcWorktrees: string[];
  gcBranches: string[];
  gcKept: string[];
}> {
  const gcWorktrees: string[] = [];
  const gcBranches: string[] = [];
  const gcKept: string[] = [];
  const repos = new Set<string>();
  for (const pod of store.listPods(ctx.store)) {
    if (!pod.repo || !pod.branch) continue;
    repos.add(pod.repo);
    if (pod.state !== 'closed') continue; // live pods are never touched
    // 1) the checkout
    const wtPath = path.join(ctx.home, 'pods', pod.role, 'work');
    if (fs.existsSync(wtPath)) {
      try {
        await worktreeRemove(pod.repo, wtPath);
        gcWorktrees.push(pod.role);
      } catch (e) {
        console.warn(`[retention] worktree remove failed ${pod.role}:`, e instanceof Error ? e.message : e);
      }
    }
    // 2) the branch: delete only when fully merged (ahead=0)
    const baseRef = pod.repo_base ?? 'main';
    try {
      const r = await gitRaw(pod.repo, 'rev-list', '--left-right', '--count', `${baseRef}...${pod.branch}`);
      if (r.code !== 0) continue; // branch already gone
      const ahead = Number(r.stdout.trim().split('\t')[1] ?? 0);
      if (ahead === 0) {
        const del = await gitRaw(pod.repo, 'branch', '-D', pod.branch);
        if (del.code === 0) gcBranches.push(`${pod.role} (${pod.branch})`);
        else gcKept.push(`${pod.role} (${pod.branch} kept: delete failed: ${del.stderr.slice(0, 60)})`);
      } else {
        gcKept.push(`${pod.role} (${pod.branch}, ahead=${ahead} — unmerged candidate)`);
      }
    } catch (e) {
      console.warn(`[retention] branch gc failed ${pod.role}:`, e instanceof Error ? e.message : e);
    }
  }
  // 3) stale git metadata
  for (const repo of repos) {
    try {
      await gitRaw(repo, 'worktree', 'prune');
    } catch { /* repo gone */ }
  }
  return { gcWorktrees, gcBranches, gcKept };
}
