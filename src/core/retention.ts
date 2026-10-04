// retention — the 24h sweep: runs older than N days -> runs_archive,
// activity.jsonl head-trim (a file over the high-water mark is trimmed to
// the tail window), core.log rotated when it grows past the size cap.
// Archive, not delete: the operator keeps an audit trail; deletion is a
// separate explicit op (retention_purge, stage 5.3+ if it earns its keep).
import fs from 'node:fs';
import path from 'node:path';
import * as store from './store.js';
import { seatPaths } from './bridge-protocol.js';

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
      // head-trim: keep the TAIL (the recent window).
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

  return { archivedRuns, trimmedActivity, coreLogRotated };
}

