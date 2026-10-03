// retention — hermetic check in a tmp home (no network, no tmux).
// Run: node dist/core/retention.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import * as store from './store.js';
import { openStore } from './store.js';
import { runRetentionSweep } from './retention.js';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-ret-'));
process.env.FLOCK_RETENTION_RUNS_DAYS = '14';
process.env.FLOCK_RETENTION_ACTIVITY_LINES = '100';
process.env.FLOCK_RETENTION_ACTIVITY_KEEP = '10';

const db = openStore(home);
const old = new Date(Date.now() - 30 * 86_400_000).toISOString();
const recent = new Date(Date.now() - 86_400_000).toISOString();

// runs reference pods(role): create the pods first (direct SQL, closed state)
{
  const raw0 = new DatabaseSync(path.join(home, 'flock.db'));
  for (const role of ['dev', 'rev']) {
    raw0.prepare("INSERT OR IGNORE INTO pods(id, role, dir, state, created_at) VALUES (?, ?, ?, 'closed', ?)").run('pod_' + role, role, path.join(home, 'pods', role), new Date().toISOString());
  }
  raw0.close();
}

// two finished old runs + one recent + one unfinished old run
store.insertRun(db, { id: 'run_old1', podRole: 'dev', pid: 1, meta: { kind: 'x' } });
store.insertRun(db, { id: 'run_old2', podRole: 'rev', pid: 2, meta: { kind: 'y' } });
store.insertRun(db, { id: 'run_recent', podRole: 'dev', pid: 3, meta: { kind: 'z' } });
store.insertRun(db, { id: 'run_open', podRole: 'dev', pid: 4, meta: { kind: 'w' } });
// backdate via a raw handle on the same file
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  raw.prepare('UPDATE runs SET started_at = ?, ended_at = ?, exit_state = ? WHERE id = ?').run(old, old, 'clean', 'run_old1');
  raw.prepare('UPDATE runs SET started_at = ?, ended_at = ?, exit_state = ? WHERE id = ?').run(old, old, 'crashed(code 1)', 'run_old2');
  raw.prepare('UPDATE runs SET started_at = ?, ended_at = ?, exit_state = ? WHERE id = ?').run(recent, recent, 'clean', 'run_recent');
  raw.prepare('UPDATE runs SET started_at = ? WHERE id = ?').run(old, 'run_open');
  raw.close();
}

// activity file for dev: 150 lines (over the 100 high-water) -> trim to 10
const devSeat = path.join(home, 'pods', 'dev');
fs.mkdirSync(path.join(devSeat, '.pi'), { recursive: true });
fs.writeFileSync(path.join(devSeat, '.pi', 'activity.jsonl'), Array.from({ length: 150 }, (_, i) => JSON.stringify({ event: 'tool_start', n: i, at: new Date().toISOString() })).join('\n'));
// core.log over the 10MB cap
fs.writeFileSync(path.join(home, 'core.log'), 'x'.repeat(11 * 1024 * 1024));

const report = runRetentionSweep({ store: db, home });
assert.strictEqual(report.archivedRuns, 2, 'only the two FINISHED old runs are archived');
assert.ok(report.coreLogRotated, 'core.log rotated');
assert.ok(report.trimmedActivity.includes('dev'), 'dev activity trimmed');

// archived rows keep their meta
const arch = new DatabaseSync(path.join(home, 'flock.db'));
const rows = arch.prepare('SELECT * FROM runs_archive ORDER BY id').all() as { id: string; meta: string | null }[];
assert.deepStrictEqual(rows.map((r) => r.id).sort(), ['run_old1', 'run_old2']);
assert.ok(rows.find((r) => r.id === 'run_old1')!.meta?.includes('kind'), 'meta preserved in archive');
const remaining = (arch.prepare('SELECT id FROM runs').all() as { id: string }[]).map((r) => r.id).sort();
assert.deepStrictEqual(remaining, ['run_open', 'run_recent'], 'recent + unfinished runs survive');
arch.close();

// activity trimmed to the tail 10 lines
const kept = fs.readFileSync(path.join(devSeat, '.pi', 'activity.jsonl'), 'utf8').trim().split('\n');
assert.strictEqual(kept.length, 10, 'tail window kept');
assert.strictEqual(JSON.parse(kept[0]).n, 140, 'head (oldest) dropped');

// idempotent second sweep: nothing to do
const again = runRetentionSweep({ store: db, home });
assert.strictEqual(again.archivedRuns, 0);
assert.deepStrictEqual(again.trimmedActivity, []);
assert.strictEqual(again.coreLogRotated, false);

fs.rmSync(home, { recursive: true, force: true });
console.log('retention: all checks passed');
