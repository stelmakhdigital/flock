// task-flow: the TASK_FLOW guard + the idempotent same-state no-op.
// A queued->queued re-report (agent double-report racing the arbiter claim)
// must be a no-op, not an error. Run: node dist/core/task-flow.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { openStore, setTaskStatus } from './store.js';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-tf-'));
const db = openStore(home);
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  for (const role of ['dev']) {
    raw.prepare("INSERT OR IGNORE INTO pods(id, role, dir, state, created_at) VALUES (?, ?, ?, 'closed', ?)").run('pod_' + role, role, path.join(home, 'pods', role), new Date().toISOString());
  }
  raw.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES ('t_a', 'a', 'dev', 'queued', ?)").run(new Date().toISOString());
  raw.close();
}

// idempotent: same-state re-report is a no-op, not an error
setTaskStatus(db, 't_a', 'queued', { reason: 'first' });
setTaskStatus(db, 't_a', 'queued', { reason: 'again' }); // must not throw

// legal transition
setTaskStatus(db, 't_a', 'active');
// illegal transition: active -> queued is not in TASK_FLOW... actually it IS
// (the arbiter can re-queue). Use active -> cancelled (legal), then done (illegal)
setTaskStatus(db, 't_a', 'cancelled');
assert.throws(() => setTaskStatus(db, 't_a', 'done'), /bad task transition/, 'terminal state is terminal');

fs.rmSync(home, { recursive: true, force: true });
console.log('task-flow: all checks passed');
