import { DatabaseSync, type SQLInputValue } from 'node:sqlite';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

// ---------- types (rows) ----------

export interface Pod {
  id: string;
  role: string;
  dir: string;
  terminal_target: string | null;
  model: string | null;
  agent: string | null; // runtime adapter id (pi | bash | ...)
  state: string; // live | idle | closed
  resume_token: string | null; // pinned session file (honest resume override)
  profile: string | null; // manifest profile applied at spawn (survives relaunch)
  created_at: string;
}

export interface Run {
  id: string;
  pod_role: string;
  pid: number | null;
  started_at: string;
  ended_at: string | null;
  exit_state: string | null; // done | crashed | replaced
  meta: string; // JSON array (audit: sent bytes, etc.)
}

export interface Store {
  home: string;
  token: string;
  db: DatabaseSync;
}

// ---------- helpers ----------

export const nowIso = () => new Date().toISOString();

export function newId(prefix: string): string {
  return `${prefix}_${Date.now().toString(36)}${crypto.randomBytes(3).toString('hex')}`;
}

// ---------- schema (inlined; single source) ----------

const MIGRATIONS: { name: string; sql: string }[] = [
  {
    name: '001_init',
    sql: `
CREATE TABLE IF NOT EXISTS pods(
  id TEXT PRIMARY KEY,
  role TEXT NOT NULL UNIQUE,
  dir TEXT NOT NULL,
  terminal_target TEXT,
  model TEXT,
  state TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs(
  id TEXT PRIMARY KEY,
  pod_role TEXT NOT NULL REFERENCES pods(role),
  pid INTEGER,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  exit_state TEXT,
  meta TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS runs_pod_idx ON runs(pod_role, started_at);
CREATE TABLE IF NOT EXISTS pod_notes(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pod_role TEXT NOT NULL REFERENCES pods(role),
  body TEXT NOT NULL,
  ts TEXT NOT NULL
);
`,
  },
  {
    name: '002_watchdog',
    sql: `
CREATE TABLE IF NOT EXISTS watchdog_jobs(
  id TEXT PRIMARY KEY,
  policy TEXT NOT NULL,
  spec TEXT NOT NULL,
  target_pod TEXT NOT NULL,
  interval_seconds INTEGER NOT NULL,
  active_wake_interval_seconds INTEGER,
  state TEXT NOT NULL DEFAULT 'active',
  actionable INTEGER NOT NULL DEFAULT 0,
  last_evaluation_at TEXT,
  last_fire_at TEXT,
  last_actionable_at TEXT,
  last_state TEXT,
  last_skip_reason TEXT,
  registered_by TEXT NOT NULL,
  registered_at TEXT NOT NULL,
  terminal_reason TEXT
);
CREATE INDEX IF NOT EXISTS watchdog_jobs_state_idx ON watchdog_jobs(state);
CREATE TABLE IF NOT EXISTS watchdog_history(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL REFERENCES watchdog_jobs(id),
  evaluated_at TEXT NOT NULL,
  outcome TEXT NOT NULL,
  skip_reason TEXT,
  delivery_status TEXT,
  delivery_message TEXT
);
CREATE INDEX IF NOT EXISTS watchdog_history_job_idx ON watchdog_history(job_id, id);
`,
  },
  {
    name: '003_tasks',
    sql: `
CREATE TABLE IF NOT EXISTS tasks(
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT,
  pod_role TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  result TEXT,
  created_at TEXT NOT NULL,
  claimed_at TEXT,
  finished_at TEXT
);
CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks(status, created_at);
CREATE INDEX IF NOT EXISTS tasks_pod_idx ON tasks(pod_role, status);
CREATE TABLE IF NOT EXISTS task_transitions(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  from_status TEXT,
  to_status TEXT NOT NULL,
  reason TEXT,
  ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS task_transitions_task_idx ON task_transitions(task_id, id);
`,
  },
  {
    name: '004_workflows',
    sql: `
CREATE TABLE IF NOT EXISTS workflows(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  spec TEXT NOT NULL, -- JSON {steps: [{id, role, title?}]}
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS workflow_instances(
  id TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL REFERENCES workflows(id),
  payload TEXT,
  state TEXT NOT NULL DEFAULT 'running', -- running|done|blocked|cancelled
  current_step TEXT,
  created_at TEXT NOT NULL,
  finished_at TEXT
);
ALTER TABLE tasks ADD COLUMN workflow_instance_id TEXT;
ALTER TABLE tasks ADD COLUMN workflow_step TEXT;
CREATE INDEX IF NOT EXISTS tasks_wf_idx ON tasks(workflow_instance_id);
ALTER TABLE pods ADD COLUMN agent TEXT;
`,
  },
  {
    name: '005_health',
    sql: `
CREATE TABLE IF NOT EXISTS health_alerts(
  pod_role TEXT NOT NULL,
  kind TEXT NOT NULL, -- gate | idle
  ref TEXT NOT NULL, -- dialog id | task id
  state TEXT NOT NULL,
  count INTEGER NOT NULL DEFAULT 0,
  first_at TEXT NOT NULL,
  last_at TEXT NOT NULL,
  note TEXT,
  PRIMARY KEY (pod_role, kind, ref)
);
`,
  },
  {
    name: '006_resume_token',
    sql: `
ALTER TABLE pods ADD COLUMN resume_token TEXT;
`,
  },
  {
    name: '007_profile',
    sql: `
ALTER TABLE pods ADD COLUMN profile TEXT;
`,
  },
  {
    name: '008_repo',
    sql: `
ALTER TABLE pods ADD COLUMN repo TEXT;
ALTER TABLE pods ADD COLUMN repo_base TEXT;
ALTER TABLE pods ADD COLUMN branch TEXT;
`,
  },
  {
    name: '009_usage',
    sql: `
CREATE TABLE IF NOT EXISTS usage_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  role TEXT NOT NULL,
  at TEXT NOT NULL,
  ts TEXT NOT NULL,
  input INTEGER NOT NULL DEFAULT 0,
  output INTEGER NOT NULL DEFAULT 0,
  cache_read INTEGER NOT NULL DEFAULT 0,
  cache_write INTEGER NOT NULL DEFAULT 0,
  model TEXT,
  UNIQUE(role, at, input, output, cache_read, cache_write)
);
CREATE INDEX IF NOT EXISTS idx_usage_role_ts ON usage_events(role, ts);
CREATE TABLE IF NOT EXISTS runs_archive(
  id TEXT PRIMARY KEY,
  pod_role TEXT NOT NULL,
  pid INTEGER,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  exit_state TEXT,
  meta TEXT,
  archived_at TEXT NOT NULL
);
`,
  },
  {
    name: '010_wf2',
    sql: `
ALTER TABLE tasks ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflow_instances ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS wf_step_state(
  instance_id TEXT NOT NULL,
  step TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (instance_id, step)
);
`,
  },
  {
    // 5.4b S1+S2: merge policy per pod (ff|squash|never, default ff) and the
    // quality-gate flag per workflow instance (require: test)
    name: '011_merge',
    sql: `
ALTER TABLE pods ADD COLUMN merge_policy TEXT;
ALTER TABLE workflow_instances ADD COLUMN require_test INTEGER NOT NULL DEFAULT 0;
`,
  },
  {
    // 5.4b S5: conflict-resolution chain — one row per origin (conflicted)
    // task; the resolver pod works on a <branch>-resolve fork, core applies
    // the result back and re-runs the merge gate. Hard attempt limit: when
    // exhausted, the task stays blocked for the operator.
    name: '012_resolutions',
    sql: `
CREATE TABLE IF NOT EXISTS conflict_resolutions(
  id TEXT PRIMARY KEY,
  origin_task_id TEXT NOT NULL UNIQUE,
  origin_role TEXT NOT NULL,
  resolver_role TEXT NOT NULL,
  resolver_task_id TEXT,
  attempts INTEGER NOT NULL DEFAULT 1,
  apply_attempts INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'running',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
`,
  },
  {
    // 5.4c: durable escalation ladder — pm triggers are persisted, not
    // fire-and-forget: pm dead -> the trigger survives the restart and the
    // ladder walks pm -> operator with timestamps (audit: why it hung)
    name: '013_escalations',
    sql: `
CREATE TABLE IF NOT EXISTS escalations(
  id TEXT PRIMARY KEY,
  key TEXT NOT NULL,
  kind TEXT NOT NULL,
  subject TEXT NOT NULL,
  severity TEXT NOT NULL DEFAULT 'warn',
  state TEXT NOT NULL DEFAULT 'open',
  pm_notified_at TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  resolved_reason TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_esc_key_state ON escalations(key, state);
`,
  },
  {
    // 5.4d: DAG workflows — per-step state for the dependency engine
    // (pending | running | done | blocked); wf_step_state now carries both
    // the retry counter and the step's place in the DAG
    name: '014_wf_step_state',
    sql: `ALTER TABLE wf_step_state ADD COLUMN state TEXT NOT NULL DEFAULT 'pending';`,
  },
  {
    // C1: git is owned by the agents — core drops worktree/merge machinery.
    // DROP COLUMN needs sqlite >= 3.35 (node:sqlite ships 3.46).
    name: '015_git_drop',
    sql: `
DROP TABLE IF EXISTS conflict_resolutions;
ALTER TABLE pods DROP COLUMN repo;
ALTER TABLE pods DROP COLUMN repo_base;
ALTER TABLE pods DROP COLUMN branch;
ALTER TABLE pods DROP COLUMN merge_policy;
ALTER TABLE workflow_instances DROP COLUMN require_test;
`,
  },  {
    // C2: economy is out of core — usage_events goes with the usage tick.
    name: '016_economy_drop',
    sql: `
DROP TABLE IF EXISTS usage_events;
`,
  },
  {
    // C3: hot-potato closure — a terminal task state carries the closure
    // {reason, target?, at, by}; the closure rides the terminal transition
    // too (task_transitions.closed) so the audit trail is self-contained.
    name: '017_tasks_closed',
    sql: `
ALTER TABLE tasks ADD COLUMN closed TEXT;
ALTER TABLE task_transitions ADD COLUMN closed TEXT;
`,
  },
];

function migrate(db: DatabaseSync): void {
  db.exec('CREATE TABLE IF NOT EXISTS schema_migrations(name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)');
  const rows = db.prepare('SELECT name FROM schema_migrations').all() as { name: string }[];
  const applied = new Set(rows.map((r) => r.name));
  for (const m of MIGRATIONS) {
    if (applied.has(m.name)) continue;
    db.exec('BEGIN');
    try {
      db.exec(m.sql);
      db.prepare('INSERT INTO schema_migrations(name, applied_at) VALUES (?, ?)').run(m.name, nowIso());
      db.exec('COMMIT');
    } catch (e) {
      db.exec('ROLLBACK');
      throw e;
    }
  }
}

// ---------- store lifecycle ----------

export function openStore(home: string): Store {
  fs.mkdirSync(home, { recursive: true });
  const tokenPath = path.join(home, 'token');
  let token: string;
  if (fs.existsSync(tokenPath)) {
    token = fs.readFileSync(tokenPath, 'utf8').trim();
  } else {
    token = crypto.randomBytes(24).toString('hex');
    fs.writeFileSync(tokenPath, token + '\n', { mode: 0o600 });
  }
  const db = new DatabaseSync(path.join(home, 'flock.db'));
  migrate(db);
  return { home, token, db };
}

// ---------- pods ----------

function dbOf(store: Store): DatabaseSync {
  return store.db;
}

export function openPod(
  store: Store,
  p: { id: string; role: string; dir: string; terminalTarget: string; model: string | null; agent?: string | null; profile?: string | null },
): void {
  dbOf(store)
    .prepare(
      `INSERT INTO pods(id, role, dir, terminal_target, model, state, created_at, agent, profile)
       VALUES (?, ?, ?, ?, ?, 'live', ?, ?, ?)
       ON CONFLICT(role) DO UPDATE SET
         dir = excluded.dir,
         terminal_target = excluded.terminal_target, model = excluded.model,
         agent = excluded.agent, profile = excluded.profile, state = 'live'`,
    )
    .run(p.id, p.role, p.dir, p.terminalTarget, p.model, nowIso(), p.agent ?? null, p.profile ?? null);
}

export function getPodByRole(store: Store, role: string): Pod | null {
  const r = dbOf(store).prepare('SELECT * FROM pods WHERE role = ?').get(role) as Pod | undefined;
  return r ?? null;
}

export function setPodResumeToken(store: Store, role: string, token: string | null): void {
  dbOf(store)
    .prepare('UPDATE pods SET resume_token = ? WHERE role = ?')
    .run(token, role);
}

export function setPodProfile(store: Store, role: string, profile: string | null): void {
  dbOf(store)
    .prepare('UPDATE pods SET profile = ? WHERE role = ?')
    .run(profile, role);
}

export function listPods(store: Store): Pod[] {
  return dbOf(store).prepare('SELECT * FROM pods ORDER BY role').all() as unknown as Pod[];
}

export function setPodState(store: Store, role: string, state: string): void {
  dbOf(store).prepare('UPDATE pods SET state = ? WHERE role = ?').run(state, role);
}

// ---------- runs ----------

export function insertRun(store: Store, r: { id: string; podRole: string; pid: number | null; meta?: Record<string, unknown> }): Run {
  const meta =
    r.meta && Object.keys(r.meta).length
      ? JSON.stringify([{ ts: nowIso(), kind: 'created', ...r.meta }])
      : null;
  dbOf(store)
    .prepare('INSERT INTO runs(id, pod_role, pid, started_at, meta) VALUES (?, ?, ?, ?, ?)')
    .run(r.id, r.podRole, r.pid, nowIso(), meta);
  return currentRun(store, r.podRole)!;
}

// Latest session file recorded for a role (any run, most recent first) —
// used by fork/relaunch bookkeeping.
export function latestSessionFile(store: Store, role: string): string | null {
  const rows = dbOf(store)
    .prepare('SELECT meta FROM runs WHERE pod_role = ? AND meta IS NOT NULL ORDER BY started_at DESC, rowid DESC LIMIT 50')
    .all(role) as { meta: string }[];
  for (const row of rows) {
    try {
      const arr = JSON.parse(row.meta);
      if (!Array.isArray(arr)) continue;
      for (let i = arr.length - 1; i >= 0; i--) {
        const e = arr[i];
        if (e && typeof e.sessionFile === 'string') return e.sessionFile;
      }
    } catch {
      /* malformed meta row: skip */
    }
  }
  return null;
}

export function currentRun(store: Store, role: string): Run | null {
  const r = dbOf(store)
    .prepare('SELECT * FROM runs WHERE pod_role = ? ORDER BY started_at DESC, rowid DESC LIMIT 1')
    .get(role) as Run | undefined;
  return r ?? null;
}

export function endRun(store: Store, runId: string, exitState: string): void {
  dbOf(store)
    .prepare('UPDATE runs SET ended_at = ?, exit_state = ? WHERE id = ? AND ended_at IS NULL')
    .run(nowIso(), exitState, runId);
}

export function crashStaleRuns(store: Store): number {
  const res = dbOf(store)
    .prepare('UPDATE runs SET ended_at = ?, exit_state = ? WHERE ended_at IS NULL')
    .run(nowIso(), 'crashed');
  return Number(res.changes);
}

export function listRuns(store: Store, role?: string): Run[] {
  if (role) {
    return dbOf(store)
      .prepare('SELECT * FROM runs WHERE pod_role = ? ORDER BY started_at DESC, rowid DESC LIMIT 50')
      .all(role) as unknown as Run[];
  }
  return dbOf(store)
    .prepare('SELECT * FROM runs ORDER BY started_at DESC, rowid DESC LIMIT 50')
    .all() as unknown as Run[];
}

export function appendRunMeta(store: Store, runId: string, entry: Record<string, unknown>): void {
  const row = dbOf(store).prepare('SELECT meta FROM runs WHERE id = ?').get(runId) as { meta: string } | undefined;
  if (!row) return;
  let arr: unknown[] = [];
  try {
    arr = JSON.parse(row.meta);
  } catch {
    arr = [];
  }
  arr.push({ ts: nowIso(), ...entry });
  if (arr.length > 100) arr = arr.slice(-100);
  dbOf(store).prepare('UPDATE runs SET meta = ? WHERE id = ?').run(JSON.stringify(arr), runId);
}

// ---------- watchdog ----------

export interface WatchdogJob {
  id: string;
  policy: string;
  spec: string; // JSON
  target_pod: string;
  interval_seconds: number;
  active_wake_interval_seconds: number | null;
  state: string; // active | terminal
  actionable: number;
  last_evaluation_at: string | null;
  last_fire_at: string | null;
  last_actionable_at: string | null;
  last_state: string | null; // policy memory (JSON)
  last_skip_reason: string | null;
  registered_by: string;
  registered_at: string;
  terminal_reason: string | null;
}

const WD_FIELDS = [
  'policy', 'spec', 'target_pod', 'interval_seconds', 'active_wake_interval_seconds',
  'state', 'actionable', 'last_evaluation_at', 'last_fire_at', 'last_actionable_at',
  'last_state', 'last_skip_reason', 'terminal_reason',
] as const;

export function insertWatchdogJob(
  store: Store,
  j: { id: string; policy: string; spec: string; targetPod: string; intervalSeconds: number; activeWakeIntervalSeconds: number | null; registeredBy: string },
): void {
  dbOf(store)
    .prepare(
      `INSERT INTO watchdog_jobs(id, policy, spec, target_pod, interval_seconds, active_wake_interval_seconds, state, registered_by, registered_at)
       VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)`,
    )
    .run(j.id, j.policy, j.spec, j.targetPod, j.intervalSeconds, j.activeWakeIntervalSeconds, j.registeredBy, nowIso());
}

export function getWatchdogJob(store: Store, id: string): WatchdogJob | null {
  const r = dbOf(store).prepare('SELECT * FROM watchdog_jobs WHERE id = ?').get(id) as WatchdogJob | undefined;
  return r ?? null;
}

export function listWatchdogJobs(store: Store, state?: string): WatchdogJob[] {
  if (state) {
    return dbOf(store)
      .prepare('SELECT * FROM watchdog_jobs WHERE state = ? ORDER BY registered_at DESC, rowid DESC')
      .all(state) as unknown as WatchdogJob[];
  }
  return dbOf(store)
    .prepare('SELECT * FROM watchdog_jobs ORDER BY registered_at DESC, rowid DESC LIMIT 100')
    .all() as unknown as WatchdogJob[];
}

export function watchdogUpdate(store: Store, id: string, fields: Record<string, unknown>): void {
  const keys = Object.keys(fields).filter((k) => (WD_FIELDS as readonly string[]).includes(k));
  if (!keys.length) return;
  const sets = keys.map((k) => `${k} = ?`).join(', ');
  dbOf(store).prepare(`UPDATE watchdog_jobs SET ${sets} WHERE id = ?`).run(...(keys.map((k) => fields[k]) as SQLInputValue[]), id);
}

export function addWatchdogHistory(
  store: Store,
  h: { jobId: string; outcome: string; skipReason?: string | null; deliveryStatus?: string | null; deliveryMessage?: string | null },
): void {
  dbOf(store)
    .prepare(
      'INSERT INTO watchdog_history(job_id, evaluated_at, outcome, skip_reason, delivery_status, delivery_message) VALUES (?, ?, ?, ?, ?, ?)',
    )
    .run(h.jobId, nowIso(), h.outcome, h.skipReason ?? null, h.deliveryStatus ?? null, h.deliveryMessage ?? null);
}

export function listWatchdogHistory(store: Store, jobId: string): unknown[] {
  return dbOf(store)
    .prepare('SELECT * FROM watchdog_history WHERE job_id = ? ORDER BY id DESC LIMIT 100')
    .all(jobId);
}

// ---------- tasks (stage 1) ----------

export interface Task {
  id: string;
  title: string;
  body: string | null;
  pod_role: string;
  status: string; // queued | active | done | blocked | cancelled | needs
  result: string | null;
  created_at: string;
  claimed_at: string | null;
  finished_at: string | null;
  workflow_instance_id: string | null;
  workflow_step: string | null;
  priority: number;
  closed: string | null; // C3: JSON {reason, target?, at, by} — terminal states only
}

// C3: hot-potato closure vocabulary. A unit of work cannot be closed
// without a reason; handed-off/escalated must name their target.
export const CLOSURE_REASONS = ['finished', 'handed-off', 'blocked', 'denied', 'canceled', 'escalated'] as const;
export type ClosureReason = (typeof CLOSURE_REASONS)[number];
export const CLOSURE_TARGET_REQUIRED: ClosureReason[] = ['handed-off', 'escalated'];

export interface Closure {
  reason: ClosureReason;
  target?: string;
  at: string;
  by: string;
}

const TASK_FLOW: Record<string, string[]> = {
  queued: ['active', 'cancelled'],
  active: ['done', 'blocked', 'cancelled', 'queued', 'needs'],
  needs: ['active', 'done', 'blocked', 'cancelled'], // human resolved / closed
  blocked: ['queued', 'active', 'done', 'cancelled'], // unblock (requeue/hand off), close
  done: [],
  cancelled: [],
};

export function insertTask(
  store: Store,
  t: { id: string; title: string; body: string | null; podRole: string; workflowInstanceId?: string | null; workflowStep?: string | null; priority?: number },
): void {
  dbOf(store)
    .prepare('INSERT INTO tasks(id, title, body, pod_role, status, created_at, workflow_instance_id, workflow_step, priority) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
    .run(t.id, t.title, t.body, t.podRole, 'queued', nowIso(), t.workflowInstanceId ?? null, t.workflowStep ?? null, t.priority ?? 0);
  dbOf(store)
    .prepare('INSERT INTO task_transitions(task_id, from_status, to_status, reason, ts) VALUES (?, NULL, ?, ?, ?)')
    .run(t.id, 'queued', 'created', nowIso());
}

export function getTask(store: Store, id: string): Task | null {
  const r = dbOf(store).prepare('SELECT * FROM tasks WHERE id = ?').get(id) as Task | undefined;
  return r ?? null;
}

export function listTasks(store: Store, status?: string, limit = 100): Task[] {
  if (status) {
    return dbOf(store)
      .prepare('SELECT * FROM tasks WHERE status = ? ORDER BY created_at ASC, rowid ASC LIMIT ?')
      .all(status, limit) as unknown as Task[];
  }
  return dbOf(store).prepare('SELECT * FROM tasks ORDER BY created_at DESC, rowid DESC LIMIT ?').all(limit) as unknown as Task[];
}

export function oldestQueuedTask(store: Store, podRole: string): Task | null {
  const r = dbOf(store)
    .prepare("SELECT * FROM tasks WHERE status = 'queued' AND pod_role = ? ORDER BY priority DESC, created_at ASC, rowid ASC LIMIT 1")
    .get(podRole) as Task | undefined;
  return r ?? null;
}

export function activeTaskForPod(store: Store, podRole: string): Task | null {
  const r = dbOf(store)
    .prepare("SELECT * FROM tasks WHERE status = 'active' AND pod_role = ? LIMIT 1")
    .get(podRole) as Task | undefined;
  return r ?? null;
}

export function setTaskStatus(store: Store, id: string, to: string, opts?: { reason?: string; result?: string | null }): void {
  const now = nowIso();
  const cur = dbOf(store).prepare('SELECT status FROM tasks WHERE id = ?').get(id) as { status: string } | undefined;
  if (!cur) throw new Error(`no task: ${id}`);
  if (to === cur.status) return; // idempotent no-op (same-state report)
  if (!TASK_FLOW[cur.status]?.includes(to)) {
    throw new Error(`bad task transition: ${cur.status} -> ${to}`);
  }
  const sets: string[] = ['status = ?'];
  const vals: SQLInputValue[] = [to];
  if (to === 'active') { sets.push('claimed_at = ?'); vals.push(now); }
  if (to === 'queued') { sets.push('claimed_at = NULL', 'result = NULL'); }
  if (to === 'done' || to === 'blocked' || to === 'cancelled') { sets.push('finished_at = ?'); vals.push(now); }
  if (opts?.result !== undefined) { sets.push('result = ?'); vals.push(opts.result); }
  vals.push(id);
  dbOf(store).prepare(`UPDATE tasks SET ${sets.join(', ')} WHERE id = ?`).run(...vals);
  dbOf(store)
    .prepare('INSERT INTO task_transitions(task_id, from_status, to_status, reason, ts) VALUES (?, ?, ?, ?, ?)')
    .run(id, cur.status, to, opts?.reason ?? null, now);
}

export function listTaskTransitions(store: Store, taskId: string): unknown[] {
  return dbOf(store)
    .prepare('SELECT * FROM task_transitions WHERE task_id = ? ORDER BY id ASC')
    .all(taskId);
}

// C3: the ONLY path to a terminal task state (done/cancelled). Validates the
// hot-potato closure (reason from the vocabulary; target required for
// handed-off/escalated) and writes tasks.closed + the terminal transition
// (with the closure) in one transaction. ponytail: the event-log row (C5)
// joins here once the events table exists.
export function closeWorkItem(
  store: Store,
  id: string,
  to: 'done' | 'cancelled',
  closure: { reason: ClosureReason; target?: string; by: string; result?: string | null },
): void {
  const db = dbOf(store);
  const cur = db.prepare('SELECT status FROM tasks WHERE id = ?').get(id) as { status: string } | undefined;
  if (!cur) throw new Error(`no task: ${id}`);
  if (!(CLOSURE_REASONS as readonly string[]).includes(closure.reason)) {
    throw new Error(`invalid closure reason: ${String(closure.reason)} (want: ${CLOSURE_REASONS.join(' | ')})`);
  }
  if (CLOSURE_TARGET_REQUIRED.includes(closure.reason) && !String(closure.target ?? '').trim()) {
    throw new Error(`closure ${closure.reason} requires target`);
  }
  if (cur.status === to) return; // idempotent no-op (same-state report)
  if (!TASK_FLOW[cur.status]?.includes(to)) {
    throw new Error(`bad task transition: ${cur.status} -> ${to}`);
  }
  const now = nowIso();
  const closed: Closure = { reason: closure.reason, at: now, by: closure.by };
  if (String(closure.target ?? '').trim()) closed.target = String(closure.target).trim();
  const closedJson = JSON.stringify(closed);
  db.exec('BEGIN');
  try {
    if (closure.result !== undefined) {
      db.prepare('UPDATE tasks SET status = ?, closed = ?, finished_at = ?, result = ? WHERE id = ?').run(to, closedJson, now, closure.result, id);
    } else {
      db.prepare('UPDATE tasks SET status = ?, closed = ?, finished_at = ? WHERE id = ?').run(to, closedJson, now, id);
    }
    db.prepare('INSERT INTO task_transitions(task_id, from_status, to_status, reason, closed, ts) VALUES (?, ?, ?, ?, ?, ?)')
      .run(id, cur.status, to, closure.reason, closedJson, now);
    db.exec('COMMIT');
  } catch (e) {
    db.exec('ROLLBACK');
    throw e;
  }
}

// C3: transactional handoff — the old task closes (reason 'handed-off',
// target = toRole) and the successor task is created for toRole in the SAME
// transaction: a lost handoff is impossible by construction.
export function handoffTask(
  store: Store,
  id: string,
  toRole: string,
  by: string,
): { from: Task; to: Task } {
  const db = dbOf(store);
  const t = db.prepare('SELECT * FROM tasks WHERE id = ?').get(id) as Task | undefined;
  if (!t) throw new Error(`no task: ${id}`);
  if (!TASK_FLOW[t.status]?.includes('done')) {
    throw new Error(`bad task transition: ${t.status} -> done (handoff)`);
  }
  const now = nowIso();
  const closed: Closure = { reason: 'handed-off', target: toRole, at: now, by };
  const closedJson = JSON.stringify(closed);
  const succId = newId('t');
  const body = [
    t.body,
    `Передано (handoff) из таска ${id} (pod ${t.pod_role}) — продолжай с места остановки.`,
  ].filter(Boolean).join('\n') || null;
  db.exec('BEGIN');
  try {
    db.prepare('UPDATE tasks SET status = ?, closed = ?, finished_at = ? WHERE id = ?').run('done', closedJson, now, id);
    db.prepare('INSERT INTO task_transitions(task_id, from_status, to_status, reason, closed, ts) VALUES (?, ?, ?, ?, ?, ?)')
      .run(id, t.status, 'done', 'handed-off', closedJson, now);
    db.prepare('INSERT INTO tasks(id, title, body, pod_role, status, created_at, workflow_instance_id, workflow_step, priority) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
      .run(succId, t.title, body, toRole, 'queued', now, null, null, t.priority);
    db.prepare('INSERT INTO task_transitions(task_id, from_status, to_status, reason, ts) VALUES (?, NULL, ?, ?, ?)')
      .run(succId, 'queued', `handoff from ${id}`, now);
    db.exec('COMMIT');
  } catch (e) {
    db.exec('ROLLBACK');
    throw e;
  }
  return { from: getTask(store, id)!, to: getTask(store, succId)! };
}

// ---------- workflows (multi-step pipelines over the task queue) ----------

export interface Workflow {
  id: string;
  name: string;
  spec: string; // JSON {steps: [{id, role, title?}]}
  created_at: string;
}

export interface WorkflowInstance {
  id: string;
  workflow_id: string;
  payload: string | null;
  state: string; // running | done | blocked | cancelled
  current_step: string | null;
  created_at: string;
  finished_at: string | null;
  priority: number;
}

export function insertWorkflow(store: Store, w: { id: string; name: string; spec: string }): void {
  dbOf(store).prepare('INSERT INTO workflows(id, name, spec, created_at) VALUES (?, ?, ?, ?)').run(w.id, w.name, w.spec, nowIso());
}

export function getWorkflow(store: Store, nameOrId: string): Workflow | null {
  const byId = dbOf(store).prepare('SELECT * FROM workflows WHERE id = ?').get(nameOrId) as Workflow | undefined;
  if (byId) return byId;
  const byName = dbOf(store).prepare('SELECT * FROM workflows WHERE name = ?').get(nameOrId) as Workflow | undefined;
  return byName ?? null;
}

export function listWorkflows(store: Store): Workflow[] {
  return dbOf(store).prepare('SELECT * FROM workflows ORDER BY created_at DESC, rowid DESC').all() as unknown as Workflow[];
}

export function deleteWorkflow(store: Store, id: string): void {
  dbOf(store).prepare('DELETE FROM workflows WHERE id = ?').run(id);
}

export function insertWorkflowInstance(store: Store, i: { id: string; workflowId: string; payload: string | null; priority?: number }): void {
  dbOf(store)
    .prepare("INSERT INTO workflow_instances(id, workflow_id, payload, state, created_at, priority) VALUES (?, ?, ?, 'running', ?, ?)")
    .run(i.id, i.workflowId, i.payload, nowIso(), i.priority ?? 0);
}

export function getWorkflowInstance(store: Store, id: string): WorkflowInstance | null {
  const r = dbOf(store).prepare('SELECT * FROM workflow_instances WHERE id = ?').get(id) as WorkflowInstance | undefined;
  return r ?? null;
}

export function listWorkflowInstances(store: Store): WorkflowInstance[] {
  return dbOf(store)
    .prepare('SELECT * FROM workflow_instances ORDER BY created_at DESC, rowid DESC LIMIT 100')
    .all() as unknown as WorkflowInstance[];
}

export function setWorkflowInstanceState(store: Store, id: string, state: string, currentStep?: string | null): void {
  const terminal = state !== 'running';
  dbOf(store)
    .prepare('UPDATE workflow_instances SET state = ?, current_step = COALESCE(?, current_step), finished_at = ? WHERE id = ?')
    .run(state, currentStep ?? null, terminal ? nowIso() : null, id);
}

export function listTasksForInstance(store: Store, instanceId: string): Task[] {
  return dbOf(store)
    .prepare('SELECT * FROM tasks WHERE workflow_instance_id = ? ORDER BY created_at ASC, rowid ASC')
    .all(instanceId) as unknown as Task[];
}

// per-step retry accounting: how many times a step of an instance has been
// attempted (the first enqueue counts as attempt 0; each re-queue after a
// failure increments it)
export function setWfStepState(store: Store, instanceId: string, step: string, state: string): void {
  dbOf(store)
    .prepare('INSERT INTO wf_step_state(instance_id, step, state) VALUES (?, ?, ?) ON CONFLICT(instance_id, step) DO UPDATE SET state = excluded.state')
    .run(instanceId, step, state);
}

export function wfStepStateMap(store: Store, instanceId: string): Record<string, string> {
  const rows = dbOf(store)
    .prepare('SELECT step, state FROM wf_step_state WHERE instance_id = ?')
    .all(instanceId) as { step: string; state: string }[];
  const m: Record<string, string> = {};
  for (const r of rows) m[r.step] = r.state;
  return m;
}

export function wfStepAttempts(store: Store, instanceId: string, step: string): number {
  const r = dbOf(store)
    .prepare('SELECT attempts FROM wf_step_state WHERE instance_id = ? AND step = ?')
    .get(instanceId, step) as { attempts: number } | undefined;
  return r?.attempts ?? 0;
}

export function bumpWfStepAttempts(store: Store, instanceId: string, step: string): number {
  const next = wfStepAttempts(store, instanceId, step) + 1;
  dbOf(store)
    .prepare('INSERT INTO wf_step_state(instance_id, step, attempts) VALUES (?, ?, ?) ON CONFLICT(instance_id, step) DO UPDATE SET attempts = ?')
    .run(instanceId, step, next, next);
  return next;
}

export function listWfStepStates(store: Store, instanceId: string): { step: string; attempts: number; state: string }[] {
  return dbOf(store)
    .prepare('SELECT step, attempts, state FROM wf_step_state WHERE instance_id = ? ORDER BY step')
    .all(instanceId) as { step: string; attempts: number; state: string }[];
}

// retention: finished runs older than N days -> runs_archive (archive, not
// delete: the meta carries launch/resume/usage history the operator audits)
export interface Escalation {
  id: string;
  key: string;
  kind: string; // pod_crashed | task_blocked | task_needs | workflow_blocked | gate_red | ...
  subject: string;
  severity: string; // warn | critical
  state: string; // open | pm_notified | escalated | acknowledged | resolved
  pm_notified_at: string | null;
  attempts: number;
  resolved_reason: string | null;
  created_at: string;
  updated_at: string;
}

// 5.4c: durable escalation ladder (the pm trigger that is never lost). One
// ACTIVE row per key; a duplicate event while active is absorbed (audit
// note refreshed). Active = open | pm_notified | escalated.
export const ESC_ACTIVE_STATES = ['open', 'pm_notified', 'escalated'] as const;

export function upsertEscalation(
  store: Store,
  e: { key: string; kind: string; subject: string; severity?: string },
): { id: string; active: boolean } {
  const db = dbOf(store);
  const prev = db
    .prepare("SELECT * FROM escalations WHERE key = ? AND state IN ('open', 'pm_notified', 'escalated')")
    .get(e.key) as Escalation | undefined;
  if (prev) {
    db.prepare('UPDATE escalations SET subject = ?, updated_at = ? WHERE id = ?').run(e.subject, nowIso(), prev.id);
    return { id: prev.id, active: true };
  }
  const id = newId('esc');
  db.prepare(
    `INSERT INTO escalations(id, key, kind, subject, severity, state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'open', ?, ?)`,
  ).run(id, e.key, e.kind, e.subject, e.severity ?? 'warn', nowIso(), nowIso());
  return { id, active: true };
}

export function getEscalation(store: Store, id: string): Escalation | null {
  const r = dbOf(store).prepare('SELECT * FROM escalations WHERE id = ?').get(id) as Escalation | undefined;
  return r ?? null;
}

export function listEscalations(store: Store, activeOnly = false): Escalation[] {
  return activeOnly
    ? (dbOf(store).prepare("SELECT * FROM escalations WHERE state IN ('open', 'pm_notified', 'escalated') ORDER BY created_at DESC").all() as unknown as Escalation[])
    : (dbOf(store).prepare('SELECT * FROM escalations ORDER BY created_at DESC LIMIT 200').all() as unknown as Escalation[]);
}

export function setEscalationState(store: Store, id: string, state: string, extra?: { pmNotifiedAt?: string | null; attempts?: number; resolvedReason?: string | null; subject?: string }): void {
  const db = dbOf(store);
  const sets = ['state = ?', 'updated_at = ?'];
  const vals: (string | number | null)[] = [state, nowIso()];
  if (extra?.pmNotifiedAt !== undefined) { sets.push('pm_notified_at = ?'); vals.push(extra.pmNotifiedAt); }
  if (extra?.attempts !== undefined) { sets.push('attempts = ?'); vals.push(extra.attempts); }
  if (extra?.resolvedReason !== undefined) { sets.push('resolved_reason = ?'); vals.push(extra.resolvedReason); }
  if (extra?.subject !== undefined) { sets.push('subject = ?'); vals.push(extra.subject); }
  vals.push(id);
  db.prepare(`UPDATE escalations SET ${sets.join(', ')} WHERE id = ?`).run(...vals);
}

export function archiveOldRuns(store: Store, olderThanIso: string): number {
  const db = dbOf(store);
  const rows = db
    .prepare('SELECT id, pod_role, pid, started_at, ended_at, exit_state, meta FROM runs WHERE ended_at IS NOT NULL AND started_at < ?')
    .all(olderThanIso) as { id: string; pod_role: string; pid: number | null; started_at: string; ended_at: string | null; exit_state: string | null; meta: string | null }[];
  for (const r of rows) {
    db.prepare('INSERT OR IGNORE INTO runs_archive(id, pod_role, pid, started_at, ended_at, exit_state, meta, archived_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
      .run(r.id, r.pod_role, r.pid, r.started_at, r.ended_at, r.exit_state, r.meta, nowIso());
    db.prepare('DELETE FROM runs WHERE id = ?').run(r.id);
  }
  return rows.length;
}
