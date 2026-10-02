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
  p: { id: string; role: string; dir: string; terminalTarget: string; model: string | null; agent?: string | null },
): void {
  dbOf(store)
    .prepare(
      `INSERT INTO pods(id, role, dir, terminal_target, model, state, created_at, agent)
       VALUES (?, ?, ?, ?, ?, 'live', ?, ?)
       ON CONFLICT(role) DO UPDATE SET
         dir = excluded.dir,
         terminal_target = excluded.terminal_target, model = excluded.model,
         agent = excluded.agent, state = 'live'`,
    )
    .run(p.id, p.role, p.dir, p.terminalTarget, p.model, nowIso(), p.agent ?? null);
}

export function getPodByRole(store: Store, role: string): Pod | null {
  const r = dbOf(store).prepare('SELECT * FROM pods WHERE role = ?').get(role) as Pod | undefined;
  return r ?? null;
}

export function listPods(store: Store): Pod[] {
  return dbOf(store).prepare('SELECT * FROM pods ORDER BY role').all() as unknown as Pod[];
}

export function setPodState(store: Store, role: string, state: string): void {
  dbOf(store).prepare('UPDATE pods SET state = ? WHERE role = ?').run(state, role);
}

// ---------- runs ----------

export function insertRun(store: Store, r: { id: string; podRole: string; pid: number | null }): Run {
  dbOf(store)
    .prepare('INSERT INTO runs(id, pod_role, pid, started_at) VALUES (?, ?, ?, ?)')
    .run(r.id, r.podRole, r.pid, nowIso());
  return currentRun(store, r.podRole)!;
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
}

const TASK_FLOW: Record<string, string[]> = {
  queued: ['active', 'cancelled'],
  active: ['done', 'blocked', 'cancelled', 'queued', 'needs'],
  needs: ['active', 'done', 'blocked', 'cancelled'], // human resolved / closed
  done: [],
  blocked: [],
  cancelled: [],
};

export function insertTask(
  store: Store,
  t: { id: string; title: string; body: string | null; podRole: string; workflowInstanceId?: string | null; workflowStep?: string | null },
): void {
  dbOf(store)
    .prepare('INSERT INTO tasks(id, title, body, pod_role, status, created_at, workflow_instance_id, workflow_step) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
    .run(t.id, t.title, t.body, t.podRole, 'queued', nowIso(), t.workflowInstanceId ?? null, t.workflowStep ?? null);
  dbOf(store)
    .prepare('INSERT INTO task_transitions(task_id, from_status, to_status, reason, ts) VALUES (?, NULL, ?, ?, ?)')
    .run(t.id, 'queued', 'created', nowIso());
}

export function getTask(store: Store, id: string): Task | null {
  const r = dbOf(store).prepare('SELECT * FROM tasks WHERE id = ?').get(id) as Task | undefined;
  return r ?? null;
}

export function listTasks(store: Store, status?: string): Task[] {
  if (status) {
    return dbOf(store)
      .prepare('SELECT * FROM tasks WHERE status = ? ORDER BY created_at ASC, rowid ASC')
      .all(status) as unknown as Task[];
  }
  return dbOf(store).prepare('SELECT * FROM tasks ORDER BY created_at DESC, rowid DESC LIMIT 100').all() as unknown as Task[];
}

export function oldestQueuedTask(store: Store, podRole: string): Task | null {
  const r = dbOf(store)
    .prepare("SELECT * FROM tasks WHERE status = 'queued' AND pod_role = ? ORDER BY created_at ASC, rowid ASC LIMIT 1")
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

export function insertWorkflowInstance(store: Store, i: { id: string; workflowId: string; payload: string | null }): void {
  dbOf(store)
    .prepare("INSERT INTO workflow_instances(id, workflow_id, payload, state, created_at) VALUES (?, ?, ?, 'running', ?)")
    .run(i.id, i.workflowId, i.payload, nowIso());
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
