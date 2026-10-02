import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

// ---------- types (rows) ----------

export interface Post {
  id: string;
  pod: string;
  role: string;
  dir: string;
  terminal_target: string | null;
  model: string | null;
  state: string; // live | idle | closed
  created_at: string;
}

export interface Run {
  id: string;
  post_role: string;
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
CREATE TABLE IF NOT EXISTS posts(
  id TEXT PRIMARY KEY,
  pod TEXT NOT NULL,
  role TEXT NOT NULL UNIQUE,
  dir TEXT NOT NULL,
  terminal_target TEXT,
  model TEXT,
  state TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs(
  id TEXT PRIMARY KEY,
  post_role TEXT NOT NULL REFERENCES posts(role),
  pid INTEGER,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  exit_state TEXT,
  meta TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS runs_post_idx ON runs(post_role, started_at);
CREATE TABLE IF NOT EXISTS tasks(
  id TEXT PRIMARY KEY,
  spec TEXT NOT NULL,
  state TEXT NOT NULL,
  pipeline TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS task_transitions(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL REFERENCES tasks(id),
  from_state TEXT,
  to_state TEXT NOT NULL,
  ts TEXT NOT NULL,
  meta TEXT
);
CREATE INDEX IF NOT EXISTS transitions_task_idx ON task_transitions(task_id, id);
CREATE TABLE IF NOT EXISTS post_notes(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  post_role TEXT NOT NULL REFERENCES posts(role),
  body TEXT NOT NULL,
  ts TEXT NOT NULL
);
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

// ---------- posts ----------

function dbOf(store: Store): DatabaseSync {
  return store.db;
}

export function openPost(
  store: Store,
  p: { id: string; pod: string; role: string; dir: string; terminalTarget: string; model: string | null },
): void {
  dbOf(store)
    .prepare(
      `INSERT INTO posts(id, pod, role, dir, terminal_target, model, state, created_at)
       VALUES (?, ?, ?, ?, ?, ?, 'live', ?)
       ON CONFLICT(role) DO UPDATE SET
         pod = excluded.pod, dir = excluded.dir,
         terminal_target = excluded.terminal_target, model = excluded.model, state = 'live'`,
    )
    .run(p.id, p.pod, p.role, p.dir, p.terminalTarget, p.model, nowIso());
}

export function getPostByRole(store: Store, role: string): Post | null {
  const r = dbOf(store).prepare('SELECT * FROM posts WHERE role = ?').get(role) as Post | undefined;
  return r ?? null;
}

export function listPosts(store: Store): Post[] {
  return dbOf(store).prepare('SELECT * FROM posts ORDER BY role').all() as unknown as Post[];
}

export function setPostState(store: Store, role: string, state: string): void {
  dbOf(store).prepare('UPDATE posts SET state = ? WHERE role = ?').run(state, role);
}

// ---------- runs ----------

export function insertRun(store: Store, r: { id: string; postRole: string; pid: number | null }): Run {
  dbOf(store)
    .prepare('INSERT INTO runs(id, post_role, pid, started_at) VALUES (?, ?, ?, ?)')
    .run(r.id, r.postRole, r.pid, nowIso());
  return currentRun(store, r.postRole)!;
}

export function currentRun(store: Store, role: string): Run | null {
  const r = dbOf(store)
    .prepare('SELECT * FROM runs WHERE post_role = ? ORDER BY started_at DESC, rowid DESC LIMIT 1')
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
      .prepare('SELECT * FROM runs WHERE post_role = ? ORDER BY started_at DESC, rowid DESC LIMIT 50')
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
