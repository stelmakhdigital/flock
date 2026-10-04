// events (C5): the append-only event log — every op leaves a row.
// Run: node dist/core/events.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { openStore, listEvents, latestEventId } from './store.js';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-ev-'));
const db = openStore(home);
const raw = new DatabaseSync(path.join(home, 'flock.db'));
raw.prepare('INSERT INTO pods(id, role, dir, state, created_at) VALUES (?, ?, ?, ?, ?)')
  .run('pod_dev', 'dev', path.join(home, 'pods', 'dev'), 'closed', new Date().toISOString());
raw.prepare('INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES (\'t_e\', \'e\', \'dev\', \'queued\', ?)')
  .run(new Date().toISOString());
raw.close();

const { apply } = await import('./ops.js');
const ctx = { store: db, ticks: {} } as never;

// a successful op leaves an event row
const before = latestEventId(db);
await apply({ type: 'task_add', title: 'e2', role: 'dev' }, ctx);
const evs = listEvents(db, before);
assert.strictEqual(evs.length, 1, 'one event per successful op');
assert.strictEqual(evs[0].kind, 'task_add');
assert.strictEqual(evs[0].actor, 'core');

// a FAILED op leaves no event row
const before2 = latestEventId(db);
await assert.rejects(() => apply({ type: 'task_done', id: 't_e' }, ctx), /closure reason/);
assert.strictEqual(listEvents(db, before2).length, 0, 'failed op: no event');

// pod-scoped ops carry the pod actor (t_e must be active first)
const raw2 = new DatabaseSync(path.join(home, 'flock.db'));
raw2.prepare("UPDATE tasks SET status = 'active' WHERE id = 't_e'").run();
raw2.close();
const podCtx = { store: db, ticks: {}, caller: { kind: 'pod' as const, role: 'dev' } } as never;
const before3 = latestEventId(db);
await apply({ type: 'task_done', id: 't_e', reason: 'finished' }, podCtx);
const ev3 = listEvents(db, before3);
assert.strictEqual(ev3.length, 1);
assert.strictEqual(ev3[0].kind, 'task_done');
assert.strictEqual(ev3[0].actor, 'pod:dev');
const payload = JSON.parse(ev3[0].payload ?? '{}') as { reason: string };
assert.strictEqual(payload.reason, 'finished');

// since-cursor + limit semantics (the SSE backlog reads exactly this way)
const all = listEvents(db, 0, 100);
assert.strictEqual(all.length, 2);
const tail = listEvents(db, all[0].id, 100);
assert.strictEqual(tail.length, 1, 'since=firstId: only events after it');
const limited = listEvents(db, 0, 1);
assert.strictEqual(limited.length, 1, 'limit bounds the backlog');
assert.strictEqual(limited[0].id, all[0].id);

fs.rmSync(home, { recursive: true, force: true });
console.log('events: all checks passed');
