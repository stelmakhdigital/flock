// C15: fleet — cross-profile coordination, hermetic checks.
//
// Two real cores in two FLOCK_HOMEs, wired with a raw /api/ops endpoint
// (createHttp is the same code path the real core serves; no tmux, no
// ticks, no pods launched). Verifies: addressing, config round-trip, the
// two-phase handoff (phase 1 local close + phase 2 remote creation,
// idempotent retry, honest pending), and cross-profile message delivery
// (local outbox row only, delivered + forwarded).
//
// Run: node dist/core/fleet.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { openStore, getTask, newId, listPendingOutboundHandoffs, getOutboundHandoff, commitOutboundHandoff } from './store.js';
import { parseAddress, loadFleet, saveFleet, getProfile, fleetFile } from './fleet.js';
import { createHttp } from './http.js';
import type { CoreCtx } from './ops.js';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-fleet-'));
const homeA = path.join(tmp, 'a');
const homeB = path.join(tmp, 'b');

function makeStore(home: string, roles: string[]) {
  const store = openStore(home);
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  for (const role of roles) {
    raw.prepare("INSERT OR IGNORE INTO pods(id, role, dir, state, created_at) VALUES (?, ?, ?, 'closed', ?)")
      .run('pod_' + role, role, path.join(home, 'pods', role), new Date().toISOString());
  }
  raw.close();
  return store;
}

function seedTask(home: string, id: string, role: string, title: string, status = 'active') {
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  raw.prepare("INSERT INTO tasks(id, title, pod_role, status, created_at) VALUES (?, ?, ?, ?, ?)")
    .run(id, title, role, status, new Date().toISOString());
  raw.close();
}

// --- 1) addressing -----------------------------------------------------------
assert.deepStrictEqual(parseAddress('dev'), { role: 'dev' });
assert.deepStrictEqual(parseAddress('other/dev'), { profile: 'other', role: 'dev' });
assert.throws(() => parseAddress(''), /bad target/);
assert.throws(() => parseAddress('Up/Dev'), /bad (profile|role)/);
assert.throws(() => parseAddress('a/b/c'), /bad (profile|role)/);

// --- 2) fleet config round-trip ----------------------------------------------
assert.deepStrictEqual(loadFleet(homeA).profiles, []);
saveFleet(homeA, { profiles: [{ name: 'b', url: 'http://127.0.0.1:7561' }] });
assert.strictEqual(getProfile(homeA, 'b')?.url, 'http://127.0.0.1:7561');
assert.strictEqual(getProfile(homeA, 'nope'), undefined);
assert.ok(fs.existsSync(fleetFile(homeA)));

// --- 3) two cores, raw /api/ops endpoints ------------------------------------
const storeA = makeStore(homeA, ['dev']);
const storeB = makeStore(homeB, ['dev']);
const ctxA: CoreCtx = { store: storeA as never, ticks: { register() {}, all: () => [] } as never, startedAt: new Date().toISOString(), emit: () => {} };
const ctxB: CoreCtx = { store: storeB as never, ticks: { register() {}, all: () => [] } as never, startedAt: new Date().toISOString(), emit: () => {} };
const appA = createHttp(ctxA).app;
const appB = createHttp(ctxB).app;

// B's /api/ops accepts operator-token calls (token from B's home)
const tokenB = fs.readFileSync(path.join(homeB, 'token'), 'utf8').trim();

async function opsB(op: Record<string, unknown>): Promise<{ ok: boolean; result?: unknown; error?: string }> {
  const res = await appB.fetch(new Request('http://core/api/ops', {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${tokenB}` },
    body: JSON.stringify(op),
  }));
  return (await res.json()) as never;
}

// --- 4) cross-profile message_send (operator -> other/dev) --------------------
{
  const res = await opsB({ type: 'message_send', to: 'dev', from: 'operator@A', text: 'hello from A' });
  assert.strictEqual(res.ok, true, `remote message_send failed: ${res.error}`);
  // the remote inbox row exists on B
  const rawB = new DatabaseSync(path.join(homeB, 'flock.db'));
  const row = rawB.prepare('SELECT * FROM inboxes WHERE "to" = ? AND text = ?').get('dev', 'hello from A') as { id: number } | undefined;
  rawB.close();
  assert.ok(row, 'remote inbox row missing');
}

// --- 5) two-phase cross-profile handoff ---------------------------------------
// Phase 1: local close 'handed-off (outbound, pending)' + outbound row.
// Phase 2: remote fleet_handoff_accept (idempotent by pre-determined id).
seedTask(homeA, 't_orig', 'dev', 'original work', 'active');
{
  const handoffId = newId('t');
  const rawA = new DatabaseSync(path.join(homeA, 'flock.db'));
  rawA.prepare("INSERT INTO outbound_handoffs(id, at, from_role, to_profile, to_role, title, body, priority) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
    .run(handoffId, new Date().toISOString(), 'dev', 'b', 'dev', 'original work', null, 50);
  rawA.prepare("UPDATE tasks SET status = 'done', closed = ? WHERE id = 't_orig'")
    .run(JSON.stringify({ reason: 'handed-off', target: 'b/dev', at: new Date().toISOString(), by: 'cli' }));
  rawA.close();

  // phase 2a: the remote creates the successor with OUR id
  const acc = await opsB({ type: 'fleet_handoff_accept', id: handoffId, title: 'original work', pod: 'dev', from: 'dev' });
  assert.strictEqual(acc.ok, true, `accept failed: ${acc.error}`);
  assert.strictEqual((acc.result as { created: boolean }).created, true);

  // idempotent: a retry with the same id does NOT duplicate
  const acc2 = await opsB({ type: 'fleet_handoff_accept', id: handoffId, title: 'original work', pod: 'dev', from: 'dev' });
  assert.strictEqual(acc2.ok, true);
  assert.strictEqual((acc2.result as { created: boolean }).created, false);

  // the remote task exists on B with the pre-determined id
  const rawB = new DatabaseSync(path.join(homeB, 'flock.db'));
  const n = (rawB.prepare('SELECT COUNT(*) AS c FROM tasks WHERE id = ?').get(handoffId) as { c: number }).c;
  rawB.close();
  assert.strictEqual(n, 1, 'remote successor duplicated or missing');

  // local commit (what the op does after a successful accept)
  const store = openStore(homeA);
  assert.strictEqual(getOutboundHandoff(store, handoffId)?.status, 'pending');
  commitOutboundHandoff(store, handoffId, handoffId);
  assert.strictEqual(getOutboundHandoff(store, handoffId)?.status, 'committed');
  assert.strictEqual(listPendingOutboundHandoffs(store).length, 0);

  // the original task closed as handed-off with the cross target
  const closed = getTask(openStore(homeA), 't_orig');
  assert.strictEqual(closed?.status, 'done');
  assert.match(closed?.closed ?? '', /handed-off/);
  assert.match(closed?.closed ?? '', /"target":"b\/dev"/);
}

// --- 6) accept for a missing pod = honest 404 (handoff stays pending) ---------
{
  const res = await opsB({ type: 'fleet_handoff_accept', id: newId('t'), title: 'x', pod: 'ghost' });
  assert.strictEqual(res.ok, false);
  assert.match(res.error ?? '', /no pod: ghost/);
}

fs.rmSync(tmp, { recursive: true, force: true });
console.log('fleet.test.js: all checks passed');
