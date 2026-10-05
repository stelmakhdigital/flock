// messages (C4): inboxes + outboxes — hermetic: durable send, list, claim,
// pod-scope isolation, dead-target durability. Run: node dist/core/messages.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { openStore } from './store.js';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-msg-'));
const db = openStore(home);
const now = new Date().toISOString();
const raw = new DatabaseSync(path.join(home, 'flock.db'));
for (const [role, state] of [['dev', 'live'], ['q', 'closed']] as const) {
  raw.prepare('INSERT INTO pods(id, role, dir, state, created_at) VALUES (?, ?, ?, ?, ?)')
    .run('pod_' + role, role, path.join(home, 'pods', role), state, now);
}
raw.close();

const { apply } = await import('./ops.js');
// ctx without emit: messageSend must not need a live pane (poke is best-effort)
const ctx = { store: db, ticks: {} } as never;

// 1) operator -> dev: inbox row + outbox row (pending=0, local)
const s1 = (await apply({ type: 'message_send', to: 'dev', text: 'привет' }, ctx)) as { ok: boolean; inboxId: number; poked: string | boolean };
assert.ok(s1.ok, 'send ok');
assert.ok(s1.poked === 'failed' || s1.poked === 'target-not-live', 'no live pane in hermetic ctx: poke is best-effort (the inbox row is the record)');
const rows = new DatabaseSync(path.join(home, 'flock.db'));
const inbox = rows.prepare('SELECT * FROM inboxes').all() as unknown as { id: number; to: string; from_: string; text: string; claimed: number }[];
assert.strictEqual(inbox.length, 1);
assert.strictEqual(inbox[0].to, 'dev');
assert.strictEqual(inbox[0].from_, 'operator');
assert.strictEqual(inbox[0].claimed, 0);
const outbox = rows.prepare('SELECT * FROM outboxes').all() as unknown as { pending: number }[];
assert.strictEqual(outbox.length, 1);
assert.strictEqual(outbox[0].pending, 0, 'local send: outbox immediately forwarded (pending=0)');

// 2) message_list (operator, own-role filter + unclaimed)
const l1 = (await apply({ type: 'message_list', to: 'dev' }, ctx)) as { messages: { id: number; text: string }[] };
assert.strictEqual(l1.messages.length, 1);
assert.strictEqual(l1.messages[0].text, 'привет');
const l2 = (await apply({ type: 'message_list', to: 'dev', unclaimed: true }, ctx)) as { messages: unknown[] };
assert.strictEqual(l2.messages.length, 1);

// 3) message_claim
const c1 = (await apply({ type: 'message_claim', id: inbox[0].id }, ctx)) as { ok: boolean };
assert.ok(c1.ok);
const l3 = (await apply({ type: 'message_list', to: 'dev', unclaimed: true }, ctx)) as { messages: unknown[] };
assert.strictEqual(l3.messages.length, 0, 'claimed message leaves the unclaimed view');

// 4) unknown target / empty text
await assert.rejects(() => apply({ type: 'message_send', to: 'ghost', text: 'x' }, ctx), /no pod/);
await assert.rejects(() => apply({ type: 'message_send', to: 'dev', text: '  ' }, ctx), /text required/);

// 5) pod scope: own inbox only, cannot address self
const podCtx = { store: db, ticks: {}, caller: { kind: 'pod' as const, role: 'dev' } } as never;
await assert.rejects(() => apply({ type: 'message_send', to: 'dev', text: 'self' }, podCtx), /cannot message your own pod/);
const s2 = (await apply({ type: 'message_send', to: 'q', text: 'от dev' }, podCtx)) as { ok: boolean };
assert.ok(s2.ok, 'pod -> another pod (target closed: message is durable, poked=failed or target-not-live)');
const l4 = (await apply({ type: 'message_list' }, podCtx)) as { to: string; messages: { from_: string }[] };
assert.strictEqual(l4.to, 'dev', 'pod token lists its own inbox');
assert.strictEqual(l4.messages.length, 1, 'dev sees its own unclaimed message (the poke failed, the inbox is the record)');
// a pod cannot read/claim another pod's inbox
await assert.rejects(() => apply({ type: 'message_claim', id: 2 }, podCtx), /belongs to pod/);

rows.close();

// 6) broadcast: the team «chatroom» — one durable row per pod over the same
// inbox path as message_send (no second contour)
{
  // to all pods (dev=live, q=closed): both get a durable row; poke is best-effort
  const b1 = (await apply({ type: 'message_broadcast', text: 'всем привет' }, ctx)) as { ok: boolean; count: number; inboxIds: number[]; poked: Record<string, unknown> };
  assert.ok(b1.ok);
  assert.strictEqual(b1.count, 2);
  assert.strictEqual(b1.inboxIds.length, 2);
  assert.strictEqual(b1.poked.q, 'target-not-live', 'closed pod: durable row, no poke');
  assert.ok(b1.poked.dev === true || b1.poked.dev === 'failed', 'live pod: poke best-effort (no pane in hermetic ctx)');
  const rows2 = new DatabaseSync(path.join(home, 'flock.db'));
  const per = rows2.prepare('SELECT "to", COUNT(*) AS n FROM inboxes WHERE text = ? GROUP BY "to"').all('всем привет') as { to: string; n: number }[];
  rows2.close();
  assert.deepStrictEqual(per.map((r) => ({ to: r.to, n: r.n })).sort((a, b) => a.to.localeCompare(b.to)), [{ n: 1, to: 'dev' }, { n: 1, to: 'q' }], 'exactly one inbox row per pod');

  // to a subset: only the named pod gets a row
  const b2 = (await apply({ type: 'message_broadcast', text: 'только dev', roles: ['dev'] }, ctx)) as { ok: boolean; count: number };
  assert.strictEqual(b2.count, 1);
  const rows3 = new DatabaseSync(path.join(home, 'flock.db'));
  const n = (rows3.prepare('SELECT COUNT(*) AS n FROM inboxes WHERE text = ?').get('только dev') as { n: number }).n;
  rows3.close();
  assert.strictEqual(n, 1, 'subset broadcast: one row, only for the named pod');

  // unknown role: 404 BEFORE anything is written
  await assert.rejects(() => apply({ type: 'message_broadcast', text: 'x', roles: ['ghost'] }, ctx), /no pod/);
  await assert.rejects(() => apply({ type: 'message_broadcast', text: '  ' }, ctx), /text required/);

  // pod scope: a random pod cannot broadcast to the team (pm/operator only)
  await assert.rejects(() => apply({ type: 'message_broadcast', text: 'spam' }, podCtx), /operator\/pm/);
}

fs.rmSync(home, { recursive: true, force: true });
console.log('messages: all checks passed');
