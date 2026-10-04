// 5.6: campaigns — lifecycle tick + ops, hermetic checks (no tmux, no
// live core, no LLM). Run: node dist/core/campaign.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import {
  openStore,
  createCampaign,
  getCampaign,
  setCampaignStatus,
  campaignTasks,
  insertTask,
  newId,
  getTask,
  listCampaigns,
  campaignIdFromGoal,
} from './store.js';
import { apply } from './ops.js';
import { runCampaignTick } from './campaign-tick.js';
import type { CoreCtx } from './ops.js';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-camp-'));
const db = openStore(home);

// a pod to attach tasks to
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  raw.prepare("INSERT OR IGNORE INTO pods(id, role, dir, state, created_at) VALUES ('pod_dev', 'dev', ?, 'closed', ?)")
    .run(path.join(home, 'pods', 'dev'), new Date().toISOString());
  raw.close();
}

const ctx: CoreCtx = {
  store: db as never,
  ticks: { register() {}, all: () => [] } as never,
  startedAt: new Date().toISOString(),
  emit: () => {},
};

function addTask(role: string, campaignId: string | null): string {
  const id = newId('t');
  insertTask(db, { id, title: `task ${id}`, body: null, podRole: role, campaignId });
  return id;
}

// --- 1) planning -> running (a task appeared) --------------------------------
const c1 = createCampaign(db, { id: 'alpha', goal: 'goal A' });
assert.strictEqual(c1.status, 'planning');
addTask('dev', 'alpha');
await runCampaignTick(ctx);
assert.strictEqual(getCampaign(db, 'alpha')?.status, 'running');

// --- 2) running -> done (all tasks done, at least one done) -------------------
for (const t of campaignTasks(db, 'alpha')) {
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  raw.prepare("UPDATE tasks SET status = 'done', finished_at = ?, closed = ? WHERE id = ?")
    .run(new Date().toISOString(), JSON.stringify({ reason: 'finished', at: new Date().toISOString(), by: 'test' }), t.id);
  raw.close();
}
await runCampaignTick(ctx);
assert.strictEqual(getCampaign(db, 'alpha')?.status, 'done');

// --- 3) running -> blocked (nothing open, something stuck) + escalation -------
const c2 = createCampaign(db, { id: 'beta', goal: 'goal B' });
const bt1 = addTask('dev', 'beta');
await runCampaignTick(ctx); // planning -> running
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  raw.prepare("UPDATE tasks SET status = 'blocked' WHERE id = ?").run(bt1);
  raw.close();
}
await runCampaignTick(ctx);
assert.strictEqual(getCampaign(db, 'beta')?.status, 'blocked');
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  const esc = raw.prepare("SELECT * FROM escalations WHERE key = 'campaign:beta:stuck'").get() as { id: number } | undefined;
  raw.close();
  assert.ok(esc, 'stalled campaign must open an escalation (5.4c ladder)');
}

// --- 4) blocked -> running (pm unblocked: an open task appeared) --------------
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  raw.prepare("UPDATE tasks SET status = 'active' WHERE id = ?").run(bt1);
  raw.close();
}
await runCampaignTick(ctx);
assert.strictEqual(getCampaign(db, 'beta')?.status, 'running');

// --- 5) all cancelled (zero done) -> blocked, not done ------------------------
const c3 = createCampaign(db, { id: 'gamma', goal: 'goal C' });
const gt = addTask('dev', 'gamma');
await runCampaignTick(ctx); // -> running
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  raw.prepare("UPDATE tasks SET status = 'cancelled', finished_at = ?, closed = ? WHERE id = ?")
    .run(new Date().toISOString(), JSON.stringify({ reason: 'canceled', at: new Date().toISOString(), by: 'test' }), gt);
  raw.close();
}
await runCampaignTick(ctx);
assert.strictEqual(getCampaign(db, 'gamma')?.status, 'blocked');

// --- 6) planning TTL -> blocked (synthetic old created_at) --------------------
const c4 = createCampaign(db, { id: 'delta', goal: 'goal D' });
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  const old = new Date(Date.now() - 20 * 60_000).toISOString();
  raw.prepare('UPDATE campaigns SET created_at = ? WHERE id = ?').run(old, 'delta');
  raw.close();
}
await runCampaignTick(ctx);
assert.strictEqual(getCampaign(db, 'delta')?.status, 'blocked');
assert.match(getCampaign(db, 'delta')?.note ?? '', /не декомпозировал/);

// --- 7) paused/cancelled/done are terminal for the tick ------------------------
setCampaignStatus(db, 'beta', 'paused');
await runCampaignTick(ctx);
assert.strictEqual(getCampaign(db, 'beta')?.status, 'paused'); // untouched

// --- 8) ops: validation ---------------------------------------------------------
await assert.rejects(apply({ type: 'campaign_new', goal: '  ' }, ctx), /goal required/);
await assert.rejects(apply({ type: 'campaign_pause', id: 'alpha' }, ctx), /cannot pause/); // alpha is done
await assert.rejects(apply({ type: 'campaign_cancel', id: 'alpha' }, ctx), /cannot cancel/);
await assert.rejects(apply({ type: 'campaign_resume', id: 'alpha' }, ctx), /only paused/);
await assert.rejects(apply({ type: 'campaign_pause', id: 'ghost' }, ctx), /no campaign/);

// pause -> resume (beta: paused in step 7 with an open task -> running)
const resumed = (await apply({ type: 'campaign_resume', id: 'beta' }, ctx)) as { status: string };
assert.strictEqual(resumed.status, 'running');
const paused = (await apply({ type: 'campaign_pause', id: 'beta' }, ctx)) as { status: string };
assert.strictEqual(paused.status, 'paused');
await assert.rejects(apply({ type: 'campaign_pause', id: 'beta' }, ctx), /cannot pause/); // already paused
await apply({ type: 'campaign_resume', id: 'beta' }, ctx); // leave it running for later checks

// task_add campaign validation
await assert.rejects(apply({ type: 'task_add', role: 'dev', title: 'x', campaign_id: 'ghost' }, ctx), /no campaign/);
await assert.rejects(apply({ type: 'task_add', role: 'dev', title: 'x', campaign_id: 'alpha' }, ctx), /done/); // done campaign

// task_add with a valid campaign
const added = (await apply({ type: 'task_add', role: 'dev', title: 'into beta', campaign_id: 'beta' }, ctx)) as { id: string; campaign_id: string };
assert.strictEqual(added.campaign_id, 'beta');
assert.strictEqual(getTask(db, added.id)?.campaign_id, 'beta');

// --- 9) handoff keeps the campaign (successor inherits campaign_id) -----------
{
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  raw.prepare('UPDATE tasks SET status = ? WHERE id = ?').run('active', added.id);
  raw.close();
}
const ho = (await apply({ type: 'task_handoff', id: added.id, to: 'dev' }, ctx)) as { next: { id: string } };
assert.strictEqual(getTask(db, ho.next.id)?.campaign_id, 'beta', 'handoff successor must stay in the campaign');

// --- 10) ls/status surface + slug id with collision ----------------------------
const lsRes = (await apply({ type: 'campaign_ls' }, ctx)) as { campaigns: { id: string; done: number; total: number }[] };
assert.ok(lsRes.campaigns.some((c) => c.id === 'beta'));
const st = (await apply({ type: 'campaign_status', id: 'beta' }, ctx)) as { campaign: { id: string }; tasks: unknown[] };
assert.strictEqual(st.campaign.id, 'beta');
assert.ok(st.tasks.length >= 1);

const slug1 = campaignIdFromGoal(db.db, 'Make the thing work');
assert.match(slug1, /^make-the-thing-work$/);
createCampaign(db, { id: slug1, goal: 'duplicate goal' });
const slug2 = campaignIdFromGoal(db.db, 'Make the thing work');
assert.strictEqual(slug2, `${slug1}-2`);

// listCampaigns(false) = active only (done/cancelled excluded)
const active = listCampaigns(db, false).map((c) => c.id);
assert.ok(!active.includes('alpha'), 'done campaign must be out of the active list');

fs.rmSync(home, { recursive: true, force: true });
console.log('campaign.test.js: all checks passed');
