// 5.6: campaign orchestrator tick — deterministic lifecycle walk (NO LLM).
//
// A campaign is a named persistent goal: planning -> running ->
// (blocked <-> running) -> done/cancelled. The tick decides ONLY the
// lifecycle; the actual work (decompose, unblock, re-plan) is the pm's,
// through ordinary ops. Hangs go through the 5.4c escalation ladder
// (durable: pm -> operator), never a silent stall.
//
// Rules (first match per campaign):
//  planning  -> running: at least one task with this campaign_id exists.
//  planning  -> blocked: FLOCK_CAMPAIGN_PLAN_TTL_MIN (default 15) without
//              a single task -> escalation, note "pm не декомпозировал".
//  running   -> done: all tasks terminal (done|cancelled) and at least
//              one done.
//  running   -> blocked: no open work left to run (nothing queued/active)
//              and something is stuck (blocked/needs) OR everything is
//              cancelled (no done) -> escalation with what is stuck.
//  blocked   -> running: an open task appeared again (pm unblocked or
//              added one) — the ladder auto-resolves on the next heal.
//  paused/cancelled/done — terminal for the tick (resume = operator).
import * as store from './store.js';
import { openEscalation } from './escalation.js';
import type { CoreCtx } from './ops.js';

const PLAN_TTL_MIN = Number(process.env.FLOCK_CAMPAIGN_PLAN_TTL_MIN ?? 15);

export async function runCampaignTick(ctx: CoreCtx): Promise<void> {
  const now = Date.now();
  for (const c of store.listCampaigns(ctx.store, false)) {
    const tasks = store.campaignTasks(ctx.store, c.id);
    switch (c.status) {
      case 'planning': {
        if (tasks.length > 0) {
          moveTo(ctx, c, 'running', 'задачи появились — декомпозиция началась');
        } else if (now - new Date(c.created_at).getTime() > PLAN_TTL_MIN * 60_000) {
          moveTo(ctx, c, 'blocked', 'pm не декомпозировал за TTL');
          openEscalation(ctx, {
            key: `campaign:${c.id}:planning`,
            kind: 'campaign_planning_stalled',
            detail: `кампания ${c.id} не декомпозирована за ${PLAN_TTL_MIN} мин (goal: ${c.goal.slice(0, 120)})`,
            severity: 'warn',
          });
        }
        break;
      }
      case 'running': {
        const terminal = tasks.filter((t) => t.status === 'done' || t.status === 'cancelled');
        const open = tasks.filter((t) => t.status === 'queued' || t.status === 'active');
        const stuck = tasks.filter((t) => t.status === 'blocked' || t.status === 'needs');
        if (tasks.length > 0 && terminal.length === tasks.length) {
          const done = tasks.filter((t) => t.status === 'done').length;
          if (done > 0) {
            moveTo(ctx, c, 'done', `все задачи закрыты (done ${done}/${tasks.length})`);
            ctx.emit?.({ type: 'campaign_status', id: c.id, to: 'done', from: 'running', reason: `done ${done}/${tasks.length}` });
          } else {
            moveTo(ctx, c, 'blocked', 'нет ни одной done-задачи (всё отменено)');
            openEscalation(ctx, {
              key: `campaign:${c.id}:no-done`,
              kind: 'campaign_no_done',
              detail: `кампания ${c.id}: все задачи отменены, цель не достигнута`,
              severity: 'warn',
            });
          }
          break;
        }
        if (open.length === 0 && stuck.length > 0) {
          const names = stuck.map((t) => `${t.id}(${t.status})`).join(', ');
          moveTo(ctx, c, 'blocked', `застряло: ${names}`);
          openEscalation(ctx, {
            key: `campaign:${c.id}:stuck`,
            kind: 'campaign_stuck',
            detail: `кампания ${c.id}: нет открытой работы, застряло: ${names}`,
            severity: 'warn',
          });
        }
        break;
      }
      case 'blocked': {
        const open = tasks.filter((t) => t.status === 'queued' || t.status === 'active');
        if (open.length > 0) {
          moveTo(ctx, c, 'running', `pm разблокировал: открыто ${open.length}`);
        }
        break;
      }
      default:
        break; // paused/cancelled/done — terminal for the tick
    }
  }
}

function moveTo(ctx: CoreCtx, c: store.Campaign, to: store.Campaign['status'], note: string): void {
  const from = c.status;
  store.setCampaignStatus(ctx.store, c.id, to, note);
  ctx.emit?.({ type: 'campaign_status', id: c.id, from, to, reason: note });
  console.log(`[core] campaigns: ${c.id} ${from} -> ${to} (${note})`);
}
