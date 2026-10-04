// fleet tick (C15): retry pending cross-profile work (watchdog semantics).
//
// ponytail: an atomic cross-DB handoff is impossible under the single-
// writer invariant (two cores = two writers = two DBs). The ceiling is an
// EVENTUAL handoff with an explicit pending state; true atomicity means a
// shared store — a different architecture, not now.
//
// Two queues, both eventual:
//  1. outbound_handoffs (two-phase cross-profile task handoff): phase 1
//     (local close 'handed-off (outbound, pending)') is already committed;
//     this tick retries PHASE 2 — the remote `fleet_handoff_accept`
//     (idempotent: same task id, the remote creates the successor exactly
//     once). Success -> local 'committed'. Budget exhausted ->
//     'attention_required' + escalation (the operator sees it and acts).
//  2. cross-profile outbox messages: resend `message_send` to the remote
//     core until it lands. A redelivery is an acceptable duplicate (a
//     message twice is better than lost; inbox rows are auditable and
//     claimed by the owner). Budget exhausted -> escalation; the row
//     stays pending with the last error (honest, visible, not deleted).
import * as store from './store.js';
import * as fleet from './fleet.js';
import { openEscalation } from './escalation.js';
import type { CoreCtx } from './ops.js';

// retry budgets (FLOCK-tunable like the rest of the tuning surface)
const HANDOFF_MAX_ATTEMPTS = Number(process.env.FLOCK_FLEET_HANDOFF_MAX_ATTEMPTS ?? 10);
const MESSAGE_MAX_ATTEMPTS = Number(process.env.FLOCK_FLEET_MESSAGE_MAX_ATTEMPTS ?? 10);

export async function runFleetTick(ctx: CoreCtx): Promise<void> {
  await retryOutboundHandoffs(ctx);
  await retryCrossProfileMessages(ctx);
}

async function retryOutboundHandoffs(ctx: CoreCtx): Promise<void> {
  for (const h of store.listPendingOutboundHandoffs(ctx.store)) {
    // phase 2: idempotent on the remote (the task id is fixed by us,
    // created in phase 1 — the remote either has it or creates it)
    const res = await fleet.remoteApply(ctx.store.home, h.to_profile, {
      type: 'fleet_handoff_accept',
      id: h.id,
      title: h.title,
      body: h.body,
      pod: h.to_role,
      priority: h.priority,
      from: h.from_role,
    });
    const attempts = h.attempts + 1;
    if (res.ok) {
      store.commitOutboundHandoff(ctx.store, h.id, h.id);
      ctx.emit?.({ type: 'fleet_handoff_committed', id: h.id, to: `${h.to_profile}/${h.to_role}`, attempts });
      console.log(`[core] fleet: handoff ${h.id} committed to ${h.to_profile}/${h.to_role} (attempt ${attempts})`);
    } else {
      const attention = attempts >= HANDOFF_MAX_ATTEMPTS;
      store.failOutboundHandoffAttempt(ctx.store, h.id, res.error ?? 'unknown', attention);
      ctx.emit?.({
        type: attention ? 'fleet_handoff_attention' : 'fleet_handoff_retry',
        id: h.id,
        to: `${h.to_profile}/${h.to_role}`,
        attempts,
        error: res.error,
      });
      if (attention) {
        openEscalation(ctx, {
          key: `fleet_handoff:${h.id}`,
          kind: 'fleet_handoff_stalled',
          detail: `cross-profile handoff ${h.id} -> ${h.to_profile}/${h.to_role} stalled after ${attempts} attempts: ${res.error}`,
          severity: 'critical',
        });
        console.warn(`[core] fleet: handoff ${h.id} -> ${h.to_profile}/${h.to_role} ATTENTION REQUIRED after ${attempts} attempts`);
      } else {
        console.warn(`[core] fleet: handoff ${h.id} -> ${h.to_profile}/${h.to_role} retry ${attempts}/${HANDOFF_MAX_ATTEMPTS}: ${res.error}`);
      }
    }
  }
}

async function retryCrossProfileMessages(ctx: CoreCtx): Promise<void> {
  for (const m of store.listPendingOutboxMessages(ctx.store)) {
    let addr: fleet.Address;
    try {
      addr = fleet.parseAddress(m.to);
    } catch {
      continue; // not a valid address — not this queue's problem
    }
    if (!addr.profile) continue; // local pending (legacy/other) — not ours
    // strip retry annotations before resending (the row keeps its history)
    const cleanText = m.text.replace(/\n\[delivery error @[^]]*\]\s*$/g, '').trim();
    const res = await fleet.remoteApply(ctx.store.home, addr.profile, {
      type: 'message_send',
      to: addr.role,
      from: m.from_,
      text: cleanText,
    });
    const attempts = m.attempts + 1;
    if (res.ok) {
      store.markOutboxForwarded(ctx.store, m.id);
      ctx.emit?.({ type: 'fleet_message_delivered', outboxId: m.id, to: m.to, attempts });
      console.log(`[core] fleet: message ${m.id} -> ${m.to} delivered (attempt ${attempts})`);
    } else {
      store.bumpOutboxAttempt(ctx.store, m.id, res.error ?? 'unknown');
      if (attempts >= MESSAGE_MAX_ATTEMPTS) {
        openEscalation(ctx, {
          key: `fleet_message:${m.id}`,
          kind: 'fleet_message_stalled',
          detail: `cross-profile message ${m.id} -> ${m.to} undelivered after ${attempts} attempts: ${res.error}`,
          severity: 'warn',
        });
        ctx.emit?.({ type: 'fleet_message_stalled', outboxId: m.id, to: m.to, attempts });
        console.warn(`[core] fleet: message ${m.id} -> ${m.to} STALLED after ${attempts} attempts (row stays pending)`);
      } else {
        console.warn(`[core] fleet: message ${m.id} -> ${m.to} retry ${attempts}/${MESSAGE_MAX_ATTEMPTS}: ${res.error}`);
      }
    }
  }
}
