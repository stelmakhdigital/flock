// 5.4c: durable escalation ladder.
//
// pm triggers used to be fire-and-forget: pm pod dead -> the trigger is lost
// until the 5-minute sweep, and there is no audit of "why did it hang".
// Now every trigger opens (or absorbs into) a durable escalation row; a
// 30s tick walks the ladder:
//
//   open -> pm_notified (pm is live: the trigger is sent AND persisted)
//   open -> escalated   (pm not live: skip straight to the operator stage)
//   pm_notified -> escalated (pm stayed silent for FLOCK_ESC_PM_TIMEOUT_S)
//   escalated -> operator-visible: health alert + core.log (re-reminder every
//   FLOCK_ESC_REMINDER_S) until acknowledged or auto-resolved
//
// Auto-resolution: the tick re-checks the original condition (task left
// blocked, pod run alive, workflow instance left blocked) — the ladder
// closes itself when the world heals. Operator: `flock esc ack <id>`.
//
// The pattern mirrors the S5 conflict-resolution chain: a BDD state in the
// DB, a dedicated tick, emit events for the audit/UI.
import * as store from './store.js';
import type { CoreCtx } from './ops.js';

export interface EscOpts {
  pmTimeoutMs: number; // pm silence before operator escalation
  reminderMs: number; // operator re-reminder cadence while escalated
}

export function escOptsFromEnv(): EscOpts {
  return {
    pmTimeoutMs: Number(process.env.FLOCK_ESC_PM_TIMEOUT_S ?? 300) * 1000,
    reminderMs: Number(process.env.FLOCK_ESC_REMINDER_S ?? 3600) * 1000,
  };
}

export interface EscEvent {
  key: string;
  kind: string;
  detail: string;
  severity?: 'warn' | 'critical';
}

/** Open (or absorb into) a durable escalation. Never throws. */
export function openEscalation(ctx: CoreCtx, e: EscEvent): void {
  try {
    const r = store.upsertEscalation(ctx.store, {
      key: e.key,
      kind: e.kind,
      subject: e.detail.slice(0, 300),
      severity: e.severity,
    });
    ctx.emit?.({ type: 'escalation_opened', id: r.id, key: e.key, kind: e.kind, severity: e.severity ?? 'warn' });
  } catch (err) {
    console.warn('[core] escalation open failed:', err instanceof Error ? err.message : err);
  }
}

// ---- auto-resolution: the condition that opened the ladder has cleared ----

function isHealed(ctx: CoreCtx, row: store.Escalation): string | null {
  try {
    if (row.kind === 'task_blocked' || row.kind === 'task_needs' || row.kind === 'gate_red') {
      const m = row.key.match(/^task:(.+)$/);
      if (!m) return null;
      const t = store.getTask(ctx.store, m[1]);
      if (!t) return 'task gone';
      if (t.status !== 'blocked' && t.status !== 'needs') return `task is ${t.status}`;
      return null;
    }
    if (row.kind === 'pod_crashed') {
      const m = row.key.match(/^pod:(.+):crashed$/);
      if (!m) return null;
      const run = store.currentRun(ctx.store, m[1]);
      if (run && !run.ended_at) return 'pod run alive again';
      return null;
    }
    if (row.kind === 'workflow_blocked') {
      const m = row.key.match(/^wf:(.+)$/);
      if (!m) return null;
      const inst = store.getWorkflowInstance(ctx.store, m[1]);
      if (!inst) return 'instance gone';
      if (inst.state !== 'blocked') return `instance is ${inst.state}`;
      return null;
    }
    return null;
  } catch {
    return null; // healing check is best-effort
  }
}

// ---- the ladder tick (30s, registered in main.ts) ----

export async function runEscalationTick(ctx: CoreCtx): Promise<void> {
  const opts = escOptsFromEnv();
  const now = Date.now();
  for (const row of store.listEscalations(ctx.store, true)) {
    // heal first: the world moved on — close the ladder
    const healed = isHealed(ctx, row);
    if (healed) {
      store.setEscalationState(ctx.store, row.id, 'resolved', { resolvedReason: healed });
      ctx.emit?.({ type: 'escalation_resolved', id: row.id, key: row.key, reason: healed });
      continue;
    }
    if (row.state === 'open') {
      const pm = store.getPodByRole(ctx.store, 'pm');
      if (pm && pm.state === 'live') {
        store.setEscalationState(ctx.store, row.id, 'pm_notified', { pmNotifiedAt: store.nowIso(), attempts: row.attempts + 1 });
        ctx.emit?.({ type: 'escalation_pm_notified', id: row.id, key: row.key });
        // pm already got the trigger via pmNotify; mark the ladder rung so
        // the pm-silence timer starts now
        store.setEscalationState(ctx.store, row.id, 'pm_notified', { pmNotifiedAt: row.pm_notified_at ?? store.nowIso(), attempts: row.attempts });
      } else {
        // no live pm: skip the pm stage, the operator is the next rung
        store.setEscalationState(ctx.store, row.id, 'escalated', { attempts: row.attempts + 1 });
        ctx.emit?.({ type: 'escalation_escalated', id: row.id, key: row.key, reason: 'pm not live' });
        operatorAlert(ctx, row, 'escalated (pm not live)');
      }
    } else if (row.state === 'pm_notified') {
      const notifiedAt = row.pm_notified_at ? Date.parse(row.pm_notified_at) : NaN;
      if (!Number.isNaN(notifiedAt) && now - notifiedAt > opts.pmTimeoutMs) {
        store.setEscalationState(ctx.store, row.id, 'escalated');
        ctx.emit?.({ type: 'escalation_escalated', id: row.id, key: row.key, reason: `pm silent for ${Math.round(opts.pmTimeoutMs / 60000)} min` });
        operatorAlert(ctx, row, `pm silent for ${Math.round(opts.pmTimeoutMs / 60000)} min`);
      }
    } else if (row.state === 'escalated') {
      // re-reminder cadence so the operator cannot scroll past it once
      const updated = Date.parse(row.updated_at);
      if (!Number.isNaN(updated) && now - updated >= opts.reminderMs) {
        operatorAlert(ctx, row, 're-reminder');
      }
    }
  }
}

// The operator rung: a loud, durable, visible signal (health alert + log).
// alertUpsert is local to health.ts, so the row is written here directly
// (same table, same shape the UI already renders).
function operatorAlert(ctx: CoreCtx, row: store.Escalation, note: string): void {
  const nowIso = store.nowIso();
  const db = ctx.store.db;
  try {
    const prev = db.prepare('SELECT 1 AS x FROM health_alerts WHERE pod_role = ? AND kind = ? AND ref = ?').get('escalation', 'ladder', row.key);
    if (prev) {
      db.prepare('UPDATE health_alerts SET state = ?, note = ?, last_at = ? WHERE pod_role = ? AND kind = ? AND ref = ?')
        .run('alert', note, nowIso, 'escalation', 'ladder', row.key);
    } else {
      db.prepare('INSERT INTO health_alerts (pod_role, kind, ref, state, count, first_at, last_at, note) VALUES (?, ?, ?, ?, 0, ?, ?, ?)')
        .run('escalation', 'ladder', row.key, 'alert', nowIso, nowIso, note);
    }
    console.warn(`[core] ESCALATION [${row.severity}] ${row.key}: ${row.subject} — ${note}`);
    ctx.emit?.({ type: 'escalation_operator_alert', id: row.id, key: row.key, note });
  } catch (e) {
    console.warn('[core] escalation operator alert failed:', e instanceof Error ? e.message : e);
  }
}
