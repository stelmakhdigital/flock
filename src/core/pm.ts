// C8: the pm coordination layer. The pm is a REGULAR pod (spawned like any
// other, agent manifest 'pm'); this module is the delivery side of the
// coordination plane:
//   - notifyPm: a durable message to the pm inbox (message_send) + a poke
//     through the verified transport when the pm is live. pm dead -> the
//     message waits in the inbox, never lost.
//   - openEscalationFromEvent: the HANG-WORTHY kinds (pod crash, blocked /
//     needs tasks) open a durable escalation FIRST (5.4c ladder: pm ->
//     operator, restart-proof audit of "why did it hang").
//   - pmDigest: what the pm sees via `flock pm state` (read-only).
// The 5-minute sweep and the pm-as-core-subsystem are gone: the pm is woken
// by interest events (inbox/poke) like any other pod.

import * as store from './store.js';
import { apply, OpError, adapterForPod, type CoreCtx } from './ops.js';

export const PM_ROLE = 'pm';

export interface PmTrigger {
  type: string; // task_added | task_done | task_blocked | task_needs | task_cancelled | pod_crashed | pod_woken
  detail: string;
  subject?: string; // explicit subject id (task/pod) — overrides detail-parsing
}

/**
 * Deliver a pm interest event: durable inbox message ALWAYS (+ a poke to the
 * live pm through the same verified transport as pod_send). The 5.4c ladder
 * opens for the hang-worthy kinds BEFORE delivery, so a dead pm loses
 * nothing: runEscalationTick walks pm -> operator and the audit survives.
 */
export async function notifyPm(ctx: CoreCtx, t: PmTrigger): Promise<boolean> {
  const key = pmEscalationKey(t);
  if (key) {
    // only hang-worthy kinds start a ladder; task_added/done/pod_woken are
    // informational (delivered, not audited)
    if (t.type === 'pod_crashed' || t.type === 'task_blocked' || t.type === 'task_needs') {
      const { openEscalation } = await import('./escalation.js');
      openEscalation(ctx, {
        key,
        kind: t.type,
        detail: t.detail,
        severity: t.type === 'pod_crashed' ? 'critical' : 'warn',
      });
    }
  }
  const pod = store.getPodByRole(ctx.store, PM_ROLE);
  const text = `[flock] ${t.type}: ${t.detail}\nСмотри \`flock pm state\` / \`flock message ls\`. Решение — твоими ops (task_*, pod_*, workflow_start, message_send); pm-intents больше нет.`;
  try {
    const r = await apply({ type: 'message_send', to: PM_ROLE, text }, ctx) as { poked?: boolean | string };
    return r?.poked === true;
  } catch {
    return false; // no pm pod yet: the message op 404s; the event is still in the event log
  }
}

function pmEscalationKey(t: PmTrigger): string | null {
  const subject = t.subject
    ?? (t.type.startsWith('task_') ? (t.detail.match(/^таск (t_\w+)/)?.[1] ?? null)
    : t.type === 'pod_crashed' ? (t.detail.match(/^под (\S+):/)?.[1] ?? null)
    : null);
  if (!subject) return null;
  if (t.type === 'pod_crashed') return `pod:${subject}:crashed`;
  if (t.type.startsWith('task_')) return `task:${subject}`;
  return null;
}

// ---- pm state (what the pm sees via `flock pm state`) ---------------------------

export async function pmDigest(ctx: CoreCtx): Promise<Record<string, unknown>> {
  const s = ctx.store;
  const tasks = store.listTasks(s);
  const byStatus: Record<string, number> = {};
  for (const t of tasks) byStatus[t.status] = (byStatus[t.status] ?? 0) + 1;
  const open = tasks.filter((t) => t.status === 'active' || t.status === 'needs' || t.status === 'blocked');
  // queued tasks waiting on a CLOSED pod (zombie queue) — visible so the pm
  // sees the arbiter's wake-ups
  const liveRoles = new Set(store.listPods(s).filter((p) => p.state === 'live').map((p) => p.role));
  const waitingOnClosed = tasks
    .filter((t) => t.status === 'queued' && !liveRoles.has(t.pod_role))
    .map((t) => ({ id: t.id, title: t.title.slice(0, 80), pod: t.pod_role }));
  // Runtime-agnostic pod activity: the adapter's healthProbe answers for any
  // runtime (pi: sidecar ready/streaming).
  const pods = await Promise.all(
    store.listPods(s).map(async (p) => {
      const resolved = p.state === 'live' ? adapterForPod(p, ctx) : null;
      let activity: 'busy' | 'idle' | 'not-ready' | null = null;
      if (resolved?.adapter.healthProbe) {
        try {
          const probe = await resolved.adapter.healthProbe(resolved.binding);
          if (probe) activity = probe.busy ? 'busy' : probe.ready ? 'idle' : 'not-ready';
        } catch {
          activity = null; // transient probe failure: unknown, not an error
        }
      }
      return { role: p.role, state: p.state, agent: p.agent, activity };
    }),
  );
  const runs = store.listRuns(s).filter((r) => !r.ended_at).map((r) => ({ pod: r.pod_role, run: r.id, started_at: r.started_at }));
  const messages = store.listInboxMessages(s, PM_ROLE, true, 20);
  return {
    at: store.nowIso(),
    tasks: byStatus,
    openTasks: open.map((t) => ({ id: t.id, title: t.title.slice(0, 80), status: t.status, pod: t.pod_role, claimed_at: t.claimed_at ?? null, closed: t.closed })),
    waitingOnClosed,
    pods,
    liveRuns: runs,
    unclaimedMessages: messages.map((m) => ({ id: m.id, from: m.from_, at: m.at, text: m.text.slice(0, 300) })),
  };
}

// ---- intent whitelist (legacy pm-intents path) --------------------------------
// C8: the pm is a regular pod — it already holds the pod-scoped ops
// (task_*, pod_*, workflow_*, message_*) through its own token. The
// pm-intents whitelist exists so EXISTING pm agents keep working; it is a
// compatibility shim, not a core subsystem.

const INTENT_OPS = new Set([
  'task_add',
  'task_done',
  'task_blocked',
  'task_needs',
  'task_cancel',
  'task_unblock',
  'task_handoff',
  'message_send',
  'pod_send',
  'pod_relaunch',
  'pod_spawn',
  'pod_close',
  'workflow_start',
]);

export interface IntentResult {
  op: string;
  ok: boolean;
  detail: string;
}

/** Validate one intent: known op, required fields per op. Returns an error string or null. */
export function validateIntent(i: Record<string, unknown>): string | null {
  const op = String(i.op ?? '');
  if (!INTENT_OPS.has(op)) return `op not in pm whitelist: ${op || '(empty)'}`;
  if (op === 'task_add') {
    if (!String(i.title ?? '').trim()) return 'task_add: title required';
    if (i.pod_role !== undefined && !/^[a-z0-9][a-z0-9-]{0,30}$/.test(String(i.pod_role))) return 'task_add: bad pod_role';
  }
  if (op === 'task_done' || op === 'task_blocked' || op === 'task_needs' || op === 'task_cancel' || op === 'task_unblock' || op === 'task_handoff') {
    if (!String(i.id ?? '').trim()) return `${op}: id required`;
    if (op === 'task_done') {
      // C3: hot-potato — the closure reason is required
      if (!String(i.reason ?? '').trim()) return 'task_done: reason required (closure vocabulary)';
      return null;
    }
    if (op === 'task_handoff') return /^[a-z0-9][a-z0-9-]{0,30}$/.test(String(i.to ?? '')) ? null : 'task_handoff: bad to role';
    if (op === 'task_blocked' || op === 'task_needs') return String(i.reason ?? '').trim() ? null : `${op}: reason required`;
  }
  if (op === 'message_send' || op === 'pod_send') {
    const targetKey = op === 'message_send' ? 'to' : 'role';
    if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(String(i[targetKey] ?? ''))) return `${op}: bad ${targetKey}`;
    if (!String(i.text ?? '').trim()) return `${op}: text required`;
  }
  if (op === 'pod_relaunch' || op === 'pod_close' || op === 'pod_spawn') {
    if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(String(i.role ?? ''))) return `${op}: bad role`;
  }
  if (op === 'workflow_start') {
    if (!String(i.name ?? '').trim()) return 'workflow_start: name required';
  }
  return null;
}

/** Apply a batch of intents sequentially through the single mutation path. */
export async function applyIntents(ctx: CoreCtx, intents: Record<string, unknown>[]): Promise<IntentResult[]> {
  const out: IntentResult[] = [];
  for (const raw of intents) {
    const intent = { ...raw };
    const op = String(intent.op ?? '');
    delete intent.op;
    intent.type = op;
    const v = validateIntent(raw);
    if (v) {
      out.push({ op: op || '(empty)', ok: false, detail: v });
      continue;
    }
    try {
      await apply(intent, ctx);
      out.push({ op, ok: true, detail: 'applied' });
    } catch (e) {
      const detail = e instanceof OpError ? `${e.status}: ${e.message}` : e instanceof Error ? e.message : String(e);
      out.push({ op, ok: false, detail });
    }
  }
  return out;
}
