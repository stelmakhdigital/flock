// Goal loop (stage 4.2): an LLM-lead "pm" pod over the task pipeline.
//
// The pm is a regular pi pod (built-in agent manifest with PM-protocol
// guidance) that wakes on typed triggers, reads the pipeline state
// (`flock pm state`) and issues typed intents (`flock pm intent <json>`)
// through the SINGLE mutation path (apply). Intent vocabulary is a
// whitelist — core ops, watchdog, and pm-recursion are not issuable.
//
// Triggers: task add / done / blocked / needs / cancel, pod crash, and the
// 5-minute PM tick (sweep only when the digest changed — no LLM cost on
// a quiet pipeline).

import * as store from './store.js';
import { apply, OpError, type CoreCtx } from './ops.js';
import { readRunnerState } from './terminal.js';
import { podRuntime } from './agent.js';
import { PM_PROTOCOL } from './pm-protocol.js';
export { PM_PROTOCOL };

// ---- intent whitelist --------------------------------------------------------

const INTENT_OPS = new Set([
  'task_add',
  'task_done',
  'task_blocked',
  'task_needs',
  'task_cancel',
  'task_unblock',
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
  if (op === 'task_done' || op === 'task_blocked' || op === 'task_needs' || op === 'task_cancel' || op === 'task_unblock') {
    if (!String(i.id ?? '').trim()) return `${op}: id required`;
    if (op === 'task_blocked' || op === 'task_needs') return String(i.reason ?? '').trim() ? null : `${op}: reason required`;
  }
  if (op === 'pod_send') {
    if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(String(i.role ?? ''))) return 'pod_send: bad role';
    if (!String(i.text ?? '').trim()) return 'pod_send: text required';
  }
  if (op === 'pod_relaunch' || op === 'pod_close') {
    if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(String(i.role ?? ''))) return `${op}: bad role`;
  }
  if (op === 'pod_spawn') {
    if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(String(i.role ?? ''))) return 'pod_spawn: bad role';
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

// ---- pm state (what the pm sees) ----------------------------------------------

export function pmDigest(ctx: CoreCtx): Record<string, unknown> {
  const s = ctx.store;
  const tasks = store.listTasks(s);
  const byStatus: Record<string, number> = {};
  for (const t of tasks) byStatus[t.status] = (byStatus[t.status] ?? 0) + 1;
  const open = tasks.filter((t) => t.status === 'active' || t.status === 'needs' || t.status === 'blocked');
  const pods = store.listPods(s).map((p) => ({
    role: p.role,
    state: p.state,
    agent: p.agent,
    ready: podRuntime(p.agent) === 'pi' ? (readPodReady(ctx, p.role) ?? null) : null,
  }));
  const runs = store.listRuns(s).filter((r) => !r.ended_at).map((r) => ({ pod: r.pod_role, run: r.id, started_at: r.started_at }));
  return {
    at: store.nowIso(),
    tasks: byStatus,
    openTasks: open.map((t) => ({ id: t.id, title: t.title.slice(0, 80), status: t.status, pod: t.pod_role, claimed_at: t.claimed_at ?? null })),
    pods,
    liveRuns: runs,
  };
}

function readPodReady(ctx: CoreCtx, role: string): boolean | null {
  try {
    const st = readRunnerState(ctx.store.home, role);
    return st ? st.ready : null;
  } catch {
    return null;
  }
}

// ---- triggers ------------------------------------------------------------------

export interface PmTrigger {
  type: string; // task_added | task_done | task_blocked | task_needs | task_cancelled | pod_crashed | pm_tick
  detail: string;
}

let lastDigest: string | null = null;

/** Wake the pm pod on a trigger (no-op when the pm is not live). */
export async function pmNotify(ctx: CoreCtx, t: PmTrigger): Promise<boolean> {
  const pod = store.getPodByRole(ctx.store, 'pm');
  if (!pod || pod.state !== 'live') return false;
  await apply(
    { type: 'pod_send', role: 'pm', text: `[flock-pm] TRIGGER ${t.type}: ${t.detail}\nСмотри \`flock pm state\`, затем решение: \`flock pm intent '<json>'\` (batch: {"intents":[...]}) — или одно предложение, если решение не нужно.` },
    ctx,
  );
  return true;
}

const PM_TICK_INTERVAL_MS = 5 * 60_000;
let lastTickAt = 0;

/** 5-minute sweep: wake the pm only when the digest changed (or 30min quiet). */
export async function pmTick(ctx: CoreCtx): Promise<void> {
  const now = Date.now();
  if (now - lastTickAt < PM_TICK_INTERVAL_MS) return;
  lastTickAt = now;
  const digest = pmDigest(ctx);
  const sig = JSON.stringify({ tasks: digest.tasks, open: (digest.openTasks as unknown[]).map((t) => JSON.stringify(t)), pods: (digest.pods as unknown[]).map((p) => JSON.stringify(p)) });
  const firstSweep = lastDigest === null;
  if (!firstSweep && sig === lastDigest) return; // nothing changed — no LLM cost
  lastDigest = sig;
  const pod = store.getPodByRole(ctx.store, 'pm');
  if (!pod || pod.state !== 'live') return;
  const lines = (digest.openTasks as { id: string; title: string; status: string; pod: string | null }[])
    .map((t) => `  ${t.id} [${t.status}] ${t.title} (pod: ${t.pod ?? '-'})`)
    .join('\n');
  const podLines = (digest.pods as { role: string; state: string }[])
    .filter((p) => p.state === 'live')
    .map((p) => `${p.role}:${p.state}`)
    .join(', ') || 'нет';
  const counts = Object.entries(digest.tasks as Record<string, number>).map(([k, v]) => `${k}=${v}`).join(' ');
  await apply({
    type: 'pod_send',
    role: 'pm',
    text: `[flock-pm] SWEEP (периодический осмотр, ${firstSweep ? 'первый' : 'изменения в pipeline'}):\nзадачи: ${counts}\nоткрытые:\n${lines || '  (нет)'}\nживые поды: ${podLines}\nОцени: всё движется? Если решение нужно — flock pm intent, если нет — просто подтверди одним предложением.`,
  }, ctx);
}
