import crypto from 'node:crypto';
import * as store from './store.js';
import * as terminal from './terminal.js';
import type { CoreCtx } from './ops.js';
import type { WatchdogJob } from './store.js';

// watchdog: declarative checks registered by agents/CLI; core evaluates them
// on schedule and wakes the target pod. One runner, one tick, policy registry.
// (OpenRig-style architecture, see docs/04-watchdog.md.)

export const WATCHDOG_POLICIES = ['marker', 'timer', 'stall'] as const;
export type PolicyName = (typeof WATCHDOG_POLICIES)[number];

interface PolicyResult {
  action: 'skip' | 'send' | 'terminal';
  reason?: string;
  message?: string;
  newState?: string; // policy memory -> job.last_state (JSON)
  terminalAfterSend?: boolean;
}

interface Policy {
  evaluate(job: WatchdogJob, ctx: CoreCtx): Promise<PolicyResult>;
}

function specOf<T>(job: WatchdogJob): T {
  return JSON.parse(job.spec) as T;
}

function podTarget(ctx: CoreCtx, role: string): string | null {
  const pod = store.getPodByRole(ctx.store, role);
  return pod && pod.state === 'live' ? pod.terminal_target : null;
}

const policies: Record<PolicyName, Policy> = {
  marker: {
    async evaluate(job, ctx) {
      const s = specOf<{ text: string; lines?: number; once?: boolean }>(job);
      const target = podTarget(ctx, job.target_pod);
      if (!target) return { action: 'skip', reason: 'target pod not live' };
      const text = await terminal.capture(target, s.lines ?? 100);
      if (!text.includes(s.text)) return { action: 'skip', reason: 'marker not seen' };
      const tail = text.split('\n').slice(-10).join('\n');
      return {
        action: 'send',
        message: `marker "${s.text}" seen in ${job.target_pod}\n--- tail ---\n${tail}`,
        terminalAfterSend: s.once !== false,
        newState: 'fired',
      };
    },
  },
  timer: {
    async evaluate(job) {
      const s = specOf<{ afterSeconds: number }>(job);
      const due = Date.parse(job.registered_at) + s.afterSeconds * 1000;
      if (Date.now() < due) {
        return { action: 'skip', reason: `timer not due (${Math.ceil((due - Date.now()) / 1000)}s left)` };
      }
      return {
        action: 'send',
        message: `timer fired: ${s.afterSeconds}s elapsed since registration`,
        terminalAfterSend: true,
      };
    },
  },
  stall: {
    async evaluate(job, ctx) {
      const s = specOf<{ idleSeconds: number; lines?: number }>(job);
      const target = podTarget(ctx, job.target_pod);
      if (!target) return { action: 'skip', reason: 'target pod not live' };
      const text = await terminal.capture(target, s.lines ?? 40);
      const hash = crypto.createHash('sha256').update(text).digest('hex').slice(0, 12);
      let prev: { hash: string; since: number } | null = null;
      try {
        prev = job.last_state ? (JSON.parse(job.last_state) as { hash: string; since: number }) : null;
      } catch {
        prev = null;
      }
      if (!prev) return { action: 'skip', reason: 'baseline captured', newState: JSON.stringify({ hash, since: Date.now() }) };
      if (prev.hash !== hash) {
        return { action: 'skip', reason: 'activity detected', newState: JSON.stringify({ hash, since: Date.now() }) };
      }
      const idleMs = Date.now() - prev.since;
      if (idleMs < s.idleSeconds * 1000) {
        return { action: 'skip', reason: `idle ${Math.floor(idleMs / 1000)}s < ${s.idleSeconds}s` };
      }
      return {
        action: 'send',
        message: `pod ${job.target_pod} looks stalled: no screen change for ${Math.floor(idleMs / 1000)}s`,
      };
    },
  },
};

export function validateSpec(policy: PolicyName, spec: Record<string, unknown>): string | null {
  if (policy === 'marker') {
    if (typeof spec.text !== 'string' || !spec.text.trim()) return 'marker: spec.text required';
  } else if (policy === 'timer') {
    if (typeof spec.afterSeconds !== 'number' || spec.afterSeconds <= 0) return 'timer: spec.afterSeconds > 0 required';
  } else if (policy === 'stall') {
    if (typeof spec.idleSeconds !== 'number' || spec.idleSeconds <= 0) return 'stall: spec.idleSeconds > 0 required';
  }
  return null;
}

export function formatDeliveryMessage(job: WatchdogJob, message: string): string {
  return `[flock-watchdog ${job.id} policy=${job.policy} from=${job.registered_by}] ${message}`;
}

export function terminateJob(ctx: CoreCtx, job: WatchdogJob, reason: string, nowIso: string): void {
  store.watchdogUpdate(ctx.store, job.id, { state: 'terminal', terminal_reason: reason, actionable: 0 });
  store.addWatchdogHistory(ctx.store, { jobId: job.id, outcome: 'terminal', skipReason: reason });
  ctx.emit?.({ type: 'watchdog_terminal', jobId: job.id, policy: job.policy, reason });
}

async function evaluateJob(ctx: CoreCtx, job: WatchdogJob, nowIso: string): Promise<void> {
  if (job.last_evaluation_at && Date.parse(nowIso) - Date.parse(job.last_evaluation_at) < job.interval_seconds * 1000) {
    return; // per-job interval not elapsed
  }
  // hard timeout
  let spec: Record<string, unknown> = {};
  try {
    spec = JSON.parse(job.spec);
  } catch {
    spec = {};
  }
  if (typeof spec.timeoutSeconds === 'number' && Date.parse(nowIso) - Date.parse(job.registered_at) > spec.timeoutSeconds * 1000) {
    return terminateJob(ctx, job, 'timeout', nowIso);
  }
  const policy = policies[job.policy as PolicyName];
  if (!policy) return terminateJob(ctx, job, `unknown policy: ${job.policy}`, nowIso);

  const res = await policy.evaluate(job, ctx);
  const updates: Record<string, unknown> = { last_evaluation_at: nowIso };
  if (res.newState !== undefined) updates.last_state = res.newState;

  if (res.action === 'skip') {
    updates.last_skip_reason = res.reason ?? 'skip';
    if (job.actionable) updates.actionable = 0;
    store.watchdogUpdate(ctx.store, job.id, updates);
    return;
  }
  if (res.action === 'terminal') {
    store.watchdogUpdate(ctx.store, job.id, updates);
    return terminateJob(ctx, job, res.reason ?? 'terminal', nowIso);
  }

  // send: quiet period (do not wake more often than active_wake_interval)
  updates.actionable = 1;
  updates.last_actionable_at = nowIso;
  if (job.last_fire_at && job.active_wake_interval_seconds != null) {
    const sinceFire = Date.parse(nowIso) - Date.parse(job.last_fire_at);
    if (sinceFire < job.active_wake_interval_seconds * 1000) {
      updates.last_skip_reason = `quiet: ${Math.ceil((job.active_wake_interval_seconds * 1000 - sinceFire) / 1000)}s to next wake`;
      store.watchdogUpdate(ctx.store, job.id, updates);
      return;
    }
  }
  const target = podTarget(ctx, job.target_pod);
  if (!target) {
    updates.last_skip_reason = 'target pod not live';
    store.watchdogUpdate(ctx.store, job.id, updates);
    return;
  }
  const message = formatDeliveryMessage(job, res.message ?? '');
  await terminal.paste(target, message);
  await terminal.sendEnter(target);
  updates.last_fire_at = nowIso;
  updates.last_skip_reason = null;
  store.watchdogUpdate(ctx.store, job.id, updates);
  store.addWatchdogHistory(ctx.store, { jobId: job.id, outcome: 'send', deliveryStatus: 'delivered', deliveryMessage: message });
  ctx.emit?.({ type: 'watchdog_fired', jobId: job.id, policy: job.policy, targetPod: job.target_pod });
  if (res.terminalAfterSend) terminateJob(ctx, job, 'fired', nowIso);
}

// One tick: evaluate every active job that is due. Errors never kill the tick.
export async function runWatchdogTick(ctx: CoreCtx): Promise<void> {
  const nowIso = new Date().toISOString();
  const jobs = store.listWatchdogJobs(ctx.store, 'active');
  for (const job of jobs) {
    try {
      await evaluateJob(ctx, job, nowIso);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      store.watchdogUpdate(ctx.store, job.id, { last_skip_reason: `error: ${msg}` });
    }
  }
}
