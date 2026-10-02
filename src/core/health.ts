// Built-in pod health checks (stage 4.1).
//
// Not agent-registered watchdogs — core's own liveness ladder over the typed
// signals we already have (sidecar + activity.jsonl):
//
//   gate: an extension dialog (select/confirm/input/editor) is waiting for a
//         human. pi has no timeout — the agent blocks forever until the
//         operator answers (`flock pod answer <role> <arg>`). Detection:
//         ext_dialog_unanswered in activity with no ext_dialog_answered for
//         the same id after it.
//
//   idle: the agent is at rest (streaming=false) but a task it claimed is
//         still active — classic "finished, forgot flock task done". Nudge
//         via pod send; after the escalation budget — task needs.
//
// Wake ladder: detect -> alert (gate) / nudge (idle) -> escalate (task needs).
// State in the health_alerts table survives core restarts.

import * as path from 'node:path';
import fs from 'node:fs';
import { seatPaths } from './runner-protocol.js';
import type { CoreCtx } from './ops.js';
import { apply } from './ops.js';
import * as store from './store.js';
import { readRunnerState } from './terminal.js';

export interface HealthOpts {
  gateDetectMin: number; // dialog pending longer than this -> alert
  gateRealertMin: number; // re-alert interval for an open gate
  idleMin: number; // at rest + lastPrompt older than this -> stall
  nudgeEveryMin: number; // idle: nudge interval
  escalateAfterNudges: number; // idle: nudges before task needs
  escalateAfterMin: number; // idle: minutes of idle before escalation
}

export const HEALTH_DEFAULTS: HealthOpts = {
  gateDetectMin: 2,
  gateRealertMin: 5,
  idleMin: 15,
  nudgeEveryMin: 10,
  escalateAfterNudges: 2,
  escalateAfterMin: 25,
};

export function healthOptsFromEnv(env: NodeJS.ProcessEnv = process.env): HealthOpts {
  const num = (v: string | undefined, d: number): number => {
    const n = v === undefined ? NaN : Number(v);
    return Number.isFinite(n) && n > 0 ? n : d;
  };
  return {
    gateDetectMin: num(env.FLOCK_HEALTH_GATE_MIN, HEALTH_DEFAULTS.gateDetectMin),
    gateRealertMin: num(env.FLOCK_HEALTH_GATE_REALERT_MIN, HEALTH_DEFAULTS.gateRealertMin),
    idleMin: num(env.FLOCK_HEALTH_IDLE_MIN, HEALTH_DEFAULTS.idleMin),
    nudgeEveryMin: num(env.FLOCK_HEALTH_NUDGE_EVERY_MIN, HEALTH_DEFAULTS.nudgeEveryMin),
    escalateAfterNudges: num(env.FLOCK_HEALTH_ESCALATE_NUDGES, HEALTH_DEFAULTS.escalateAfterNudges),
    escalateAfterMin: num(env.FLOCK_HEALTH_ESCALATE_MIN, HEALTH_DEFAULTS.escalateAfterMin),
  };
}

// ---- pure detection (testable) ----------------------------------------------

export interface ActivityLine {
  at?: string;
  event?: string;
  id?: string;
  via?: string;
  method?: string;
  title?: string;
  [k: string]: unknown;
}

/** Parse (tail of) the pod activity log: newest last, non-JSON lines skipped. */
export function parseActivity(raw: string, maxLines = 400): ActivityLine[] {
  const lines = raw.split('\n').filter(Boolean).slice(-maxLines);
  const out: ActivityLine[] = [];
  for (const l of lines) {
    try {
      out.push(JSON.parse(l) as ActivityLine);
    } catch {
      /* not json — skip */
    }
  }
  return out;
}

export function readActivity(stateRoot: string, role: string, maxLines = 400): ActivityLine[] {
  let raw: string;
  try {
    raw = fs.readFileSync(path.join(seatPaths(stateRoot, role).activityPath), 'utf8');
  } catch {
    return [];
  }
  return parseActivity(raw, maxLines);
}

/**
 * The currently-open dialog gate: any ext_dialog_unanswered without an
 * ext_dialog_answered for the same id after it; the most recent one wins.
 */
export function detectGate(activity: ActivityLine[]): ActivityLine | null {
  const openIdx = new Map<string, number>(); // dialog id -> index of its unanswered event
  activity.forEach((a, i) => {
    if (a.event === 'ext_dialog_unanswered' && typeof a.id === 'string') openIdx.set(a.id, i);
    else if (a.event === 'ext_dialog_answered' && typeof a.id === 'string') openIdx.delete(a.id);
  });
  let best: ActivityLine | null = null;
  let bestIdx = -1;
  for (const [id, idx] of openIdx) {
    if (idx > bestIdx) {
      bestIdx = idx;
      best = activity[idx];
    }
  }
  return best;
}

export interface IdleProbe {
  ready: boolean;
  streaming?: boolean;
  lastPromptAt?: string;
}

/** Agent at rest long enough to be a stall (task liveness is checked by the caller). */
export function detectIdle(sidecar: IdleProbe, nowMs: number, opts: HealthOpts): boolean {
  if (!sidecar.ready) return false;
  if (sidecar.streaming) return false; // still working
  if (!sidecar.lastPromptAt) return false;
  const ageMin = (nowMs - Date.parse(sidecar.lastPromptAt)) / 60000;
  return Number.isFinite(ageMin) && ageMin >= opts.idleMin;
}

// ---- alert state (health_alerts table) ---------------------------------------

export interface AlertRow {
  pod_role: string;
  kind: 'gate' | 'idle';
  ref: string;
  state: string;
  count: number;
  first_at: string;
  last_at: string;
  note: string | null;
}

function db(ctx: CoreCtx): store.Store['db'] {
  return ctx.store.db;
}

function alertUpsert(ctx: CoreCtx, a: { pod_role: string; kind: 'gate' | 'idle'; ref: string; state: string; note: string }): void {
  const now = store.nowIso();
  const prev = db(ctx)
    .prepare('SELECT * FROM health_alerts WHERE pod_role = ? AND kind = ? AND ref = ?')
    .get(a.pod_role, a.kind, a.ref) as AlertRow | undefined;
  if (prev) {
    db(ctx).prepare('UPDATE health_alerts SET state = ?, note = ?, last_at = ? WHERE pod_role = ? AND kind = ? AND ref = ?')
      .run(a.state, a.note, now, a.pod_role, a.kind, a.ref);
  } else {
    db(ctx)
      .prepare('INSERT INTO health_alerts (pod_role, kind, ref, state, count, first_at, last_at, note) VALUES (?, ?, ?, ?, 0, ?, ?, ?)')
      .run(a.pod_role, a.kind, a.ref, a.state, now, now, a.note);
  }
  ctx.emit?.({ type: 'health', pod: a.pod_role, kind: a.kind, ref: a.ref, state: a.state, note: a.note });
}

function alertBumpCount(ctx: CoreCtx, pod: string, kind: 'gate' | 'idle', ref: string): void {
  db(ctx).prepare('UPDATE health_alerts SET count = count + 1 WHERE pod_role = ? AND kind = ? AND ref = ?').run(pod, kind, ref);
}

function alertClear(ctx: CoreCtx, pod: string, kind: 'gate' | 'idle', ref: string): void {
  const r = db(ctx).prepare('DELETE FROM health_alerts WHERE pod_role = ? AND kind = ? AND ref = ?').run(pod, kind, ref);
  if (r.changes > 0) ctx.emit?.({ type: 'health_cleared', pod, kind, ref });
}

export function listAlerts(ctx: CoreCtx): AlertRow[] {
  return db(ctx).prepare('SELECT * FROM health_alerts ORDER BY last_at DESC').all() as unknown as AlertRow[];
}

// ---- tick ---------------------------------------------------------------------

/** One health tick. Safe to call from the timer; never throws. */
export async function runHealthTick(ctx: CoreCtx): Promise<void> {
  const opts = healthOptsFromEnv();
  const nowMs = Date.now();
  for (const pod of store.listPods(ctx.store)) {
    if (pod.state !== 'live' || pod.agent !== 'pi') continue; // only runner pods
    try {
      await checkPod(ctx, pod.role, opts, nowMs);
    } catch (e) {
      console.error(`[core] health: ${pod.role}: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
}

async function checkPod(ctx: CoreCtx, role: string, opts: HealthOpts, nowMs: number): Promise<void> {
  const state = readRunnerState(ctx.store.home, role);
  const activity = readActivity(ctx.store.home, role);

  // --- gate: a dialog is waiting for a human ---------------------------------
  const gate = detectGate(activity);
  if (gate && typeof gate.id === 'string') {
    const at = Date.parse(typeof gate.at === 'string' ? gate.at : '') || nowMs;
    const ageMin = (nowMs - at) / 60000;
    const prev = db(ctx)
      .prepare('SELECT * FROM health_alerts WHERE pod_role = ? AND kind = ? AND ref = ?')
      .get(role, 'gate', gate.id) as AlertRow | undefined;
    if (ageMin >= opts.gateDetectMin) {
      const title = typeof gate.title === 'string' ? gate.title.slice(0, 120) : '(без заголовка)';
      const shouldAlert = !prev || nowMs - Date.parse(prev.last_at) >= opts.gateRealertMin * 60000;
      if (shouldAlert) {
        alertUpsert(ctx, {
          pod_role: role,
          kind: 'gate',
          ref: gate.id,
          state: 'open',
          note: `dialog waiting: ${title} — ответ: flock pod answer ${role} <n|текст>`,
        });
      }
      // escalation: park an active task in needs so it is not lost in the queue
      if (prev && !prev.state.includes('needs')) {
        const task = store.activeTaskForPod(ctx.store, role);
        if (task && ageMin >= opts.gateDetectMin + opts.gateRealertMin) {
          await apply({ type: 'task_needs', id: task.id, reason: `agent waiting for operator at a dialog gate: ${title.slice(0, 80)}` }, ctx);
          alertUpsert(ctx, {
            pod_role: role,
            kind: 'gate',
            ref: gate.id,
            state: 'escalated(needs)',
            note: `→ needs: task ${task.id}; dialog: ${title.slice(0, 80)}`,
          });
        }
      }
    }
    return; // a dialog-blocked agent is not "idle"
  }
  for (const a of listAlerts(ctx).filter((a) => a.pod_role === role && a.kind === 'gate')) {
    alertClear(ctx, role, 'gate', a.ref);
  }

  // --- idle: at rest while a claimed task is still active ----------------------
  const activeTasks = store.listTasks(ctx.store, 'active').filter((t) => t.pod_role === role);
  if (!state || activeTasks.length === 0) {
    for (const a of listAlerts(ctx).filter((a) => a.pod_role === role && a.kind === 'idle')) {
      alertClear(ctx, role, 'idle', a.ref);
    }
    return;
  }
  for (const task of activeTasks) {
    const idle = detectIdle({ ready: !!state.ready, streaming: state.streaming, lastPromptAt: state.lastPrompt?.at }, nowMs, opts);
    if (!idle) {
      alertClear(ctx, role, 'idle', task.id);
      continue;
    }
    const prev = db(ctx)
      .prepare('SELECT * FROM health_alerts WHERE pod_role = ? AND kind = ? AND ref = ?')
      .get(role, 'idle', task.id) as AlertRow | undefined;
    if (!prev) {
      alertUpsert(ctx, {
        pod_role: role,
        kind: 'idle',
        ref: task.id,
        state: 'watching',
        note: `agent at rest with active task ${task.id} ("${task.title.slice(0, 60)}")`,
      });
      continue;
    }
    const sinceNudge = nowMs - Date.parse(prev.last_at);
    const sinceFirst = nowMs - Date.parse(prev.first_at);
    if (prev.count >= opts.escalateAfterNudges || sinceFirst >= opts.escalateAfterMin * 60000) {
      if (!prev.state.includes('needs')) {
        await apply({
          type: 'task_needs',
          id: task.id,
          reason: `agent idle ${Math.round(sinceFirst / 60000)}min with ${prev.count} nudges — parked for operator`,
        }, ctx);
        alertUpsert(ctx, {
          pod_role: role,
          kind: 'idle',
          ref: task.id,
          state: 'escalated(needs)',
          note: `→ needs: ${prev.count} nudges over ${Math.round(sinceFirst / 60000)}min`,
        });
      }
    } else if (sinceNudge >= opts.nudgeEveryMin * 60000) {
      await apply({
        type: 'pod_send',
        role,
        text: `Health-check: задача ${task.id} ("${task.title.slice(0, 60)}") всё ещё active, а ты на паузе. Если готов — \`flock task done ${task.id} <результат>\`; если ждёшь что-то конкретное — напиши что.`,
      }, ctx);
      alertBumpCount(ctx, role, 'idle', task.id);
      alertUpsert(ctx, {
        pod_role: role,
        kind: 'idle',
        ref: task.id,
        state: 'nudging',
        note: `nudge ${prev.count + 1} sent`,
      });
    }
  }
}
