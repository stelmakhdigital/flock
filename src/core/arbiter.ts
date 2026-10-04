import path from 'node:path';
import * as store from './store.js';
import * as terminal from './terminal.js';
import { frameMessage, newNonce } from "./runner-protocol.js";
import { advanceWorkflow, checkWorkflowTimeouts, podRelaunch, type CoreCtx } from './ops.js';
import { pmNotify } from './pm.js';
import { podRuntime } from './agent.js';

// arbiter — the pipeline engine (stage 1). One tick does three things:
//   claim   a free pod takes the oldest queued task for its role
//   verify  an active task whose pod died is marked blocked (pod lost)
//   handoff an active task whose pod is gone is released; when a task is
//           completed (task_done op) the pod frees up and the next claim
//           picks the next queued task for that pod.
//
// Completion in stage 1 is EXPLICIT (task_done op, from CLI/LLM). Capture-based
// auto-detection (FLOCK:DONE marker) is stage 2 — keep this tick simple.
// ponytail: per-pod serialization only; no cross-pod dependencies/priority.
// 5.0.4: wake closes the zombie queue — a closed pod with queued work is
// revived by the arbiter, so queued tasks can no longer rot forever.

export const ARBITER_INTERVAL_MS = 10_000;

// per-pod wake cooldown: a failing relaunch (bad repo, missing runtime)
// must not flap every tick. Env override FLOCK_WAKE_COOLDOWN_MS.
const WAKE_COOLDOWN_MS = Number(process.env.FLOCK_WAKE_COOLDOWN_MS ?? 5 * 60_000);
const wakeCooldown = new Map<string, number>();

export async function runArbiterTick(ctx: CoreCtx): Promise<void> {
  const pods = store.listPods(ctx.store);
  const byRole = new Map(pods.map((p) => [p.role, p]));

  // 1) wake: closed pod with queued work -> relaunch (honest resume), then
  // the claim step of THIS tick picks the task up.
  for (const pod of pods) {
    if (pod.state !== 'closed' || pod.agent === 'cmd') continue; // cmd has no resume
    const queued = store.oldestQueuedTask(ctx.store, pod.role);
    if (!queued) continue;
    const last = wakeCooldown.get(pod.role) ?? 0;
    if (Date.now() - last < WAKE_COOLDOWN_MS) continue;
    wakeCooldown.set(pod.role, Date.now());
    try {
      await podRelaunch({ role: pod.role }, ctx);
      console.log(`[core] arbiter: woke pod ${pod.role} for queued task ${queued.id}`);
      ctx.emit?.({ type: 'pod_woken', role: pod.role, taskId: queued.id });
      void pmNotify(ctx, { type: 'pod_woken', detail: `под ${pod.role} разбужен: queued-задача ${queued.id} "${queued.title.slice(0, 60)}"` }).catch(() => {});
    } catch (e) {
      console.warn(`[core] arbiter: wake of ${pod.role} failed: ${e instanceof Error ? e.message : String(e)} (cooldown ${Math.round(WAKE_COOLDOWN_MS / 1000)}s)`);
    }
  }

  // 2) claim: free live pod + oldest queued task for that role
  for (const pod of pods) {
    if (pod.state !== 'live' || !pod.terminal_target) continue;
    if (store.activeTaskForPod(ctx.store, pod.role)) continue; // busy
    const task = store.oldestQueuedTask(ctx.store, pod.role);
    if (!task) continue;
    claimTask(ctx, pod.role, task.id);
  }

  // 3) verify: active task whose pod is gone -> blocked
  for (const task of store.listTasks(ctx.store, 'active')) {
    const pod = byRole.get(task.pod_role);
    if (!pod || pod.state !== 'live' || !pod.terminal_target) {
      try {
        store.setTaskStatus(ctx.store, task.id, 'blocked', { reason: 'pod lost (not live)', result: 'pod lost (not live)' });
        ctx.emit?.({ type: 'task_blocked', taskId: task.id, pod: task.pod_role, reason: 'pod lost' });
        advanceWorkflow(ctx, task.id);
      } catch {
        // already transitioned by a concurrent op; harmless
      }
    }
  }

  // 4) 5.4a: workflow step timeout/TTL (active step task outlived its
  // step.timeoutMin -> blocked; the retry budget then re-queues or stops)
  checkWorkflowTimeouts(ctx);
}

function claimPrompt(task: store.Task): string {
  const body = [task.title, task.body ?? ''].join('\n');
  return [
    `[flock-task ${task.id}]`,
    body,
    '',
    'Протокол: когда закончишь — выполни в bash одну из команд:',
    `  flock task done ${task.id} '<reason: finished|blocked|denied|canceled|escalated>'`,
    `  flock task blocked ${task.id} '<краткая причина>'`,
    `  flock task needs ${task.id} '<что нужно от человека>'`,
  ].join('\n');
}

function claimTask(ctx: CoreCtx, role: string, taskId: string): void {
  store.setTaskStatus(ctx.store, taskId, 'active', { reason: 'claimed' });
  const task = store.getTask(ctx.store, taskId)!;
  const pod = store.getPodByRole(ctx.store, role)!;
  const isRunner = podRuntime(pod.agent) === 'pi'; // flock-rpc bridge: typed delivery ack
  const text = claimPrompt(task);
  const nonce = isRunner ? newNonce() : undefined;
  const wire = isRunner ? frameMessage(text, nonce) : text;
  // Fire-and-forget the send; the verified send() may retry a few times.
  // If the pod is gone it throws — swallow, the verify pass will block it.
  void terminal
    .send(pod.terminal_target!, wire, isRunner ? { raw: true } : undefined)
    .then(async (r) => {
      if (!r.delivered) {
        store.setTaskStatus(ctx.store, taskId, 'blocked', { reason: 'delivery not verified' });
        ctx.emit?.({ type: 'task_blocked', taskId, pod: role, reason: 'delivery not verified' });
        return;
      }
      if (isRunner) {
        // semantic ack: the runner recorded the prompt in its sidecar
        const deadline = Date.now() + 8000;
        let acked = false;
        while (Date.now() < deadline) {
          const st = terminal.readRunnerState(ctx.store.home, role);
          // nonce match (v2): an identical repeated prompt can't false-positive
          if (st?.lastPrompt?.nonce === nonce) {
            acked = true;
            break;
          }
          await new Promise((res) => setTimeout(res, 300));
        }
        if (!acked) {
          store.setTaskStatus(ctx.store, taskId, 'blocked', { reason: 'runner did not ack prompt (sidecar)' });
          ctx.emit?.({ type: 'task_blocked', taskId, pod: role, reason: 'no runner ack' });
        }
      }
    })
    .catch(() => {
      // verify pass marks it blocked on next tick
    });
  ctx.emit?.({ type: 'task_claimed', taskId, pod: role });
}
