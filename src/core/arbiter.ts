import * as store from './store.js';
import * as terminal from './terminal.js';
import type { CoreCtx } from './ops.js';

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

export const ARBITER_INTERVAL_MS = 10_000;

export async function runArbiterTick(ctx: CoreCtx): Promise<void> {
  const pods = store.listPods(ctx.store);
  const byRole = new Map(pods.map((p) => [p.role, p]));

  // 1) claim: free live pod + oldest queued task for that role
  for (const pod of pods) {
    if (pod.state !== 'live' || !pod.terminal_target) continue;
    if (store.activeTaskForPod(ctx.store, pod.role)) continue; // busy
    const task = store.oldestQueuedTask(ctx.store, pod.role);
    if (!task) continue;
    claimTask(ctx, pod.role, task.id);
  }

  // 2) verify: active task whose pod is gone -> blocked
  for (const task of store.listTasks(ctx.store, 'active')) {
    const pod = byRole.get(task.pod_role);
    if (!pod || pod.state !== 'live' || !pod.terminal_target) {
      try {
        store.setTaskStatus(ctx.store, task.id, 'blocked', { reason: 'pod lost (not live)', result: 'pod lost (not live)' });
        ctx.emit?.({ type: 'task_blocked', taskId: task.id, pod: task.pod_role, reason: 'pod lost' });
      } catch {
        // already transitioned by a concurrent op; harmless
      }
    }
  }
}

function claimPrompt(task: store.Task): string {
  const body = [task.title, task.body ?? ''].join('\n');
  return [
    `[flock-task ${task.id}]`,
    body,
    '',
    'Протокол: когда закончишь — выполни в bash одну из команд:',
    `  flock task done ${task.id}`,
    `  flock task blocked ${task.id} '<краткая причина>'`,
    `  flock task needs ${task.id} '<что нужно от человека>'`,
  ].join('\n');
}

function claimTask(ctx: CoreCtx, role: string, taskId: string): void {
  store.setTaskStatus(ctx.store, taskId, 'active', { reason: 'claimed' });
  const task = store.getTask(ctx.store, taskId)!;
  const pod = store.getPodByRole(ctx.store, role)!;
  // Fire-and-forget the send; the verified send() may retry a few times.
  // If the pod is gone it throws — swallow, the verify pass will block it.
  void terminal
    .send(pod.terminal_target!, claimPrompt(task))
    .then((r) => {
      if (!r.delivered) {
        store.setTaskStatus(ctx.store, taskId, 'blocked', { reason: 'delivery not verified' });
        ctx.emit?.({ type: 'task_blocked', taskId, pod: role, reason: 'delivery not verified' });
      }
    })
    .catch(() => {
      // verify pass marks it blocked on next tick
    });
  ctx.emit?.({ type: 'task_claimed', taskId, pod: role });
}
