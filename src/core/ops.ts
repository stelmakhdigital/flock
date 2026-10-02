import fs from 'node:fs';
import path from 'node:path';
import * as store from './store.js';
import * as terminal from './terminal.js';
import { resolveAgent, loadAgents, firstUserModel, manifestRuntime } from './agent.js';
import { startPodSocket, stopPodSocket } from './http.js';
import { seatPaths, frameMessage } from './runner-protocol.js';
import { getAdapter, mergeManagedBlock, type PodBinding, type StartupFile } from './runtime-adapter.js';
import { WATCHDOG_POLICIES, validateSpec, terminateJob, type PolicyName } from './watchdog.js';
import type { Ticks } from './ticks.js';

// apply(op) — the SINGLE mutation path.
// Ticks, CLI, and (stage 4) LLM intents all mutate the world through ops.
// Delivery ops (pod_send) are transport actions, not state mutations:
// they are audited in runs.meta, while state transitions are logged in
// *_transitions (from stage 1, with tasks).

export interface CoreCtx {
  store: store.Store;
  ticks: Ticks;
  startedAt: string;
  emit?: (e: Record<string, unknown>) => void;
}

export class OpError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

const ROLE_RE = /^[a-z0-9][a-z0-9-]{0,30}$/;

function requireRole(op: Record<string, unknown>): string {
  const role = String(op.role ?? '');
  if (!ROLE_RE.test(role)) throw new OpError(400, `bad role (want [a-z0-9-]): ${role || '(empty)'}`);
  return role;
}

function requireLivePod(ctx: CoreCtx, role: string): store.Pod {
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod || pod.state !== 'live' || !pod.terminal_target) {
    throw new OpError(404, `no live pod: ${role}`);
  }
  return pod;
}

export async function apply(op: Record<string, unknown> | null, ctx: CoreCtx): Promise<unknown> {
  const o = op ?? {};
  const t = String(o.type ?? '');
  switch (t) {
    case 'pod_spawn':
      return podSpawn(o, ctx);
    case 'pod_relaunch':
      return podRelaunch(o, ctx);
    case 'pod_send':
      return podSend(o, ctx);
    case 'pod_capture':
      return podCapture(o, ctx);
    case 'pod_close':
      return podClose(o, ctx);
    case 'watchdog_register':
      return watchdogRegister(o, ctx);
    case 'watchdog_cancel':
      return watchdogCancel(o, ctx);
    case 'watchdog_list':
      return { jobs: store.listWatchdogJobs(ctx.store) };
    case 'workflow_define':
      return workflowDefine(o, ctx);
    case 'workflow_start':
      return workflowStart(o, ctx);
    case 'workflow_ls':
      return { workflows: store.listWorkflows(ctx.store), instances: store.listWorkflowInstances(ctx.store) };
    case 'workflow_status':
      return workflowStatus(o, ctx);
    case 'task_add':
      return taskAdd(o, ctx);
    case 'task_list':
      return { tasks: store.listTasks(ctx.store, o.status ? String(o.status) : undefined) };
    case 'task_history':
      return taskHistory(o, ctx);
    case 'task_cancel':
      return taskReport(o, ctx, 'cancelled');
    case 'task_done':
      return taskReport(o, ctx, 'done');
    case 'task_blocked':
      return taskReport(o, ctx, 'blocked');
    case 'task_needs':
      return taskReport(o, ctx, 'needs');
    case 'terminal_check':
      return terminalCheck(ctx);
    default:
      throw new OpError(400, `unknown op: ${t || '(empty)'}`);
  }
}

// Shared spawn path for pod_spawn and pod_relaunch.
// v3.1: the RuntimeAdapter owns projection, startup delivery, harness launch
// and the typed ready gate; core owns state. pi: honest resume via exact
// session file (relaunch) / fork via native_id, retry_fresh on a missing
// file (recorded, never silent). bash: plain window.
async function spawnAgent(ctx: CoreCtx, o: {
  role: string;
  dir: string;
  model?: string;
  agentId?: string;
  rawCmd?: string;
  resumeToken?: string;
  forkRef?: string;
  posture?: 'floor' | 'full_bypass';
  freshStart?: boolean;
}): Promise<{ pod: store.Pod; run: store.Run }> {
  fs.mkdirSync(o.dir, { recursive: true });
  let target: string;
  let pid: number | null = null;
  let agentStored: string;
  const runMeta: Record<string, unknown> = {};
  let model = o.model ?? null;

  if (o.rawCmd) {
    // Raw window (--cmd): no adapter, legacy plain path.
    agentStored = 'cmd';
    writePodAgentsMd(o.dir, o.role);
    const r = await terminal.spawnPod({ role: o.role, dir: o.dir, cmd: o.rawCmd });
    target = r.target;
    pid = r.pid;
  } else {
    writePodAgentsMd(o.dir, o.role);
    const agentId = o.agentId ?? 'pi';
    const r = resolveAgent(agentId, null);
    if (!r) throw new OpError(400, `unknown agent: ${agentId} (want ${Object.keys(loadAgents()).join(' | ')})`);
    const manifest = r.manifest;
    agentStored = r.id;
    const adapter = getAdapter(manifest, { home: ctx.store.home, token: ctx.store.token, runnerPath: path.join(import.meta.dirname, 'runner.js') });
    if (!adapter) throw new OpError(400, `agent ${agentId}: no runtime adapter (manifest needs a supported "runtime")`);

    if (adapter.runtime === 'pi' && !model) model = firstUserModel();
    const binding: PodBinding = {
      role: o.role,
      cwd: o.dir,
      model: model ?? undefined,
      launchPosture: o.posture ?? manifest.launchPosture,
      permissionMode: manifest.permissionMode,
    };
    // listInstalled: a clear spawn error instead of a dead window.
    const installed = await adapter.listInstalled(binding);
    if (!installed.installed) throw new OpError(400, `runtime not available: ${installed.detail ?? adapter.runtime}`);

    // Startup files (manifest-driven): guidance blocks merge BEFORE launch
    // (pi reads the context file at process start), first prompt after ready.
    const startup: StartupFile[] = (manifest.guidance ?? []).map((g) => ({
      path: g.id,
      content: g.content,
      deliveryHint: 'guidance_merge' as const,
    }));
    if (o.freshStart !== false && manifest.firstPrompt) {
      startup.push({ path: 'first-prompt', content: manifest.firstPrompt, deliveryHint: 'send_text', appliesOn: ['fresh_start'] });
    }
    adapter.project(binding);
    await adapter.deliverStartup(startup, binding, 'pre_launch');

    let launchId = store.newId('la');
    let launch = await adapter.launchHarness(binding, {
      launchId,
      resumeToken: o.resumeToken,
      forkSource: o.forkRef ? { kind: 'native_id', value: o.forkRef } : undefined,
    });
    if (!launch.ok && launch.recovery === 'retry_fresh') {
      // HONEST fresh (v1): missing session file -> logged + recorded in the
      // run meta, then retried fresh. Never a silent fresh start.
      console.warn(`[flock] ${o.role}: ${launch.error} — retrying fresh`);
      runMeta.resume = 'fresh_after_missing_session';
      launchId = store.newId('la');
      launch = await adapter.launchHarness(binding, { launchId });
    }
    if (!launch.ok) {
      throw new OpError(500, `launch failed: ${launch.error}${launch.evidence ? `\n${launch.evidence}` : ''}`);
    }
    target = launch.target;
    pid = launch.pid ?? null;
    if (runMeta.resume === undefined) runMeta.resume = launch.mode;
    runMeta.launchId = launchId;
    runMeta.runtime = adapter.runtime;
    if (launch.sessionFile) runMeta.sessionFile = launch.sessionFile;
    if (launch.sessionId) runMeta.sessionId = launch.sessionId;
    if (launch.resumeToken) runMeta.resumeToken = launch.resumeToken;
    if (launchPostureUsed(binding)) runMeta.launchPosture = binding.launchPosture;
    const post = await adapter.deliverStartup(startup, binding, 'post_ready');
    for (const f of post.failed) console.warn(`[flock] ${o.role}: startup failed for ${f.path}: ${f.error}`);
  }

  store.openPod(ctx.store, {
    id: store.newId('pod'),
    role: o.role,
    dir: o.dir,
    terminalTarget: target,
    model,
    agent: agentStored,
  });
  const run = store.insertRun(ctx.store, { id: store.newId('run'), podRole: o.role, pid, meta: runMeta });
  startPodSocket(ctx, o.dir); // pod-local API socket (sandbox-visible)
  ctx.emit?.({ type: 'pod_spawned', role: o.role, target, run: run.id });
  return { pod: store.getPodByRole(ctx.store, o.role)!, run };
}

const launchPostureUsed = (b: PodBinding): boolean => b.launchPosture !== undefined && b.launchPosture !== 'floor';

async function podSpawn(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const existing = store.getPodByRole(ctx.store, role);
  if (existing && existing.state !== 'closed') {
    throw new OpError(409, `pod ${role} already ${existing.state}`);
  }
  const dir = String(op.dir ?? path.join(ctx.store.home, 'pods', role));
  const { pod, run } = await spawnAgent(ctx, {
    role,
    dir,
    model: op.model ? String(op.model) : undefined,
    agentId: op.agent ? String(op.agent) : undefined,
    rawCmd: op.cmd ? String(op.cmd) : undefined,
    forkRef: op.fork ? resolveForkRef(ctx, String(op.fork)) : undefined,
    posture: op.posture === 'full_bypass' || op.posture === 'floor' ? op.posture : undefined,
  });
  return { pod, run };
}

// relaunch: agent dies (or operator wants a fresh one) -> new run on the same
// pod. pi (v3.1): HONEST resume — the exact persisted session file is
// relaunched (--session <file>), never an interactive picker; a missing file
// is retry_fresh (recorded in the run meta, never silent). bash: fresh window.
async function podRelaunch(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod) throw new OpError(404, `no pod: ${role}`);
  if (pod.agent === 'cmd') throw new OpError(400, `relaunch not supported for raw-cmd pods (cmd is not stored)`);
  const run = store.currentRun(ctx.store, role);
  if (run && !run.ended_at) store.endRun(ctx.store, run.id, 'replaced');
  await terminal.killWindow(role).catch(() => {});
  const resolved = resolveAgent(pod.agent ?? undefined, null);
  const isPi = !!resolved && manifestRuntime(resolved.manifest) === 'pi';
  const resumeToken = isPi ? (store.latestSessionFile(ctx.store, role) ?? undefined) : undefined;
  const res = await spawnAgent(ctx, {
    role,
    dir: pod.dir,
    model: op.model ? String(op.model) : (pod.model ?? undefined),
    agentId: pod.agent ?? undefined,
    resumeToken,
    freshStart: !resumeToken,
  });
  ctx.emit?.({ type: 'pod_relaunched', role, run: res.run.id });
  let mode: string | null = null;
  try {
    const arr = JSON.parse(res.run.meta ?? '[]');
    const e = Array.isArray(arr) ? arr[arr.length - 1] : arr;
    mode = e?.resume ?? null;
  } catch {
    /* meta absent */
  }
  return { pod: res.pod, run: res.run, resumed: mode === 'resume', resume: mode };
}

// --fork <role|path>: role -> that pod's current session file (session-id is
// in the filename); path is used as-is.
function resolveForkRef(ctx: CoreCtx, ref: string): string {
  if (ref.includes('/') || ref.includes('\\')) return ref;
  const pod = store.getPodByRole(ctx.store, ref);
  if (!pod) throw new OpError(404, `no pod to fork from: ${ref}`);
  const dir = path.join(pod.dir, '.pi', 'sessions');
  let files: string[] = [];
  try {
    files = fs.readdirSync(dir).filter((f) => f.endsWith(`_${ref}.jsonl`)).sort();
  } catch {
    /* no sessions dir */
  }
  if (!files.length) throw new OpError(404, `no session file for pod ${ref}`);
  return path.join(dir, files[files.length - 1]);
}

async function podSend(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const text = String(op.text ?? '');
  if (!text.trim()) throw new OpError(400, 'text required');
  const pod = requireLivePod(ctx, role);
  if (pod.agent === 'pi') {
    // runner bridge: framed message + raw paste, verified by the sidecar ack
    // (the visual probe breaks on long/wrapped lines in a TTY line editor).
    const wire = frameMessage(text);
    const res = await terminal.send(pod.terminal_target!, wire, { raw: true });
    const deadline = Date.now() + 5000;
    let acked = false;
    while (Date.now() < deadline) {
      const st = terminal.readRunnerState(ctx.store.home, pod.role);
      if (st?.lastPrompt && st.lastPrompt.text.startsWith(text.slice(0, 200))) {
        acked = true;
        break;
      }
      await new Promise((r) => setTimeout(r, 300));
    }
    if (!acked) throw new OpError(504, 'runner did not ack the message (check the pod pane)');
    const run = store.currentRun(ctx.store, role);
    if (run && !run.ended_at) {
      store.appendRunMeta(ctx.store, run.id, {
        kind: 'sent',
        bytes: Buffer.byteLength(text),
        attempts: res.attempts,
        ack: 'sidecar',
      });
    }
    return { ok: true, attempts: res.attempts, ack: 'sidecar' };
  }
  const res = await terminal.send(pod.terminal_target!, text);
  if (!res.delivered) {
    throw new OpError(503, `delivery not verified after ${res.attempts} attempts (pod busy or pane gone)`);
  }
  const run = store.currentRun(ctx.store, role);
  if (run && !run.ended_at) {
    store.appendRunMeta(ctx.store, run.id, {
      kind: 'sent',
      bytes: Buffer.byteLength(text),
      attempts: res.attempts,
    });
  }
  ctx.emit?.({ type: 'pod_sent', role, bytes: Buffer.byteLength(text), attempts: res.attempts });
  return { ok: true, attempts: res.attempts };
}

async function podCapture(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const lines = Math.max(10, Math.min(2000, Number(op.lines ?? 200)));
  const pod = requireLivePod(ctx, role);
  const text = await terminal.capture(pod.terminal_target!, lines);
  return { role, text };
}

async function podClose(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod) throw new OpError(404, `no pod: ${role}`);
  try {
    await terminal.killWindow(role);
  } catch {
    // window may already be gone
  }
  const run = store.currentRun(ctx.store, role);
  if (run && !run.ended_at) store.endRun(ctx.store, run.id, 'done');
  stopPodSocket(pod.dir);
  store.setPodState(ctx.store, role, 'closed');
  ctx.emit?.({ type: 'pod_closed', role });
  return { ok: true };
}

async function terminalCheck(ctx: CoreCtx): Promise<unknown> {
  return terminal.checkTransport(ctx.store.home);
}

// Per-pod protocol doc: pi reads AGENTS.md from its cwd on startup.
// Overwritten on every spawn/boot so protocol updates propagate.
export function podAgentsMd(role: string): string {
  return `# Pod ${role} — протокол flock

Ты — агент в pod'е системы flock. Демон core управляет тобой, оператор
видит и управляет всем через CLI flock (команды доступны тебе в bash).

## Задачи
Core присылает задачи сообщением вида:

    [flock-task <id>] <название>
    <текст задачи>

Выполни работу, затем отчитайся ИМЕННО командой (не просто текстом в ответе):

- задача выполнена:        flock task done <id>
- заблокирован:            flock task blocked <id> '<краткая причина>'
- нужен человек/решение:    flock task needs <id> '<что именно нужно>'

Не решай за оператора то, что решает только он: спроси через task needs.

## Watchdog
Можешь ставить слежку за собой/окружением: flock watchdog add ... (marker,
timer, stall, file). Полезно ждать CI/файлы, пока не завис.

## Правила
- Разрушительные операции — только после подтверждения (bash-guard спросит —
  это нормально, оператор разрешит).
- Статус задачи меняется ТОЛЬКО через flock CLI, не словами.
`;
}

// The pod protocol is a MANAGED BLOCK (v3.1): boot refresh and spawns both
// call this and the content converges; guidance blocks and user text
// outside the markers survive.
export function writePodAgentsMd(dir: string, role: string): void {
  mergeManagedBlock(path.join(dir, 'AGENTS.md'), 'flock-protocol', podAgentsMd(role));
}

async function taskAdd(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const title = String(op.title ?? '').trim();
  if (!title) throw new OpError(400, 'title required');
  // only enqueue for pods that exist (spawned or closed-then-reopenable)
  if (!store.getPodByRole(ctx.store, role)) throw new OpError(404, `no pod: ${role}`);
  const id = store.newId('t');
  store.insertTask(ctx.store, { id, title, body: op.body ? String(op.body) : null, podRole: role });
  ctx.emit?.({ type: 'task_added', taskId: id, pod: role });
  return store.getTask(ctx.store, id);
}

async function taskHistory(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const id = String(op.id ?? '');
  const task = store.getTask(ctx.store, id);
  if (!task) throw new OpError(404, `no task: ${id}`);
  return { task, transitions: store.listTaskTransitions(ctx.store, id) };
}

async function taskReport(op: Record<string, unknown>, ctx: CoreCtx, to: string): Promise<unknown> {
  const id = String(op.id ?? '');
  const task = store.getTask(ctx.store, id);
  if (!task) throw new OpError(404, `no task: ${id}`);
  const by = op.registeredBy ? String(op.registeredBy) : 'cli';
  const reason =
    (to === 'blocked' || to === 'needs') && op.reason ? String(op.reason).slice(0, 200) :
    to === 'cancelled' ? 'cancelled' : `reported by ${by}`;
  // done: optional result text (carried into the next workflow step)
  const result = to === 'blocked' || to === 'needs' ? reason :
    to === 'done' && op.result ? String(op.result).slice(0, 400) : null;
  try {
    store.setTaskStatus(ctx.store, id, to, { reason, result });
  } catch (e) {
    throw new OpError(409, e instanceof Error ? e.message : String(e));
  }
  ctx.emit?.({ type: `task_${to}`, taskId: id, pod: task.pod_role, reason });
  advanceWorkflow(ctx, id);
  return store.getTask(ctx.store, id);
}

// ---------- workflows ----------

interface WfStep { id: string; role: string; title?: string }

function parseSteps(raw: unknown): WfStep[] {
  if (!Array.isArray(raw) || raw.length === 0) throw new OpError(400, 'steps: non-empty array required');
  const steps = raw as WfStep[];
  const ids = new Set<string>();
  for (const s of steps) {
    if (!s || typeof s.id !== 'string' || !s.id.trim() || typeof s.role !== 'string' || !s.role.trim()) {
      throw new OpError(400, 'each step needs {id, role}');
    }
    if (ids.has(s.id)) throw new OpError(400, `duplicate step id: ${s.id}`);
    ids.add(s.id);
  }
  return steps;
}

function enqueueStepTask(ctx: CoreCtx, inst: store.WorkflowInstance, wf: store.Workflow, step: WfStep, prevResult?: string): void {
  const bodyLines = [
    `Workflow ${wf.name}: шаг ${step.id}`,
    inst.payload ? `Payload: ${inst.payload}` : '',
    prevResult ? `Результат предыдущего шага: ${prevResult}` : '',
  ].filter(Boolean);
  store.insertTask(ctx.store, {
    id: store.newId('t'),
    title: `[wf ${wf.name}] ${step.title ?? step.id}`,
    body: bodyLines.join('\n') || null,
    podRole: step.role,
    workflowInstanceId: inst.id,
    workflowStep: step.id,
  });
}

// A workflow step task finished (done/blocked/cancelled) → move the instance:
// done + next step exists → enqueue next step (frontier advance);
// done + last step → instance done; blocked/cancelled → instance stops.
export function advanceWorkflow(ctx: CoreCtx, taskId: string): void {
  const task = store.getTask(ctx.store, taskId);
  if (!task || !task.workflow_instance_id) return;
  const inst = store.getWorkflowInstance(ctx.store, task.workflow_instance_id);
  if (!inst || inst.state !== 'running') return;
  const wf = store.getWorkflow(ctx.store, inst.workflow_id);
  if (!wf) return;
  const steps: WfStep[] = (JSON.parse(wf.spec) as { steps: WfStep[] }).steps;
  if (task.status === 'done') {
    const idx = steps.findIndex((s) => s.id === task.workflow_step);
    const next = idx >= 0 ? steps[idx + 1] : undefined;
    if (!next) {
      store.setWorkflowInstanceState(ctx.store, inst.id, 'done');
      ctx.emit?.({ type: 'workflow_done', instanceId: inst.id, workflow: wf.name });
      return;
    }
    enqueueStepTask(ctx, inst, wf, next, task.result ?? undefined);
    store.setWorkflowInstanceState(ctx.store, inst.id, 'running', next.id);
    ctx.emit?.({ type: 'workflow_step', instanceId: inst.id, step: next.id, role: next.role });
  } else {
    const state = task.status === 'cancelled' ? 'cancelled' : 'blocked';
    store.setWorkflowInstanceState(ctx.store, inst.id, state);
    ctx.emit?.({ type: `workflow_${state}`, instanceId: inst.id, reason: task.result ?? task.status });
  }
}

async function workflowDefine(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const name = String(op.name ?? '').trim();
  if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(name)) throw new OpError(400, `bad workflow name: ${name || '(empty)'}`);
  const steps = parseSteps(op.steps);
  const existing = store.getWorkflow(ctx.store, name);
  if (existing) throw new OpError(409, `workflow exists: ${name}`);
  const id = store.newId('wf');
  store.insertWorkflow(ctx.store, { id, name, spec: JSON.stringify({ steps }) });
  ctx.emit?.({ type: 'workflow_defined', name, steps: steps.length });
  return store.getWorkflow(ctx.store, id)!;
}

async function workflowStart(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const wf = store.getWorkflow(ctx.store, String(op.name ?? ''));
  if (!wf) throw new OpError(404, `no workflow: ${String(op.name ?? '')}`);
  const steps: WfStep[] = (JSON.parse(wf.spec) as { steps: WfStep[] }).steps;
  const instId = store.newId('wfi');
  store.insertWorkflowInstance(ctx.store, { id: instId, workflowId: wf.id, payload: op.payload ? String(op.payload) : null });
  store.setWorkflowInstanceState(ctx.store, instId, 'running', steps[0].id);
  enqueueStepTask(ctx, { id: instId, workflow_id: wf.id, payload: op.payload ? String(op.payload) : null, state: 'running', current_step: steps[0].id, created_at: '', finished_at: null }, wf, steps[0]);
  ctx.emit?.({ type: 'workflow_started', instanceId: instId, workflow: wf.name });
  return { instance: store.getWorkflowInstance(ctx.store, instId), workflow: wf };
}

async function workflowStatus(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const id = String(op.id ?? '');
  const inst = store.getWorkflowInstance(ctx.store, id);
  if (!inst) throw new OpError(404, `no workflow instance: ${id}`);
  return { instance: inst, workflow: store.getWorkflow(ctx.store, inst.workflow_id), tasks: store.listTasksForInstance(ctx.store, id) };
}

async function watchdogRegister(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const policy = String(op.policy ?? '');
  if (!WATCHDOG_POLICIES.includes(policy as PolicyName)) {
    throw new OpError(400, `unknown policy: ${policy || '(empty)'} (want ${WATCHDOG_POLICIES.join(' | ')})`);
  }
  const targetPod = String(op.target ?? '');
  if (!ROLE_RE.test(targetPod)) throw new OpError(400, `bad target pod (want [a-z0-9-]): ${targetPod || '(empty)'}`);
  const specObj = (op.spec && typeof op.spec === 'object' ? op.spec : {}) as Record<string, unknown>;
  const specErr = validateSpec(policy as PolicyName, specObj);
  if (specErr) throw new OpError(400, specErr);
  const intervalSeconds = Math.max(1, Math.min(3600, Number(op.intervalSeconds ?? 5)));
  // quiet tuning (W2): stall-type jobs default to a gentler wake cadence so a
  // repeatedly-stalled pod isn't hammered every tick. Explicit value wins.
  const isStall = policy === 'stall';
  const defaultWake = isStall ? 60 : 30;
  const wake = op.activeWakeIntervalSeconds != null ? Math.max(1, Number(op.activeWakeIntervalSeconds)) : defaultWake;
  const id = store.newId('wd');
  store.insertWatchdogJob(ctx.store, {
    id,
    policy,
    spec: JSON.stringify(specObj),
    targetPod,
    intervalSeconds,
    activeWakeIntervalSeconds: wake,
    registeredBy: String(op.registeredBy ?? 'cli'),
  });
  ctx.emit?.({ type: 'watchdog_registered', jobId: id, policy, targetPod });
  return store.getWatchdogJob(ctx.store, id);
}

async function watchdogCancel(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const id = String(op.id ?? '');
  const job = store.getWatchdogJob(ctx.store, id);
  if (!job) throw new OpError(404, `no watchdog job: ${id}`);
  if (job.state === 'active') {
    terminateJob(ctx, job, 'cancelled', new Date().toISOString());
  }
  return { ok: true, job: store.getWatchdogJob(ctx.store, id) };
}
