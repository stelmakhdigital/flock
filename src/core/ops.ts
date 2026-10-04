import fs from 'node:fs';
import path from 'node:path';
import * as store from './store.js';
import { parseTeamYaml, TeamParseError } from './team.js';
import * as terminal from './terminal.js';
import { resolveAgent, loadAgents, firstUserModel, manifestRuntime, podRuntime, type AgentManifest } from './agent.js';
import { startPodSocket, stopPodSocket } from './http.js';
import { seatPaths, validateResumeToken } from './runner-protocol.js';
import { claudeConfigDir, claudeProjectsDir, validateClaudeSessionToken, latestClaudeSession } from './claude-protocol.js';
import { validateCodexSessionToken } from './codex-protocol.js';
import { getAdapter, mergeManagedBlock, pruneManagedBlocks, type PodBinding, type RuntimeAdapter, type StartupFile } from './runtime-adapter.js';
import { WATCHDOG_POLICIES, validateSpec, terminateJob, type PolicyName } from './watchdog.js';
import { listAlerts } from './health.js';
import { validateIntent, applyIntents, pmDigest, pmNotify } from './pm.js';
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
  // who is calling through /api/ops: undefined = operator/core internal,
  // { kind: 'pod', role } = request arrived on a pod's scoped socket
  caller?: { kind: 'pod'; role: string };
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
  const d = OP_REGISTRY[t];
  if (!d) throw new OpError(400, `unknown op: ${t || '(empty)'}`);
  // pod-scoped token: only 'pod'-scoped ops are reachable; the operator
  // token (and core internals, which carry no caller) are unrestricted.
  // Per-resource narrowing (own pod / own tasks) happens in the handlers.
  if (ctx.caller?.kind === 'pod' && !d.scopes.includes('pod')) {
    throw new OpError(403, `op ${t} is not available to pod tokens`);
  }
  d.validate?.(o, ctx);
  const result = await d.run(o, ctx);
  // C5: the append-only event log — one row per successful mutation. The
  // log is the memory of the system (board / audit / events tail); the
  // single-writer guarantee (one process) makes this race-free.
  try {
    store.insertEvent(ctx.store, {
      kind: t,
      actor: ctx.caller?.kind === 'pod' ? `pod:${ctx.caller.role}` : 'core',
      subject: String(o.id ?? o.role ?? o.name ?? o.to ?? o.file ?? ''),
      payload: opPayload(o),
    });
  } catch {
    // event log is audit, never blocks the mutation itself
  }
  return result;
}

// A bounded projection of the op into the event payload (long strings
// trimmed so the log stays readable; secrets never ride ops).
function opPayload(o: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(o)) {
    if (k === 'type') continue;
    if (typeof v === 'string') out[k] = v.length > 300 ? v.slice(0, 300) + '…' : v;
    else if (v !== null && typeof v === 'object') out[k] = JSON.stringify(v).slice(0, 300);
    else out[k] = v;
  }
  return out;
}

// Op registry: every op is {group, summary, scopes, validate?, run}.
// apply() is the single mutation path (CLI, ticks, pm intents all route
// through it); the registry makes ops introspectable (/api/ops) and gives a
// home for per-scope authorization (5.3: pod-scoped tokens get the 'pod'
// scope). validate = cheap pre-checks before run (most validation stays in
// the handler, where it can see ctx).
export interface OpDef {
  group: string;
  summary: string;
  scopes: Array<'operator' | 'pod'>;
  validate?: (o: Record<string, unknown>, ctx: CoreCtx) => void;
  run: (o: Record<string, unknown>, ctx: CoreCtx) => Promise<unknown> | unknown;
}

export const OP_REGISTRY: Record<string, OpDef> = {
  // -- pods -----------------------------------------------------------------
  pod_spawn: { group: 'pod', scopes: ['operator'], summary: 'spawn a pod (agent manifest, plain-dir; git is owned by the agents)', run: (o, c) => podSpawn(o, c) },
  pod_relaunch: { group: 'pod', scopes: ['operator'], summary: 'new run on the same pod (honest resume, --fork)', run: (o, c) => podRelaunch(o, c) },
  pod_set_resume_token: { group: 'pod', scopes: ['operator'], summary: 'pin/reset the session used for resume', run: (o, c) => podSetResumeToken(o, c) },
  team_up: { group: 'team', scopes: ['operator'], summary: 'reconcile a pods.yaml team (spawn missing, refresh live)', run: (o, c) => teamUp(o, c) },
  esc_ls: { group: 'team', scopes: ['operator', 'pod'], summary: '5.4c durable escalations (ladder audit)', run: (o, c) => { const activeOnly = o.active === true || o.active === 'true'; return store.listEscalations(c.store, activeOnly); } },
  esc_ack: { group: 'team', scopes: ['operator'], summary: '5.4c acknowledge an escalation (stops operator reminders)', run: (o, c) => { const id = o.id as string | undefined; if (!id) throw new Error('id required'); const row = store.getEscalation(c.store, id); if (!row) throw new Error('escalation not found'); if (!store.ESC_ACTIVE_STATES.includes(row.state as (typeof store.ESC_ACTIVE_STATES)[number]) && row.state !== 'pm_notified') throw new Error(`escalation is ${row.state}`); store.setEscalationState(c.store, id, 'acknowledged', { resolvedReason: 'operator ack' }); c.emit?.({ type: 'escalation_resolved', id, key: row.key, reason: 'operator ack' }); return { ok: true, id, state: 'acknowledged' }; } },
  pod_send: { group: 'pod', scopes: ['operator'], summary: 'send text to a live pod (transport, verified)', run: (o, c) => podSend(o, c) },
  pod_answer: { group: 'pod', scopes: ['operator'], summary: 'answer a pending dialog (gate) in a pod', run: (o, c) => podAnswer(o, c) },
  pod_capture: { group: 'pod', scopes: ['operator'], summary: 'capture pod pane text', run: (o, c) => podCapture(o, c) },
  pod_close: { group: 'pod', scopes: ['operator', 'pod'], summary: 'close a pod (kill window, state=closed)', run: (o, c) => podClose(o, c) },
  // -- watchdog --------------------------------------------------------------
  watchdog_register: { group: 'watchdog', scopes: ['operator'], summary: 'register a watchdog check (agent-registered)', run: (o, c) => watchdogRegister(o, c) },
  watchdog_cancel: { group: 'watchdog', scopes: ['operator'], summary: 'cancel a watchdog job', run: (o, c) => watchdogCancel(o, c) },
  watchdog_list: { group: 'watchdog', scopes: ['operator'], summary: 'list watchdog jobs', run: (_o, c) => ({ jobs: store.listWatchdogJobs(c.store) }) },
  // -- workflows -------------------------------------------------------------
  workflow_define: { group: 'workflow', scopes: ['operator'], summary: 'define a workflow (named step list)', run: (o, c) => workflowDefine(o, c) },
  workflow_start: { group: 'workflow', scopes: ['operator'], summary: 'start a workflow instance', run: (o, c) => workflowStart(o, c) },
  workflow_rm: { group: 'workflow', scopes: ['operator'], summary: 'delete a workflow definition (instances survive)', run: (o, c) => workflowRm(o, c) },
  workflow_ls: { group: 'workflow', scopes: ['operator'], summary: 'list workflows and instances', run: (_o, c) => ({ workflows: store.listWorkflows(c.store), instances: store.listWorkflowInstances(c.store) }) },
  workflow_status: { group: 'workflow', scopes: ['operator'], summary: 'instance status (steps, states)', run: (o, c) => workflowStatus(o, c) },
  // -- tasks -----------------------------------------------------------------
  task_add: { group: 'task', scopes: ['operator', 'pod'], summary: 'add a task to the queue (pod must exist; pod token: own pod only)', run: (o, c) => taskAdd(o, c) },
  task_list: { group: 'task', scopes: ['operator', 'pod'], summary: 'list tasks (filter by status, limit default 50)', run: (o, c) => ({ tasks: store.listTasks(c.store, o.status ? String(o.status) : undefined, Math.max(1, Math.min(500, Number(o.limit ?? 50)))) }) },
  task_history: { group: 'task', scopes: ['operator', 'pod'], summary: 'task transitions (audit)', run: (o, c) => taskHistory(o, c) },
  task_cancel: { group: 'task', scopes: ['operator'], summary: 'cancel a task', run: async (o, c) => { await pmNotifyMaybe(c, 'task_cancelled', o, 'cancelled'); return taskReport(o, c, 'cancelled'); } },
  task_unblock: { group: 'task', scopes: ['operator', 'pod'], summary: 'unblock a task (-> queued; pod token: own pod only)', run: async (o, c) => { await pmNotifyMaybe(c, 'task_unblocked', o, 'queued'); return taskReport(o, c, 'queued'); } },
  task_done: { group: 'task', scopes: ['operator', 'pod'], summary: 'close a task (C3 hot-potato: {reason, target?} from the closure vocabulary)', run: async (o, c) => { await pmNotifyMaybe(c, 'task_done', o, 'done'); return taskReport(o, c, 'done'); } },
  task_handoff: { group: 'task', scopes: ['operator', 'pod'], summary: 'transactional handoff: close (handed-off) + create the successor at {to}', run: (o, c) => taskHandoff(o, c) },
  // -- messages (C4: inboxes + outboxes) ---------------------------------
  message_send: { group: 'message', scopes: ['operator', 'pod'], summary: 'send a durable message to a pod inbox (+ poke if live; from = caller pod or operator)', run: (o, c) => messageSend(o, c) },
  message_list: { group: 'message', scopes: ['operator', 'pod'], summary: 'list inbox messages (pod token: own inbox; {unclaimed?})', run: (o, c) => messageList(o, c) },
  message_claim: { group: 'message', scopes: ['operator', 'pod'], summary: 'mark an inbox message claimed (pod token: own inbox only)', run: (o, c) => messageClaim(o, c) },
  task_blocked: { group: 'task', scopes: ['operator', 'pod'], summary: 'report task blocked (pod-scoped: own pod only)', run: async (o, c) => { await pmNotifyMaybe(c, 'task_blocked', o, 'blocked'); return taskReport(o, c, 'blocked'); } },
  task_needs: { group: 'task', scopes: ['operator', 'pod'], summary: 'report task needs help (pod-scoped: own pod only)', run: async (o, c) => { await pmNotifyMaybe(c, 'task_needs', o, 'needs'); return taskReport(o, c, 'needs'); } },
  // -- pm / goal loop ---------------------------------------------------------
  pm_up: { group: 'pm', scopes: ['operator'], summary: 'start the pm pod (goal loop)', run: (_o, c) => pmUp(c) },
  pm_state: { group: 'pm', scopes: ['operator'], summary: 'pm digest snapshot + alerts', run: async (_o, c) => ({ pm: await pmDigest(c), alerts: listAlerts(c) }) },
  pm_intents: { group: 'pm', scopes: ['operator'], summary: 'typed intents from pm (whitelist, applied via apply)', run: (o, c) => pmIntents(o, c) },
  // -- health / system ---------------------------------------------------------
  health_list: { group: 'health', scopes: ['operator', 'pod'], summary: 'built-in health alerts (gate/idle)', run: (_o, c) => ({ alerts: listAlerts(c) }) },
  terminal_check: { group: 'system', scopes: ['operator'], summary: 'tmux transport self-check', run: (_o, c) => terminalCheck(c) },
};

// Introspection: op names + metadata (for /api/ops, `flock ops ls`, and
// future scope enforcement).
export function listOps(): Array<{ type: string; group: string; summary: string; scopes: string[] }> {
  return Object.entries(OP_REGISTRY)
    .map(([type, d]) => ({ type, group: d.group, summary: d.summary, scopes: d.scopes }))
    .sort((a, b) => (a.group + a.type).localeCompare(b.group + b.type));
}

// Shared spawn path for pod_spawn and pod_relaunch.
// v3.1: the RuntimeAdapter owns projection, startup delivery, harness launch
// and the typed ready gate; core owns state. pi: honest resume via exact
// session file (relaunch) / fork via native_id; C6: a failed resume is attention_required (operator decides, explicit --fresh)ing
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
  profile?: string | null;
  freshStart?: boolean;
  teamGuidance?: string; // team file: extra AGENTS.md block (managed, id team:<role>)
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
    const r = resolveAgent(agentId, null, o.profile ?? undefined);
    if (!r) throw new OpError(400, `unknown agent: ${agentId} (want ${Object.keys(loadAgents()).join(' | ')})`);
    const manifest = r.manifest;
    agentStored = r.id;
    const adapter = getAdapter(manifest, adapterEnv(ctx));
    if (!adapter) throw new OpError(400, `agent ${agentId}: no runtime adapter (manifest needs a supported "runtime")`);

    const childEnv: Record<string, string> = { ...(manifest.env ?? {}) };
    if (adapter.runtime === 'pi' && !model) model = firstUserModel();
    if ((adapter.runtime === 'claude' || adapter.runtime === 'codex') && !model) {
      // pi config uses provider/model syntax; claude/codex want the bare model id
      const fm = firstUserModel();
      model = fm ? (fm.split('/').pop() ?? fm) : fm;
    }
    const binding: PodBinding = {
      role: o.role,
      cwd: o.dir,
      model: model ?? undefined,
      launchPosture: o.posture ?? manifest.launchPosture,
      permissionMode: manifest.permissionMode,
      extraEnv: Object.keys(childEnv).length ? childEnv : undefined,
      seatRoot: path.join(ctx.store.home, 'pods', o.role),
      trustLevel: manifest.trustLevel,
      // T1: first-class pi axes from the resolved manifest (pi runtime only —
      // other adapters ignore the block)
      pi: adapter.runtime === 'pi' ? piAxesFromManifest(manifest) : undefined,
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
      required: true, // missing guidance = degraded agent: fail the launch, don't ship it
    }));
    if (o.freshStart !== false && manifest.firstPrompt) {
      startup.push({ path: 'first-prompt', content: manifest.firstPrompt, deliveryHint: 'send_text', appliesOn: ['fresh_start'] });
    }
    adapter.project(binding);
    const pre = await adapter.deliverStartup(startup, binding, 'pre_launch');
    // required startup files are launch-blocking: a failed required file is a
    // startup error, not a warning
    if (pre.failed.length) {
      throw new OpError(500, `startup delivery failed: ${pre.failed.map((f) => `${f.path}: ${f.error}`).join('; ')}`);
    }
    // team file: extra guidance block (pi reads it at process start)
    if (o.teamGuidance) {
      mergeManagedBlock(path.join(o.dir, 'AGENTS.md'), `team:${o.role}`, o.teamGuidance);
    }
    // guidance hygiene: drop managed blocks the current manifest no longer
    // provides (e.g. left over from a previous profile)
    pruneManagedBlocks(path.join(o.dir, 'AGENTS.md'), new Set(['flock-protocol', 'team:' + o.role, ...(manifest.guidance ?? []).map((g) => g.id)]));

    let launchId = store.newId('la');
    let launch = await adapter.launchHarness(binding, {
      launchId,
      resumeToken: o.resumeToken,
      forkSource: o.forkRef ? { kind: 'native_id', value: o.forkRef } : undefined,
    });
    if (!launch.ok) {
      // C6 strict honest resume: a failed resume/fork fails loudly and STAYS
      // failed. recovery 'attention_required' means the operator decides
      // (explicit --fresh, or fix the pin) — there is no automatic fresh
      // fallback, not even a "honest" one: the relaunch simply fails with
      // the evidence.
      const recovery = launch.recovery ?? 'attention_required';
      runMeta.resume = o.resumeToken || o.forkRef ? `failed_${recovery}` : runMeta.resume;
      throw new OpError(409, `launch failed: ${launch.error}${launch.evidence ? `\n${launch.evidence}` : ''}${o.resumeToken || o.forkRef ? `\nrecovery: ${recovery} — an explicit fresh start is: flock pod relaunch ${o.role} --fresh` : ''}`);
    }
    target = launch.target;
    pid = launch.pid ?? null;
    if (runMeta.resume === undefined) runMeta.resume = launch.mode;
    runMeta.launchId = launchId;
    runMeta.runtime = adapter.runtime;
    if (launch.trust) runMeta.trust = launch.trust;
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
    profile: o.profile ?? null,
  });
  const run = store.insertRun(ctx.store, { id: store.newId('run'), podRole: o.role, pid, meta: runMeta });
  startPodSocket(ctx, o.dir, o.role); // pod-local API socket (sandbox-visible)
  ctx.emit?.({ type: 'pod_spawned', role: o.role, target, run: run.id });
  return { pod: store.getPodByRole(ctx.store, o.role)!, run };
}

const launchPostureUsed = (b: PodBinding): boolean => b.launchPosture !== undefined && b.launchPosture !== 'floor';

// T1: extract the pi config axes from a resolved manifest (only the axes
// that are actually set — an unset axis must not reach the runner flag).
function piAxesFromManifest(m: AgentManifest): PodBinding['pi'] {
  const out: NonNullable<PodBinding['pi']> = {};
  if (m.thinking != null) out.thinking = m.thinking;
  if (m.tools?.length) out.tools = m.tools;
  if (m.excludeTools?.length) out.excludeTools = m.excludeTools;
  if (m.skills?.length) out.skills = m.skills;
  if (m.noSkills != null) out.noSkills = m.noSkills;
  if (m.extensions?.length) out.extensions = m.extensions;
  if (m.noExtensions != null) out.noExtensions = m.noExtensions;
  if (m.systemPrompt != null) out.systemPrompt = m.systemPrompt;
  if (m.appendSystemPrompt?.length) out.appendSystemPrompt = m.appendSystemPrompt;
  if (m.noContextFiles != null) out.noContextFiles = m.noContextFiles;
  if (m.mcp && Object.keys(m.mcp).length > 0) out.mcp = m.mcp;
  return Object.keys(out).length ? out : undefined;
}

async function podSpawn(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const existing = store.getPodByRole(ctx.store, role);
  if (existing && existing.state !== 'closed') {
    throw new OpError(409, `pod ${role} already ${existing.state}`);
  }
  // plain-dir pod: the pod's work directory (git is owned by the agents —
  // core never touches it)
  const dir = String(op.dir ?? path.join(ctx.store.home, 'pods', role));
  const { pod, run } = await spawnAgent(ctx, {
    role,
    dir,
    model: op.model ? String(op.model) : undefined,
    agentId: op.agent ? String(op.agent) : undefined,
    rawCmd: op.cmd ? String(op.cmd) : undefined,
    forkRef: op.fork ? resolveForkRef(ctx, String(op.fork)) : undefined,
    posture: op.posture === 'full_bypass' || op.posture === 'floor' ? op.posture : undefined,
    profile: op.profile ? String(op.profile) : null,
  });
  return { pod, run };
}

// relaunch: agent dies (or operator wants a fresh one) -> new run on the same
// pod. pi (v3.1): HONEST resume — the exact persisted session file is
// relaunched (--session <file>), never an interactive picker; a missing file
// is attention_required (recorded in the run meta, never silent; C6). bash: fresh window.
const adapterEnv = (ctx: CoreCtx) => ({ home: ctx.store.home, token: ctx.store.token, runnerPath: path.join(import.meta.dirname, 'runner.js'), codexBridgePath: path.join(import.meta.dirname, 'codex-bridge.js') });

// Resolve the adapter for a stored pod (manifest -> runtime -> adapter) and
// the minimal binding the signal-contract methods need (role/cwd/seatRoot).
// null = no adapter (cmd windows, unknown/broken manifests) — the caller
// degrades (generic pid check / visual probe / skip health).
// ponytail: the signal methods (liveness/sendVerified/healthProbe) only read
// the pod's cwd + seatRoot — model/posture/trust are launch-time axes and
// don't affect the answers.
export function adapterForPod(pod: { role: string; dir: string; agent: string | null }, ctx: CoreCtx): { adapter: RuntimeAdapter; binding: PodBinding } | null {
  if (!pod.agent || pod.agent === 'cmd') return null;
  try {
    const resolved = resolveAgent(pod.agent, null);
    if (!resolved || manifestRuntime(resolved.manifest) === 'cmd') return null;
    const adapter = getAdapter(resolved.manifest, adapterEnv(ctx));
    if (!adapter) return null;
    const binding: PodBinding = {
      role: pod.role,
      cwd: pod.dir,
      seatRoot: path.join(ctx.store.home, 'pods', pod.role),
    };
    return { adapter, binding };
  } catch {
    return null; // broken manifest: degrade, the error surfaces at spawn
  }
}

export async function podRelaunch(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod) throw new OpError(404, `no pod: ${role}`);
  const cur = pod;
  if (pod.agent === 'cmd') throw new OpError(400, `relaunch not supported for raw-cmd pods (cmd is not stored)`);
  const run = store.currentRun(ctx.store, role);
  if (run && !run.ended_at) store.endRun(ctx.store, run.id, 'replaced');
  // PERSISTENT PANE: no killWindow — the adapter typed-stops the old runner
  // (C-c) and launches into the same window.
  const profile = op.profile !== undefined ? String(op.profile) : (cur.profile ?? undefined);
  store.setPodProfile(ctx.store, role, profile || null);
  const resolved = resolveAgent(cur.agent ?? undefined, null, profile);
  const runtime = resolved ? manifestRuntime(resolved.manifest) : 'cmd';
  const isPi = runtime === 'pi';
  const isClaude = runtime === 'claude';
  const isCodex = runtime === 'codex';
  // pinned resume token wins; otherwise the latest session (honest resume).
  // Token shape is per-runtime: pi = session file path, claude = transcript
  // uuid, codex = thread id (UUID v7).
  const pinned = isPi || isClaude || isCodex ? (cur.resume_token ?? undefined) : undefined;
  const tokenValid = isPi ? validateResumeToken(pinned!) : isCodex ? validateCodexSessionToken(pinned!) : validateClaudeSessionToken(pinned!);
  if (pinned && !tokenValid) {
    throw new OpError(400, `pinned resume token is invalid: ${pinned} (flock pod resume-token ${role} reset)`);
  }
  // C6 strict honest resume: --fresh is the ONLY way to start fresh; a
  // failed resume (missing file / corrupt sidecar / no rollout) fails
  // loudly as attention_required — the operator then chooses --fresh or
  // fixes the pin. There is no automatic fresh fallback.
  const fresh = op.fresh === true || op.fresh === 'true';
  let resumeToken: string | undefined;
  const forkRef = op.fork !== undefined ? resolveForkRef(ctx, String(op.fork)) : undefined;
  if (!fresh) {
    if (isPi) {
      resumeToken = forkRef ? undefined : pinned ?? store.latestSessionFile(ctx.store, role) ?? undefined;
    } else if (isClaude || isCodex) {
      const adapter = getAdapter(resolved!.manifest, adapterEnv(ctx));
      resumeToken = forkRef ? undefined : pinned ?? (await adapter?.latestSessionToken?.({ role, cwd: cur.dir, seatRoot: path.join(ctx.store.home, 'pods', role) })) ?? undefined;
    }
  }
  const res = await spawnAgent(ctx, {
    role,
    dir: cur.dir,
    model: op.model ? String(op.model) : (cur.model ?? undefined),
    agentId: cur.agent ?? undefined,
    resumeToken,
    forkRef,
    profile: profile ?? null,
    freshStart: !resumeToken && !forkRef,
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

async function podSetResumeToken(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod) throw new OpError(404, `no pod: ${role}`);
  const rt = podRuntime(pod.agent);
  const raw = op.token === undefined || op.token === '' || op.token === 'reset' ? null : String(op.token);
  if (raw !== null) {
    const valid = rt === 'codex' ? validateCodexSessionToken(raw) : validateResumeToken(raw);
    if (!valid) throw new OpError(400, `invalid resume token: ${raw}`);
  }
  store.setPodResumeToken(ctx.store, role, raw);
  ctx.emit?.({ type: 'pod_resume_token', role, token: raw });
  return { role, resume_token: raw, source: raw ? 'pinned' : 'latest_session_file' };
}

// --fork <role|path>: role -> that pod's current session file (session-id is
// in the filename); path is used as-is.
function resolveForkRef(ctx: CoreCtx, ref: string): string {
  if (ref.includes('/') || ref.includes('\\')) return ref;
  const pod = store.getPodByRole(ctx.store, ref);
  if (!pod) throw new OpError(404, `no pod to fork from: ${ref}`);
  if (podRuntime(pod.agent) === 'claude') {
    const s = latestClaudeSession(claudeProjectsDir(claudeConfigDir(pod.dir), pod.dir));
    if (!s) throw new OpError(404, `no session for pod ${ref} (no transcript yet)`);
    return s.token;
  }
  if (podRuntime(pod.agent) === 'codex') {
    // the fork ref is a codex thread id (UUID — no slashes, so it reaches
    // here only as a role ref): the pod's current thread from the sidecar
    const st = terminal.readRunnerState(ctx.store.home, ref);
    if (!st?.sessionId) throw new OpError(404, `no session for pod ${ref} (no codex thread yet)`);
    return st.sessionId;
  }
  const dir = seatPaths(ctx.store.home, ref).sessionsDir;
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
  const resolved = adapterForPod(pod, ctx);
  // Verified delivery through the runtime's own typed signal (pi: sidecar
  // nonce-ack; claude: transcript growth). The core does not know which one
  // it is — the adapter answers.
  if (resolved?.adapter.sendVerified) {
    const r = await resolved.adapter.sendVerified(resolved.binding, text);
    if (!r.ok) throw new OpError(504, r.detail ?? 'delivery not verified (check the pod pane)');
    const run = store.currentRun(ctx.store, role);
    if (run && !run.ended_at) {
      store.appendRunMeta(ctx.store, run.id, {
        kind: 'sent',
        bytes: Buffer.byteLength(text),
        attempts: r.attempts ?? 0,
        ack: r.ack,
      });
    }
    ctx.emit?.({ type: 'pod_sent', role, bytes: Buffer.byteLength(text), attempts: r.attempts ?? 0 });
    return { ok: true, attempts: r.attempts ?? 0, ack: r.ack };
  }
  // Fallback (bash/cmd, adapter without sendVerified): legacy visual probe —
  // the capture-verified send. Deliberate degradation, not an error.
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

// Answer a pending extension dialog (permission gate) in a pi pod.
// The pane is a text mirror — the operator's answer travels as a `/answer`
// line the runner translates into an extension_ui_response for the pending
// dialog (stage 4.1). Delivery is fire-and-log: the dialog either resolves
// (activity ext_dialog_answered) or the runner explains why not (pane line).
async function podAnswer(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const arg = String(op.arg ?? '1').trim();
  if (!arg) throw new OpError(400, 'arg required (option number or value)');
  const pod = requireLivePod(ctx, role);
  if (podRuntime(pod.agent) !== 'pi') throw new OpError(400, `pod_answer is for pi-runtime pods (this is: ${podRuntime(pod.agent)})`);
  const res = await terminal.send(pod.terminal_target!, `/answer ${arg}`, { raw: true });
  const run = store.currentRun(ctx.store, role);
  if (run && !run.ended_at) {
    store.appendRunMeta(ctx.store, run.id, { kind: 'dialog_answer', arg: arg.slice(0, 200), attempts: res.attempts });
  }
  ctx.emit?.({ type: 'pod_dialog_answered', role, arg: arg.slice(0, 200) });
  return { ok: true, attempts: res.attempts };
}

async function podCapture(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const lines = Math.max(10, Math.min(2000, Number(op.lines ?? 200)));
  const pod = requireLivePod(ctx, role);
  if (ctx.caller?.kind === 'pod' && ctx.caller.role !== role) {
    throw new OpError(403, `pod token ${ctx.caller.role}: cannot capture pod ${role}`);
  }
  const text = await terminal.capture(pod.terminal_target!, lines);
  return { role, text };
}

async function podClose(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod) throw new OpError(404, `no pod: ${role}`);
  if (ctx.caller?.kind === 'pod' && ctx.caller.role !== role) {
    throw new OpError(403, `pod token ${ctx.caller.role}: cannot close pod ${role}`);
  }
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

- задача выполнена:        flock task done <id> '<reason: finished|blocked|denied|canceled|escalated>'
  (reason из этого набора — закрытие без причины отклоняется core)
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
  // pod token: own pod only (a pod cannot queue work for another pod)
  if (ctx.caller?.kind === 'pod' && ctx.caller.role !== role) {
    throw new OpError(403, `pod token ${ctx.caller.role}: cannot add a task for pod ${role}`);
  }
  const id = store.newId('t');
  store.insertTask(ctx.store, {
    id,
    title,
    body: op.body ? String(op.body) : null,
    podRole: role,
  });
  ctx.emit?.({ type: 'task_added', taskId: id, pod: role });
  void pmNotify(ctx, { type: 'task_added', detail: `таск ${id} "${title.slice(0, 80)}" → pod ${role} (очередь)` }).catch(() => {});
  return store.getTask(ctx.store, id);
}

async function taskHistory(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const id = String(op.id ?? '');
  const task = store.getTask(ctx.store, id);
  if (!task) throw new OpError(404, `no task: ${id}`);
  return { task, transitions: store.listTaskTransitions(ctx.store, id) };
}

// pm (goal loop): wake on a task status change — but only when the report
// came from outside the pm itself (the pm's own intents must not re-wake it).
async function pmNotifyMaybe(ctx: CoreCtx, trigger: string, o: Record<string, unknown>, to: string): Promise<void> {
  try {
    const id = String(o.id ?? '');
    const task = id ? store.getTask(ctx.store, id) : null;
    if (!task) return;
    if (o.by === 'pm' || o.registeredBy === 'pm') return;
    const reason = o.reason ? ` — ${String(o.reason).slice(0, 120)}` : '';
    await pmNotify(ctx, { type: trigger, detail: `таск ${task.id} "${task.title.slice(0, 80)}" → ${to}${reason}` });
  } catch {
    /* pm wake is best-effort; the task op itself already committed */
  }
}

// pm (goal loop): ensure the pm pod exists and is live (spawn or relaunch).
async function pmUp(ctx: CoreCtx): Promise<unknown> {
  const pod = store.getPodByRole(ctx.store, 'pm');
  if (pod && pod.state === 'live') return { pm: pod, action: 'already_live' };
  const dir = pod?.dir ?? path.join(ctx.store.home, 'pods', 'pm');
  if (pod && pod.state === 'closed') {
    return apply({ type: 'pod_relaunch', role: 'pm' }, ctx);
  }
  return apply({ type: 'pod_spawn', role: 'pm', dir, agent: 'pm' }, ctx);
}

// pm (goal loop): typed intent batch — whitelist-validated, applied through
// the single mutation path, per-intent results back to the caller.
async function pmIntents(o: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const raw = Array.isArray(o.intents) ? o.intents : Array.isArray(o) ? (o as Record<string, unknown>[]) : null;
  if (!raw || raw.length === 0) throw new OpError(400, 'intents: non-empty array required (one object or {"intents":[...]})');
  if (raw.length > 20) throw new OpError(400, 'intents: max 20 per batch');
  const results = await applyIntents(ctx, raw);
  const failed = results.filter((r) => !r.ok);
  return { ok: failed.length === 0, results, applied: results.length - failed.length, failed: failed.length };
}

async function taskReport(op: Record<string, unknown>, ctx: CoreCtx, to: string): Promise<unknown> {
  const id = String(op.id ?? '');
  const task = store.getTask(ctx.store, id);
  if (!task) throw new OpError(404, `no task: ${id}`);
  // idempotent: same-state re-report is a no-op (agent double-report vs
  // arbiter/CLI races must not 409)
  if (task.status === to) return task;
  // terminal tasks never re-open (a second `task done` after the pod
  // self-reported would double-report)
  if (task.status === 'done' || task.status === 'cancelled') return task;
  // frozen step task: a stopped workflow instance (blocked/cancelled/done)
  // does not accept late state changes — the instance state and the task
  // state must not drift apart (a bash pod can self-report the protocol
  // line after the operator already rejected the step)
  if (task.workflow_instance_id) {
    const inst = store.getWorkflowInstance(ctx.store, task.workflow_instance_id);
    if (inst && inst.state !== 'running') {
      throw new OpError(409, `workflow instance ${inst.id} is ${inst.state} — the step task is frozen`);
    }
  }
  // pod token: own pod's tasks only (an agent cannot report on another
  // pod's work or cancel operator tasks)
  if (ctx.caller?.kind === 'pod' && ctx.caller.role !== task.pod_role) {
    throw new OpError(403, `pod token ${ctx.caller.role}: task ${id} belongs to pod ${task.pod_role}`);
  }
  const by = op.registeredBy ? String(op.registeredBy) : 'cli';
  if (to === 'done') {
    // C3: hot-potato — a done task must carry its closure (reason from the
    // vocabulary; target for handed-off/escalated)
    const reason = String(op.reason ?? '').trim();
    if (!reason) {
      throw new OpError(400, `task done requires a closure reason (want: ${store.CLOSURE_REASONS.join(' | ')})`);
    }
    try {
      store.closeWorkItem(ctx.store, id, 'done', {
        reason: reason as store.ClosureReason,
        target: op.target !== undefined ? String(op.target) : undefined,
        by,
        result: op.result !== undefined ? String(op.result).slice(0, 400) : null,
      });
    } catch (e) {
      throw new OpError(409, e instanceof Error ? e.message : String(e));
    }
    ctx.emit?.({ type: 'task_done', taskId: id, pod: task.pod_role, reason });
    advanceWorkflow(ctx, id);
    return store.getTask(ctx.store, id);
  }
  if (to === 'cancelled') {
    // C3: cancelled is terminal too — closed with reason 'canceled'
    try {
      store.closeWorkItem(ctx.store, id, 'cancelled', { reason: 'canceled', by, result: null });
    } catch (e) {
      throw new OpError(409, e instanceof Error ? e.message : String(e));
    }
    ctx.emit?.({ type: 'task_cancelled', taskId: id, pod: task.pod_role, reason: 'cancelled' });
    advanceWorkflow(ctx, id);
    return store.getTask(ctx.store, id);
  }
  // non-terminal (blocked / needs / queued via unblock): status only, no
  // closure — the escalation ladder (5.4c) reads these as before
  const reason = (to === 'blocked' || to === 'needs') && op.reason ? String(op.reason).slice(0, 200) : 'unblocked';
  try {
    store.setTaskStatus(ctx.store, id, to, { reason, result: to === 'blocked' || to === 'needs' ? reason : null });
  } catch (e) {
    throw new OpError(409, e instanceof Error ? e.message : String(e));
  }
  ctx.emit?.({ type: `task_${to}`, taskId: id, pod: task.pod_role, reason });
  advanceWorkflow(ctx, id);
  return store.getTask(ctx.store, id);
}

// C3: transactional handoff — the task closes (reason 'handed-off') and the
// successor is created for the target pod in the same store transaction.
async function taskHandoff(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const id = String(op.id ?? '');
  const to = String(op.to ?? '').trim();
  const task = store.getTask(ctx.store, id);
  if (!task) throw new OpError(404, `no task: ${id}`);
  if (task.status === 'done' || task.status === 'cancelled') throw new OpError(409, `task ${id} is already ${task.status}`);
  if (task.workflow_instance_id) {
    throw new OpError(400, `task ${id} is a workflow step — close it with task_done (the DAG decides the next step)`);
  }
  // pod token: own pod's tasks only
  if (ctx.caller?.kind === 'pod' && ctx.caller.role !== task.pod_role) {
    throw new OpError(403, `pod token ${ctx.caller.role}: task ${id} belongs to pod ${task.pod_role}`);
  }
  if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(to)) throw new OpError(400, `bad target role: ${to}`);
  const targetPod = store.getPodByRole(ctx.store, to);
  if (!targetPod) throw new OpError(404, `no pod: ${to} (spawn it first)`);
  const by = op.registeredBy ? String(op.registeredBy) : 'cli';
  try {
    const r = store.handoffTask(ctx.store, id, to, by);
    ctx.emit?.({ type: 'task_handoff', taskId: id, from: task.pod_role, to, newTaskId: r.to.id });
    return { closed: r.from, next: r.to };
  } catch (e) {
    throw new OpError(409, e instanceof Error ? e.message : String(e));
  }
}

// ---------- messages (C4) ----------

// message_send: the durable coordination primitive. The inbox row is the
// record (survives restarts); the pane poke is a best-effort wake-up through
// the same verified transport as pod_send (a dead target keeps the message
// in its inbox — it is never lost).
async function messageSend(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const to = String(op.to ?? '').trim();
  const text = String(op.text ?? '').trim();
  if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(to)) throw new OpError(400, `bad target role: ${to}`);
  if (!text) throw new OpError(400, 'text required');
  const targetPod = store.getPodByRole(ctx.store, to);
  if (!targetPod) throw new OpError(404, `no pod: ${to} (spawn it first)`);
  const from = ctx.caller?.kind === 'pod' ? ctx.caller.role : 'operator';
  // pod token: can only address other pods (own inbox is pointless)
  if (ctx.caller?.kind === 'pod' && to === ctx.caller.role) {
    throw new OpError(400, `cannot message your own pod: ${to} (read your inbox with message_list)`);
  }
  const { inboxId, outboxId } = store.insertInboxMessage(ctx.store, { from, to, text });
  ctx.emit?.({ type: 'message_sent', from, to, inboxId, outboxId });
  // delivery: poke the target through the verified transport (best-effort)
  let poked: boolean | 'target-not-live' | 'failed' = false;
  if (targetPod.state === 'live') {
    try {
      await apply(
        { type: 'pod_send', role: to, text: `[flock-message from ${from}]\n${text}` },
        ctx,
      );
      poked = true;
    } catch {
      poked = 'failed'; // the inbox row already committed; the message is durable
    }
  } else {
    poked = 'target-not-live';
  }
  return { ok: true, inboxId, outboxId, poked };
}

async function messageList(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  // pod token: own inbox only; operator: any pod's inbox (or all unclaimed)
  const to = ctx.caller?.kind === 'pod' ? ctx.caller.role : (String(op.to ?? '').trim() || null);
  if (to && !/^[a-z0-9][a-z0-9-]{0,30}$/.test(to)) throw new OpError(400, `bad role: ${to}`);
  const unclaimed = op.unclaimed === true || op.unclaimed === 'true';
  const limit = Math.max(1, Math.min(500, Number(op.limit ?? 50)));
  const messages = to
    ? store.listInboxMessages(ctx.store, to, unclaimed, limit)
    : (() => { // operator without {to}: unclaimed across all pods
        const pods = store.listPods(ctx.store);
        const all: store.InboxMessage[] = [];
        for (const p of pods) all.push(...store.listInboxMessages(ctx.store, p.role, true, limit));        return all.sort((a, b) => (a.at < b.at ? 1 : -1)).slice(0, limit);
      })();
  return { to: to ?? '(all)', messages };
}

async function messageClaim(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const id = Number(op.id ?? 0);
  if (!Number.isInteger(id) || id <= 0) throw new OpError(400, 'id required (inbox message id)');
  const msg = store.getInboxMessage(ctx.store, id);
  if (!msg) throw new OpError(404, `no inbox message: ${id}`);
  if (ctx.caller?.kind === 'pod' && msg.to !== ctx.caller.role) {
    throw new OpError(403, `pod token ${ctx.caller.role}: message ${id} belongs to pod ${msg.to}`);
  }
  store.markInboxClaimed(ctx.store, id, msg.to);
  ctx.emit?.({ type: 'message_claimed', id, to: msg.to });
  return { ok: true, id, to: msg.to };
}

// ---------- team ----------

// `flock team up <pods.yaml>`: reconcile a declared team. Live pods are only
// refreshed (guidance re-merged, picked up at next launch); missing or closed
// pods are spawned. Never kills a live pod — a team file change takes effect
// on relaunch/close, which the operator does deliberately.
async function teamUp(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const file = path.resolve(String(op.file ?? 'pods.yaml'));
  let src: string;
  try {
    src = fs.readFileSync(file, 'utf8');
  } catch (e) {
    throw new OpError(400, `cannot read team file ${file}: ${e instanceof Error ? e.message : String(e)}`);
  }
  let spec;
  try {
    spec = parseTeamYaml(src);
  } catch (e) {
    if (e instanceof TeamParseError) throw new OpError(400, e.message);
    throw e;
  }
  const results: Record<string, unknown> = {};
  for (const [role, s] of Object.entries(spec.pods)) {
    const existing = store.getPodByRole(ctx.store, role);
    if (existing && existing.state === 'live') {
      let refreshed = false;
      if (s.guidance && podRuntime(existing.agent) === 'pi') {
        mergeManagedBlock(path.join(existing.dir, 'AGENTS.md'), `team:${role}`, s.guidance);
        refreshed = true;
      }
      results[role] = { action: 'live', guidance: refreshed ? 'refreshed (applies at next launch)' : undefined };
      continue;
    }
    try {
      const res = (await apply({
        type: 'pod_spawn',
        role,
        agent: s.agent,
        model: s.model,
        profile: s.profile,
        posture: s.posture,
        teamGuidance: s.guidance,
      }, ctx)) as { run?: { id: string } };
      results[role] = { action: existing ? 'respawned' : 'spawned', run: res?.run?.id };
    } catch (e) {
      results[role] = { action: 'failed', error: e instanceof Error ? e.message : String(e) };
    }
  }
  ctx.emit?.({ type: 'team_up', file, pods: Object.keys(spec.pods) });
  return { file, results };
}


// ---------- workflows ----------

interface WfStep {
  id: string;
  role: string;
  title?: string;
  // C7: the scribe model — no priority/retry/TTL knobs on steps. Stuck
  // detection is the watchdog's job (escalation ladder), retry is an
  // explicit operator action (task_unblock / relaunch).
  // 5.4d: DAG — ids of steps that must be done before this step starts.
  // Absent/empty = start with the instance (the sequential pipeline is the
  // special case: step N deps on step N-1).
  deps?: string[];
}

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
    if (s.deps != null && (!Array.isArray(s.deps) || s.deps.some((d) => typeof d !== 'string' || !d))) {
      throw new OpError(400, `step ${s.id}: deps must be an array of step ids`);
    }
  }
  // 5.4d: deps reference existing steps, no self-deps, no cycles
  for (const s of steps) {
    for (const d of s.deps ?? []) {
      if (d === s.id) throw new OpError(400, `step ${s.id}: cannot depend on itself`);
      if (!ids.has(d)) throw new OpError(400, `step ${s.id}: unknown dep: ${d}`);
    }
  }
  const indeg = new Map(steps.map((s) => [s.id, (s.deps ?? []).length]));
  const dependents = new Map<string, string[]>(steps.map((s) => [s.id, []]));
  for (const s of steps) for (const d of s.deps ?? []) dependents.get(d)!.push(s.id);
  const q = steps.filter((s) => (s.deps ?? []).length === 0).map((s) => s.id);
  let seen = 0;
  while (q.length) {
    const cur = q.shift()!;
    seen++;
    for (const nxt of dependents.get(cur) ?? []) {
      indeg.set(nxt, (indeg.get(nxt) ?? 1) - 1);
      if ((indeg.get(nxt) ?? 0) === 0) q.push(nxt);
    }
  }
  if (seen !== steps.length) throw new OpError(400, 'steps contain a dependency cycle');
  return steps;
}

function enqueueStepTask(ctx: CoreCtx, inst: store.WorkflowInstance, wf: store.Workflow, step: WfStep, depsResult?: string | null): void {
  const bodyLines = [
    `Workflow ${wf.name}: шаг ${step.id}`,
    inst.payload ? `Payload: ${inst.payload}` : '',
    step.deps?.length ? `Зависимости: ${step.deps.join(', ')} (уже done)` : '',
    depsResult ?? '',
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

// 5.4d: results of a step's deps (their done-task results) for the task body
function depsResultLine(ctx: CoreCtx, inst: store.WorkflowInstance, step: WfStep): string | null {
  if (!step.deps?.length) return null;
  const instTasks = store.listTasksForInstance(ctx.store, inst.id);
  const parts: string[] = [];
  for (const d of step.deps) {
    const done = instTasks.filter((t) => t.workflow_step === d && t.status === 'done').pop();
    if (done?.result) parts.push(`[${d}] ${String(done.result).slice(0, 400)}`);
  }
  return parts.length ? `Результаты зависимых шагов:\n${parts.join('\n')}` : null;
}

// 5.4d: the DAG frontier — steps that are pending and whose deps are all done
function readySteps(steps: WfStep[], states: Record<string, string>): WfStep[] {
  return steps.filter((s) => {
    if ((states[s.id] ?? 'pending') !== 'pending') return false;
    return (s.deps ?? []).every((d) => (states[d] ?? 'pending') === 'done');
  });
}

// A workflow step task finished (done/blocked/cancelled) → move the instance:
// done → mark the step done, enqueue the newly-ready steps (DAG frontier;
// the sequential pipeline is the degenerate case), done + nothing left →
// instance done; blocked/cancelled → the scribe records it and stops the
// instance (C7: no retry budget; unblock + advance is the operator's move).
export function advanceWorkflow(ctx: CoreCtx, taskId: string): void {
  const task = store.getTask(ctx.store, taskId);
  if (!task || !task.workflow_instance_id) return;
  const inst = store.getWorkflowInstance(ctx.store, task.workflow_instance_id);
  if (!inst || inst.state !== 'running') return;
  const wf = store.getWorkflow(ctx.store, inst.workflow_id);
  if (!wf) return;
  const steps: WfStep[] = (JSON.parse(wf.spec) as { steps: WfStep[] }).steps;
  if (task.status === 'done') {
    store.setWfStepState(ctx.store, inst.id, task.workflow_step!, 'done');
    const states = store.wfStepStateMap(ctx.store, inst.id);
    const incomplete = steps.filter((s) => (states[s.id] ?? 'pending') !== 'done');
    if (incomplete.length === 0) {
      store.setWorkflowInstanceState(ctx.store, inst.id, 'done');
      ctx.emit?.({ type: 'workflow_done', instanceId: inst.id, workflow: wf.name });
      return;
    }
    const ready = readySteps(steps, states);
    if (ready.length === 0) {
      const anyRunning = steps.some((s) => (states[s.id] ?? 'pending') === 'running');
      if (anyRunning) return; // waiting for in-flight steps — not a deadlock
      // defensive: validation forbids cycles/unknown deps, so this is
      // unreachable in a normal spec — but never leave a stuck instance
      store.setWorkflowInstanceState(ctx.store, inst.id, 'blocked');
      ctx.emit?.({ type: 'workflow_blocked', instanceId: inst.id, reason: 'dag deadlock: no ready step while the instance is incomplete' });
      return;
    }
    for (const s of ready) {
      store.setWfStepState(ctx.store, inst.id, s.id, 'running');
      enqueueStepTask(ctx, inst, wf, s, depsResultLine(ctx, inst, s));
    }
    store.setWorkflowInstanceState(ctx.store, inst.id, 'running', ready.map((s) => s.id).join(','));
    ctx.emit?.({ type: 'workflow_step', instanceId: inst.id, step: ready.map((s) => s.id).join(','), role: ready.map((s) => s.role).join(',') });
  } else {
    // C7 scribe: the runtime records the failure and stops the instance.
    // There is no automatic retry budget — a failed step is the operator's
    // decision (unblock the task and advance again, or cancel). The
    // escalation ladder (5.4c) watches the blocked instance for the audit.
    store.setWfStepState(ctx.store, inst.id, task.workflow_step!, 'blocked');
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
  // 5.4b S2: instance-level quality gate flag (default: off, per-start flag can override)
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
  store.insertWorkflowInstance(ctx.store, { id: instId, workflowId: wf.id, payload: op.payload ? String(op.payload) : null, priority: 0 });
  // 5.4d: start the DAG frontier — every step without deps (a sequential
  // pipeline: only steps[0] is ready at start, as before)
  const instObj = { id: instId, workflow_id: wf.id, payload: op.payload ? String(op.payload) : null, state: 'running', current_step: steps[0].id, created_at: '', finished_at: null, priority: 0 } as store.WorkflowInstance;
  const frontier = readySteps(steps, {});
  for (const s of frontier) {
    store.setWfStepState(ctx.store, instId, s.id, 'running');
    enqueueStepTask(ctx, instObj, wf, s);
  }
  store.setWorkflowInstanceState(ctx.store, instId, 'running', frontier.map((s) => s.id).join(','));
  ctx.emit?.({ type: 'workflow_started', instanceId: instId, workflow: wf.name, frontier: frontier.map((s) => s.id) });
  return { instance: store.getWorkflowInstance(ctx.store, instId), workflow: wf };
}

async function workflowStatus(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const id = String(op.id ?? '');
  const inst = store.getWorkflowInstance(ctx.store, id);
  if (!inst) throw new OpError(404, `no workflow instance: ${id}`);
  return { instance: inst, workflow: store.getWorkflow(ctx.store, inst.workflow_id), tasks: store.listTasksForInstance(ctx.store, id), stepState: store.listWfStepStates(ctx.store, id) };
}

async function workflowRm(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const name = String(op.name ?? '').trim();
  const wf = store.getWorkflow(ctx.store, name);
  if (!wf) throw new OpError(404, `no workflow: ${name}`);
  store.deleteWorkflow(ctx.store, wf.id);
  ctx.emit?.({ type: 'workflow_removed', name });
  return { ok: true, name };
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
