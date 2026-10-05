import fs from 'node:fs';
import path from 'node:path';
import * as store from './store.js';
import { parseTeamYaml, TeamParseError } from './team.js';
import { listTeams, listSnapshots, readSnapshot, readTeamSpec, saveSnapshot, validateTeamName, type SnapshotPod } from './team-snap.js';
import * as terminal from './terminal.js';
import { resolveAgent, loadAgents, firstUserModel, manifestRuntime, podRuntime, type AgentManifest, type ResolvedAgent } from './agent.js';
import { startPodSocket, stopPodSocket } from './http.js';
import { seatPaths, validateResumeToken } from './bridge-protocol.js';
import { getAdapter, mergeManagedBlock, pruneManagedBlocks, type PodBinding, type RuntimeAdapter, type StartupFile } from './runtime-adapter.js';
import { WATCHDOG_POLICIES, validateSpec, terminateJob, type PolicyName } from './watchdog.js';
import { listAlerts } from './health.js';
import { validateIntent, applyIntents, pmDigest, notifyPm } from './pm.js';
import { listPacks, readPackMeta, buildPackBundle } from './packs.js';
import { readWorkspace, resolveWorkspaceRef, workspacePath } from './workspace.js';
import { piList, pluginShowDetail, type PluginEntry } from './plugins.js';
import * as fleet from './fleet.js';
import { topologySpec, renderTopologyYaml } from './topologies.js';
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

// C8 nuance: the pm pod is the COORDINATOR. The PM_PROTOCOL promises it
// task/pod management over the whole team (task_add for any pod,
// task_cancel/unblock/handoff for any task, pod_spawn/relaunch/close,
// workflow_start, pm_intents). Every other pod keeps its narrow scope.
// Role-based elevation: exactly the role 'pm', never a manifest field.
function isCoordinator(ctx: CoreCtx): boolean {
  return ctx.caller?.kind === 'pod' && ctx.caller.role === 'pm';
}
// Gate for elevated ops reached with a pod token: only the pm role passes.
function requireCoordinator(ctx: CoreCtx, what: string): void {
  if (ctx.caller?.kind === 'pod' && !isCoordinator(ctx)) {
    throw new OpError(403, `pod token ${ctx.caller.role}: ${what} is a coordinator (pm) action`);
  }
}
// Per-resource narrowing shared by task_* handlers: own pod's tasks for
// plain pods, anything for the coordinator.
function assertTaskScope(ctx: CoreCtx, taskPodRole: string, what: string): void {
  if (ctx.caller?.kind === 'pod' && ctx.caller.role !== taskPodRole && !isCoordinator(ctx)) {
    throw new OpError(403, `pod token ${ctx.caller.role}: task belongs to pod ${taskPodRole} (${what} is own-pod only)`);
  }
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
    // secrets never ride the event log (the token is the fleet credential)
    if (k === 'token' || k === 'authorization' || k === 'password') {
      out[k] = '<redacted>';
      continue;
    }
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
  pod_spawn: { group: 'pod', scopes: ['operator', 'pod'], summary: 'spawn a pod (agent manifest, plain-dir; git is owned by the agents; pod token: pm only)', run: (o, c) => { requireCoordinator(c, 'pod_spawn'); return podSpawn(o, c); } },
  pod_adopt: { group: 'pod', scopes: ['operator', 'pod'], summary: 'adopt a live tmux pane as a pod (move it into the core session, no restart; pod token: pm only)', run: (o, c) => { requireCoordinator(c, 'pod_adopt'); return podAdopt(o, c); } },
  pod_relaunch: { group: 'pod', scopes: ['operator', 'pod'], summary: 'new run on the same pod (honest resume, --fork; pod token: pm only)', run: (o, c) => { requireCoordinator(c, 'pod_relaunch'); return podRelaunch(o, c); } },
  pod_set_resume_token: { group: 'pod', scopes: ['operator'], summary: 'pin/reset the session used for resume', run: (o, c) => podSetResumeToken(o, c) },
  team_up: { group: 'team', scopes: ['operator'], summary: 'reconcile a pods.yaml team (spawn missing, refresh live)', run: (o, c) => teamUp(o, c) },
  topology_up: { group: 'team', scopes: ['operator'], summary: 'launch a named topology preset (conveyor, adversarial-review, research-team, secrets-manager) via the team path', run: (o, c) => topologyUp(o, c) },
  esc_ls: { group: 'team', scopes: ['operator', 'pod'], summary: '5.4c durable escalations (ladder audit)', run: (o, c) => { const activeOnly = o.active === true || o.active === 'true'; return store.listEscalations(c.store, activeOnly); } },
  esc_ack: { group: 'team', scopes: ['operator'], summary: '5.4c acknowledge an escalation (stops operator reminders)', run: (o, c) => { const id = o.id as string | undefined; if (!id) throw new Error('id required'); const row = store.getEscalation(c.store, id); if (!row) throw new Error('escalation not found'); if (!store.ESC_ACTIVE_STATES.includes(row.state as (typeof store.ESC_ACTIVE_STATES)[number]) && row.state !== 'pm_notified') throw new Error(`escalation is ${row.state}`); store.setEscalationState(c.store, id, 'acknowledged', { resolvedReason: 'operator ack' }); c.emit?.({ type: 'escalation_resolved', id, key: row.key, reason: 'operator ack' }); return { ok: true, id, state: 'acknowledged' }; } },
  pod_send: { group: 'pod', scopes: ['operator'], summary: 'send text to a live pod (transport, verified)', run: (o, c) => podSend(o, c) },
  pod_capture: { group: 'pod', scopes: ['operator'], summary: 'capture pod pane text', run: (o, c) => podCapture(o, c) },
  pod_close: { group: 'pod', scopes: ['operator', 'pod'], summary: 'close a pod (kill window, state=closed)', run: (o, c) => podClose(o, c) },
  team_down: { group: 'team', scopes: ['operator'], summary: 'close all live pods of a named team + auto-snapshot (C14)', run: (o, c) => teamDown(o, c) },
  team_restore: { group: 'team', scopes: ['operator'], summary: 'restore a named team from a snapshot (C14, honest per-node outcomes)', run: (o, c) => teamRestore(o, c) },
  // -- campaigns (5.6: named persistent goals) -------------------------------
  campaign_new: { group: 'campaign', scopes: ['operator'], summary: 'create a campaign (named persistent goal); triggers pm to decompose', run: (o, c) => campaignNew(o, c) },
  campaign_ls: { group: 'campaign', scopes: ['operator', 'pod'], summary: 'list campaigns with progress (done/total)', run: (o, c) => campaignLs(o, c) },
  campaign_status: { group: 'campaign', scopes: ['operator', 'pod'], summary: 'campaign detail: goal, tasks, status, note', run: (o, c) => campaignStatus(o, c) },
  campaign_pause: { group: 'campaign', scopes: ['operator'], summary: 'pause a campaign (the tick stops walking it)', run: (o, c) => campaignMove(o, c, 'paused') },
  campaign_resume: { group: 'campaign', scopes: ['operator'], summary: 'resume a paused campaign', run: (o, c) => campaignResume(o, c) },
  campaign_cancel: { group: 'campaign', scopes: ['operator'], summary: 'cancel a campaign (status only — tasks are left as-is)', run: (o, c) => campaignMove(o, c, 'cancelled') },
  // -- agent images (C13) ----------------------------------------------------
  agent_image_save: { group: 'pod', scopes: ['operator'], summary: 'save an agent image (manifest + session copy) from a live pi pod', run: (o, c) => agentImageSave(o, c) },
  agent_image_ls: { group: 'pod', scopes: ['operator', 'pod'], summary: 'list agent images', run: (_o, c) => listImages(c.store.home) },
  agent_image_rm: { group: 'pod', scopes: ['operator'], summary: 'delete an agent image (guarded: --force required)', run: (o, c) => agentImageRm(o, c) },
  // -- fleet (C15: cross-profile coordination) --------------------------------
  fleet_add: { group: 'fleet', scopes: ['operator'], summary: 'add a fleet profile (another core) to ~/.flock/fleet.json', run: (o, c) => fleetAdd(o, c) },
  fleet_ls: { group: 'fleet', scopes: ['operator', 'pod'], summary: 'list fleet profiles (remote /healthz) + pending cross-profile work', run: (o, c) => fleetLs(o, c) },
  fleet_rm: { group: 'fleet', scopes: ['operator'], summary: 'remove a fleet profile', run: (o, c) => fleetRm(o, c) },
  fleet_handoff_accept: { group: 'fleet', scopes: ['operator'], summary: 'remote side of a two-phase cross-profile handoff (idempotent successor creation)', run: (o, c) => fleetHandoffAccept(o, c) },
  // -- watchdog --------------------------------------------------------------
  watchdog_register: { group: 'watchdog', scopes: ['operator'], summary: 'register a watchdog check (agent-registered)', run: (o, c) => watchdogRegister(o, c) },
  watchdog_cancel: { group: 'watchdog', scopes: ['operator'], summary: 'cancel a watchdog job', run: (o, c) => watchdogCancel(o, c) },
  watchdog_list: { group: 'watchdog', scopes: ['operator'], summary: 'list watchdog jobs', run: (_o, c) => ({ jobs: store.listWatchdogJobs(c.store) }) },
  // -- workflows -------------------------------------------------------------
  workflow_define: { group: 'workflow', scopes: ['operator'], summary: 'define a workflow (named step list)', run: (o, c) => workflowDefine(o, c) },
  workflow_start: { group: 'workflow', scopes: ['operator', 'pod'], summary: 'start a workflow instance (pod token: pm only)', run: (o, c) => { requireCoordinator(c, 'workflow_start'); return workflowStart(o, c); } },
  workflow_rm: { group: 'workflow', scopes: ['operator'], summary: 'delete a workflow definition (instances survive)', run: (o, c) => workflowRm(o, c) },
  workflow_ls: { group: 'workflow', scopes: ['operator'], summary: 'list workflows and instances', run: (_o, c) => ({ workflows: store.listWorkflows(c.store), instances: store.listWorkflowInstances(c.store) }) },
  workflow_status: { group: 'workflow', scopes: ['operator'], summary: 'instance status (steps, states)', run: (o, c) => workflowStatus(o, c) },
  // -- tasks -----------------------------------------------------------------
  task_add: { group: 'task', scopes: ['operator', 'pod'], summary: 'add a task to the queue (pod must exist; pod token: own pod only)', run: (o, c) => taskAdd(o, c) },
  task_list: { group: 'task', scopes: ['operator', 'pod'], summary: 'list tasks (filter by status, limit default 50)', run: (o, c) => ({ tasks: store.listTasks(c.store, o.status ? String(o.status) : undefined, Math.max(1, Math.min(500, Number(o.limit ?? 50)))) }) },
  task_history: { group: 'task', scopes: ['operator', 'pod'], summary: 'task transitions (audit)', run: (o, c) => taskHistory(o, c) },
  task_cancel: { group: 'task', scopes: ['operator', 'pod'], summary: 'cancel a task (pod token: pm only)', run: async (o, c) => { requireCoordinator(c, 'task_cancel'); await pmNotifyMaybe(c, 'task_cancelled', o, 'cancelled'); return taskReport(o, c, 'cancelled'); } },
  task_unblock: { group: 'task', scopes: ['operator', 'pod'], summary: 'unblock a task (-> queued; pod token: own pod or pm)', run: async (o, c) => { await pmNotifyMaybe(c, 'task_unblocked', o, 'queued'); return taskReport(o, c, 'queued'); } },
  task_done: { group: 'task', scopes: ['operator', 'pod'], summary: 'close a task (C3 hot-potato: {reason, target?} from the closure vocabulary)', run: async (o, c) => { await pmNotifyMaybe(c, 'task_done', o, 'done'); return taskReport(o, c, 'done'); } },
  task_gate: { group: 'task', scopes: ['operator', 'pod'], summary: 'set an owner→checker review gate on an active task (the checker receives a review task; the task cannot close as done before the verdict)', run: (o, c) => taskGate(o, c) },
  task_verdict: { group: 'task', scopes: ['operator', 'pod'], summary: 'checker verdict on a review gate (pass: close done; reject: back to queued for rework)', run: async (o, c) => { if (o.verdict === 'pass') await pmNotifyMaybe(c, 'task_done', o, 'done'); return taskVerdict(o, c); } },
  task_handoff: { group: 'task', scopes: ['operator', 'pod'], summary: 'transactional handoff: close (handed-off) + create the successor at {to}', run: (o, c) => taskHandoff(o, c) },
  // -- messages (C4: inboxes + outboxes) ---------------------------------
  message_send: { group: 'message', scopes: ['operator', 'pod'], summary: 'send a durable message to a pod inbox (+ poke if live; from = caller pod or operator)', run: (o, c) => messageSend(o, c) },
  message_list: { group: 'message', scopes: ['operator', 'pod'], summary: 'list inbox messages (pod token: own inbox; {unclaimed?})', run: (o, c) => messageList(o, c) },
  message_claim: { group: 'message', scopes: ['operator', 'pod'], summary: 'mark an inbox message claimed (pod token: own inbox only)', run: (o, c) => messageClaim(o, c) },
  message_broadcast: { group: 'message', scopes: ['operator', 'pod'], summary: 'durable broadcast to all pods (or a role subset): one inbox row per pod, + poke the live ones (pod token: pm only)', run: (o, c) => messageBroadcast(o, c) },
  task_blocked: { group: 'task', scopes: ['operator', 'pod'], summary: 'report task blocked (pod-scoped: own pod only)', run: async (o, c) => { await pmNotifyMaybe(c, 'task_blocked', o, 'blocked'); return taskReport(o, c, 'blocked'); } },
  task_needs: { group: 'task', scopes: ['operator', 'pod'], summary: 'report task needs help (pod-scoped: own pod only)', run: async (o, c) => { await pmNotifyMaybe(c, 'task_needs', o, 'needs'); return taskReport(o, c, 'needs'); } },
  // -- pm / goal loop ---------------------------------------------------------
  pm_up: { group: 'pm', scopes: ['operator'], summary: 'start the pm pod (goal loop)', run: (_o, c) => pmUp(c) },
  pm_state: { group: 'pm', scopes: ['operator'], summary: 'pm digest snapshot + alerts', run: async (_o, c) => ({ pm: await pmDigest(c), alerts: listAlerts(c) }) },
  pm_intents: { group: 'pm', scopes: ['operator', 'pod'], summary: 'typed intents from pm (whitelist, applied via apply; pod token: pm only)', run: (o, c) => { requireCoordinator(c, 'pm_intents'); return pmIntents(o, c); } },
  // -- health / system ---------------------------------------------------------
  health_list: { group: 'health', scopes: ['operator', 'pod'], summary: 'built-in health alerts (gate/idle)', run: (_o, c) => ({ alerts: listAlerts(c) }) },
  terminal_check: { group: 'system', scopes: ['operator'], summary: 'tmux transport self-check', run: (_o, c) => terminalCheck(c) },
  // -- content layer (C12: packs / workspace / plugins) ----------------------
  pack_ls: { group: 'content', scopes: ['operator', 'pod'], summary: 'list context packs (filesystem: <FLOCK_HOME>/packs/)', run: (_o, c) => packLs(c) },
  pack_show: { group: 'content', scopes: ['operator', 'pod'], summary: 'assemble + show a pack bundle (paste-ready)', run: (o, c) => packShow(o, c) },
  workspace_show: { group: 'content', scopes: ['operator', 'pod'], summary: 'show the workspace declaration (workspace.json, per profile)', run: (_o, c) => workspaceShow(c) },
  plugins_ls: { group: 'content', scopes: ['operator', 'pod'], summary: 'list the host pi extensions the pods inherit (read-only)', run: async (_o, c) => pluginsLs(c) },
  plugins_show: { group: 'content', scopes: ['operator', 'pod'], summary: 'show one pi extension entry + its SKILL.md/README (read-only)', run: (o, c) => pluginsShow(o, c) },
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
  restoreManifest?: AgentManifest; // C13: image restore — use this manifest as-is
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
    let r: ResolvedAgent;
    const agentId = o.agentId ?? 'pi';
    if (o.restoreManifest) {
      // C13 image restore: the manifest is captured verbatim in image.json —
      // resolve it directly (its agent id may not exist in this profile).
      const m = o.restoreManifest;
      r = { id: m.id, cmd: m.command, env: { ...(m.env ?? {}) }, manifest: m };
    } else {
      const resolved = resolveAgent(agentId, null, o.profile ?? undefined);
      if (!resolved) throw new OpError(400, `unknown agent: ${agentId} (want ${Object.keys(loadAgents()).join(' | ')})`);
      r = resolved;
    }
    const manifest = r.manifest;
    agentStored = r.id;
    const rt = manifestRuntime(manifest);
    // C12b: only pi + bash have adapters — a removed/unknown runtime is a
    // clean error, not a crash (revert = cherry-pick, see commit C12b).
    if (rt !== 'pi' && rt !== 'bash' && rt !== 'cmd') {
      throw new OpError(400, `agent ${agentId}: runtime not supported: ${rt} (C12b: builtins are pi + bash; a new runtime = one RuntimeAdapter + manifest)`);
    }
    const adapter = getAdapter(manifest, adapterEnv(ctx));
    if (!adapter) throw new OpError(400, `agent ${agentId}: no runtime adapter (manifest needs a supported "runtime")`);

    const childEnv: Record<string, string> = { ...(manifest.env ?? {}) };
    // FLOCK_PI_BASH_GUARD_FLOOR=1: flock pods are headless — bash-guard's
    // interactive confirm can never be answered there, so run pi with the
    // guard disabled (its catastrophic floor stays). Opt-in per profile
    // because pi REJECTS the flag when the bash-guard extension is not
    // loaded (unknown option), so it must never be a builtin default.
    if (adapter.runtime === 'pi' && process.env.FLOCK_PI_BASH_GUARD_FLOOR === '1') {
      childEnv.BASH_GUARD_AUTO_ALLOW ??= '1';
    }
    // C10: the raw manifest `child` env rides through the unified --child-args
    // channel (form a) — merged over the flock-managed vars in the bridge
    if (manifest.child?.env) Object.assign(childEnv, manifest.child.env);
    if (adapter.runtime === 'pi' && !model) model = firstUserModel();
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
      // C10: raw child args/env from the manifest (any bridge runtime)
      child: manifest.child?.args?.length || manifest.child?.env ? manifest.child : undefined,
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
    // C12: context packs -> `pack:<name>` managed blocks (filesystem-canonical:
    // the bundle is reassembled from disk at every spawn/relaunch)
    const packBlockIds: string[] = [];
    for (const packName of manifest.packs ?? []) {
      try {
        mergeManagedBlock(path.join(o.dir, 'AGENTS.md'), `pack:${packName}`, buildPackBundle(ctx.store.home, packName));
        packBlockIds.push(`pack:${packName}`);
      } catch (e) {
        // a broken pack is a launch error, not a silent shrink
        throw new OpError(400, `pack ${packName}: ${e instanceof Error ? e.message : String(e)}`);
      }
    }
    // guidance hygiene: drop managed blocks the current manifest no longer
    // provides (e.g. left over from a previous profile)
    pruneManagedBlocks(path.join(o.dir, 'AGENTS.md'), new Set(['flock-protocol', 'team:' + o.role, ...(manifest.guidance ?? []).map((g) => g.id), ...packBlockIds]));

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
    if (o.restoreManifest) runMeta.image = o.restoreManifest.id; // C13: spawned from an agent image
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
  // C13: --image <name> — resume from a saved checkpoint. The session file
  // is copied into the NEW pod's own sessions dir (seat isolation: the image
  // store never mutates on relaunch); the manifest comes from image.json.
  let imageRef: ReturnType<typeof readImage> | undefined;
  let imageSessionFile: string | undefined;
  if (op.image !== undefined) {
    imageRef = readImage(ctx.store.home, String(op.image));
    const sessionsDir = seatPaths(ctx.store.home, role).sessionsDir;
    fs.mkdirSync(sessionsDir, { recursive: true });
    const dest = path.join(sessionsDir, `from-image-${imageRef.name}-${Date.now()}-${path.basename(imageRef.sessionFile)}`);
    fs.copyFileSync(imageRef.sessionFile, dest);
    imageSessionFile = dest;
  }
  const { pod, run } = await spawnAgent(ctx, {
    role,
    dir,
    model: op.model ? String(op.model) : undefined,
    agentId: imageRef?.restore.id ?? (op.agent ? String(op.agent) : undefined),
    rawCmd: op.cmd ? String(op.cmd) : undefined,
    forkRef: op.fork ? resolveForkRef(ctx, String(op.fork)) : undefined,
    posture: op.posture === 'full_bypass' || op.posture === 'floor' ? op.posture : undefined,
    profile: op.profile ? String(op.profile) : null,
    // resume token: C13 image copy wins; C14 snapshot restore passes
    // op.resumeToken (the seat copy of the snapshot's session)
    resumeToken: imageSessionFile ?? (op.resumeToken ? String(op.resumeToken) : undefined),
    restoreManifest: imageRef?.restore,
    freshStart: !imageSessionFile && !op.fork && !op.resumeToken,
  });
  return { pod, run };
}

// relaunch: agent dies (or operator wants a fresh one) -> new run on the same
// pod. pi (v3.1): HONEST resume — the exact persisted session file is
// relaunched (--session <file>), never an interactive picker; a missing file
// is attention_required (recorded in the run meta, never silent; C6). bash: fresh window.

// adopt: attach an ALREADY RUNNING tmux pane to the core as a pod, without
// restart. The pane is moved into the core session (tmux move-pane transfers
// the process tree — nothing is re-launched) and registered as a 'cmd' pod:
// tracked by pid liveness, claim/send/capture/close work as usual, but there
// is no agent manifest to resume from (honest: an adopted session is not
// re-launchable by the core).
async function podAdopt(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const ref = String(op.pane ?? '').trim();
  if (!ref) throw new OpError(400, 'pane required (e.g. %12 or other:0.1 — `flock pod discover` lists them)');
  const existing = store.getPodByRole(ctx.store, role);
  if (existing && existing.state !== 'closed') {
    throw new OpError(409, `pod ${role} already ${existing.state}`);
  }
  const info = await terminal.paneInfo(ref);
  if (!info) throw new OpError(404, `no live pane: ${ref}`);
  if (info.session === terminal.TMUX_SESSION) {
    throw new OpError(400, `pane ${info.paneId} is already in the core session (${info.session}) — use pod spawn/relaunch`);
  }
  if (await terminal.windowExists(role)) {
    throw new OpError(409, `pod window already exists: ${terminal.winTarget(role)} (close the pod first: flock pod close ${role})`);
  }
  const dir = String(op.dir ?? (info.cwd || path.join(ctx.store.home, 'pods', role)));
  try {
    await terminal.movePaneToPodWindow(info.paneId, role);
  } catch (e) {
    throw new OpError(409, `adopt failed: ${e instanceof Error ? e.message : String(e)}`);
  }
  store.openPod(ctx.store, {
    id: existing?.id ?? store.newId('pod'),
    role,
    dir,
    terminalTarget: terminal.winTarget(role),
    model: null,
    agent: 'cmd', // tracked by pid liveness; no resume source (honest)
  });
  const cur = store.currentRun(ctx.store, role);
  if (cur && !cur.ended_at) store.endRun(ctx.store, cur.id, 'replaced');
  const run = store.insertRun(ctx.store, {
    id: store.newId('run'),
    podRole: role,
    pid: info.pid,
    meta: { kind: 'adopted', source: info.target, pane: info.paneId, cmd: info.cmd },
  });
  ctx.emit?.({ type: 'pod_adopted', role, source: info.target, pane: info.paneId });
  return { pod: store.getPodByRole(ctx.store, role), run, source: info.target };
}
const adapterEnv = (ctx: CoreCtx) => ({ home: ctx.store.home, token: ctx.store.token, runnerPath: path.join(import.meta.dirname, 'pi-bridge.js') });

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
  // C12b: claude/codex are gone — a pod stored on a removed runtime is a
  // clean refusal, not a crash (re-spawn it with a supported agent).
  if (!isPi && runtime !== 'bash' && runtime !== 'cmd') {
    throw new OpError(400, `pod ${role}: runtime not supported: ${runtime} (C12b: re-spawn with a pi or bash agent)`);
  }
  const imageRef = op.image !== undefined ? readImage(ctx.store.home, String(op.image)) : undefined;
  if (imageRef) {
    if (!isPi) throw new OpError(400, `image ${imageRef.name} is for pi pods; pod ${role} runtime: ${runtime}`);
    if (op.fresh === true || op.fresh === 'true') {
      throw new OpError(400, '--image and --fresh conflict (image is a resume from a known-good checkpoint)');
    }
  }
  // pinned resume token wins; otherwise the latest session (honest resume).
  // pi: session file path (the only runtime with a session in C12b).
  const pinned = isPi && !imageRef ? (cur.resume_token ?? undefined) : undefined;
  const tokenValid = isPi ? validateResumeToken(pinned!) : true;
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
    }
  }
  if (imageRef) {
    // the operator explicitly chose this checkpoint (over pin/latest/fork)
    resumeToken = imageRef.sessionFile;
  }
  const res = await spawnAgent(ctx, {
    role,
    dir: cur.dir,
    model: op.model ? String(op.model) : (cur.model ?? undefined),
    agentId: imageRef?.restore.id ?? cur.agent ?? undefined,
    restoreManifest: imageRef?.restore,
    resumeToken,
    forkRef,
    profile: profile ?? null,
    freshStart: !resumeToken && !forkRef && !imageRef,
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

// ── C13: agent images ────────────────────────────────────────────────────
// A captured snapshot of a productive agent's resumable state: the pod's
// manifest (verbatim) + a copy of its session file. Filesystem-canonical:
// ~/.flock/images/<name>/image.json + session-<name>.jsonl. sqlite is not
// touched. Deletion is guarded (--force) — the image is evidence.

interface AgentImageMeta {
  name: string;          // image name (dir name)
  sourceRole: string;    // the pod the image was saved from
  agentId: string;       // the pod's stored agent id
  manifestId: string;    // manifest id inside restore
  profile: string | null;
  model: string | null;
  savedAt: string;       // ISO
  sessionFile: string;   // ABSOLUTE path of the copied session
  restore: AgentManifest; // the verbatim manifest
}

const IMAGE_NAME_RE = /^[a-z0-9][a-z0-9._-]{0,63}$/i;

function imagesDir(home: string): string {
  return path.join(home, 'images');
}

export function validateImageName(name: string): boolean {
  return IMAGE_NAME_RE.test(name);
}

export function readImage(home: string, name: string): AgentImageMeta {
  if (!validateImageName(name)) throw new OpError(400, `invalid image name: ${name}`);
  const p = path.join(imagesDir(home), name, 'image.json');
  if (!fs.existsSync(p)) throw new OpError(404, `no image: ${name} (flock agent image ls)`);
  let meta: AgentImageMeta;
  try {
    meta = JSON.parse(fs.readFileSync(p, 'utf8')) as AgentImageMeta;
  } catch (e) {
    throw new OpError(500, `corrupt image ${name}: ${e instanceof Error ? e.message : String(e)}`);
  }
  if (!fs.existsSync(meta.sessionFile)) {
    throw new OpError(500, `image ${name} is broken: session file missing (${meta.sessionFile})`);
  }
  return meta;
}

export function listImages(home: string): { name: string; agent: string; source: string; savedAt: string }[] {
  const dir = imagesDir(home);
  let entries: string[] = [];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => d.name);
  } catch {
    return [];
  }
  const out: { name: string; agent: string; source: string; savedAt: string }[] = [];
  for (const name of entries.sort()) {
    const p = path.join(dir, name, 'image.json');
    if (!fs.existsSync(p)) continue; // a broken dir: skip in ls (loud on read)
    let m: AgentImageMeta;
    try {
      m = JSON.parse(fs.readFileSync(p, 'utf8')) as AgentImageMeta;
    } catch {
      continue; // unreadable image.json — skip (loud on read)
    }
    // a session copy that is gone = a broken image: skip it in ls too
    // (readImage stays loud — ls never lies, it only omits)
    if (!m.sessionFile || !fs.existsSync(m.sessionFile)) continue;
    out.push({ name, agent: m.manifestId, source: m.sourceRole, savedAt: m.savedAt });
  }
  return out;
}

async function agentImageSave(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const name = String(op.as ?? '');
  if (!validateImageName(name)) throw new OpError(400, `invalid image name: ${name || '(empty)'} (want [a-z0-9._-], max 64)`);
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod) throw new OpError(404, `no pod: ${role}`);
  const rt = podRuntime(pod.agent);
  if (rt !== 'pi') throw new OpError(400, `image save is for pi pods only (pod ${role} runtime: ${rt})`);
  // the manifest, verbatim (profiles not applied — the image captures what
  // the pod has; the profile is recorded for the operator's eyes)
  const resolved = resolveAgent(pod.agent ?? undefined, null);
  if (!resolved) throw new OpError(404, `pod ${role}: agent manifest not found (${pod.agent})`);
  // the session file: run-meta first (exact identity), then the seat scan
  let session: string | undefined = store.latestSessionFile(ctx.store, role) ?? undefined;
  if (!session || !fs.existsSync(session)) {
    const dir = seatPaths(ctx.store.home, role).sessionsDir;
    try {
      const files = fs.readdirSync(dir).filter((f) => f.endsWith(`_${role}.jsonl`)).sort();
      const last = files.length ? path.join(dir, files[files.length - 1]) : undefined;
      if (last) session = last;
    } catch {
      /* no sessions dir */
    }
  }
  if (!session || !fs.existsSync(session)) {
    throw new OpError(404, `no session file for pod ${role} (the pod has not written a session yet — use flock pod send to trigger one)`);
  }
  const outDir = path.join(imagesDir(ctx.store.home), name);
  fs.mkdirSync(outDir, { recursive: true });
  const sessionDest = path.join(outDir, `session-${name}.jsonl`);
  fs.copyFileSync(session, sessionDest);
  const meta: AgentImageMeta = {
    name,
    sourceRole: role,
    agentId: pod.agent ?? '',
    manifestId: resolved.manifest.id,
    profile: pod.profile,
    model: pod.model,
    savedAt: new Date().toISOString(),
    sessionFile: sessionDest,
    restore: resolved.manifest,
  };
  fs.writeFileSync(path.join(outDir, 'image.json'), JSON.stringify(meta, null, 2));
  return { name, sourceRole: role, sessionFile: sessionDest, dir: outDir };
}

async function agentImageRm(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const name = String(op.name ?? '');
  if (!validateImageName(name)) throw new OpError(400, `invalid image name: ${name || '(empty)'}`);
  const dir = path.join(imagesDir(ctx.store.home), name);
  if (!fs.existsSync(dir)) throw new OpError(404, `no image: ${name}`);
  // guarded deletion: without --force refuse with a hint (evidence protection)
  if (op.force !== true && op.force !== 'true') {
    throw new OpError(400, `image ${name} exists — deletion is guarded: confirm with --force (evidence protection)`);
  }
  fs.rmSync(dir, { recursive: true, force: true });
  return { name, removed: true };
}

async function podSetResumeToken(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod) throw new OpError(404, `no pod: ${role}`);
  const rt = podRuntime(pod.agent);
  if (rt !== 'pi') throw new OpError(400, `pod ${role}: resume token is for pi pods only (runtime: ${rt})`);
  const raw = op.token === undefined || op.token === '' || op.token === 'reset' ? null : String(op.token);
  if (raw !== null) {
    const valid = validateResumeToken(raw);
    if (!valid) throw new OpError(400, `invalid resume token: ${raw}`);
  }
  store.setPodResumeToken(ctx.store, role, raw);
  ctx.emit?.({ type: 'pod_resume_token', role, token: raw });
  return { role, resume_token: raw, source: raw ? 'pinned' : 'latest_session_file' };
}

// --fork <role|path>: role -> that pod's current session file (session-id is
// in the filename); path is used as-is.
// ── C14: team down / restore ──────────────────────────────────────────────
// Honest vocabulary: a per-node outcome, never a smoothed failure.
type NodeOutcome =
  | 'resumed' | 'rebuilt' | 'fresh' | 'fresh-primed'
  | 'awaiting-decision' | 'failed' | 'attention_required' | 'operator_recovered';

// team down: close all LIVE pods of the named team + auto-snapshot.
// A pod with a foreign runtime (non-pi) is NOT snapshotted — the honest
// note goes to the report (the prompt: "team down их не включает").
// ---------- campaigns (5.6) ----------
//
// A campaign is a named persistent goal with a lifecycle that survives
// many pm wake-ups. The ops set the goal and the operator's intent; the
// campaign tick walks the lifecycle deterministically; the pm does the
// work (decompose / unblock) through ordinary ops.

async function campaignNew(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const goal = String(op.goal ?? '').trim();
  if (!goal) throw new OpError(400, 'goal required');
  if (goal.length > 500) throw new OpError(400, 'goal too long (max 500)');
  const pod = op.pod ? String(op.pod).trim() : null;
  if (pod && !/^[a-z0-9][a-z0-9-]{0,30}$/.test(pod)) throw new OpError(400, `bad pod: ${pod}`);
  const id = store.campaignIdFromGoal(ctx.store.db, goal);
  const c = store.createCampaign(ctx.store, { id, goal, pod });
  ctx.emit?.({ type: 'campaign_status', id, from: null, to: 'planning', reason: 'created' });
  // pm trigger (durable inbox + poke): the pm decomposes the goal into
  // tasks with task_add {campaign_id} — the tick picks it up (planning -> running).
  void notifyPm(
    ctx,
    {
      type: 'campaign_new',
      detail: `Кампания ${id}: расклади цель в задачи — task_add {title, pod_role, campaign_id: "${id}"} (${pod ? `основной под: ${pod}` : 'под выбирай сам'}). Цель: ${goal.slice(0, 200)}`,
      subject: id,
    },
  ).catch(() => {});
  return c;
}

async function campaignLs(_op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const rows = store.listCampaigns(ctx.store).map((c) => {
    const ts = store.campaignTasks(ctx.store, c.id);
    const done = ts.filter((t) => t.status === 'done').length;
    return { id: c.id, status: c.status, goal: c.goal.slice(0, 120), pod: c.pod, done, total: ts.length, note: c.note, updated_at: c.updated_at };
  });
  return { campaigns: rows };
}

async function campaignStatus(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const id = String(op.id ?? '').trim();
  const c = store.getCampaign(ctx.store, id);
  if (!c) throw new OpError(404, `no campaign: ${id}`);
  const tasks = store.campaignTasks(ctx.store, id).map((t) => ({
    id: t.id,
    title: t.title.slice(0, 120),
    status: t.status,
    pod: t.pod_role,
    closed: t.closed,
  }));
  return { campaign: c, tasks };
}

async function campaignMove(op: Record<string, unknown>, ctx: CoreCtx, to: 'paused' | 'cancelled'): Promise<unknown> {
  const id = String(op.id ?? '').trim();
  const c = store.getCampaign(ctx.store, id);
  if (!c) throw new OpError(404, `no campaign: ${id}`);
  const allowed: Record<string, store.Campaign['status'][]> = {
    paused: ['planning', 'running', 'blocked'],
    cancelled: ['planning', 'running', 'blocked', 'paused'],
  };
  if (!allowed[to].includes(c.status)) {
    throw new OpError(409, `campaign ${id} is ${c.status} — cannot ${to} from here`);
  }
  const from = c.status;
  store.setCampaignStatus(ctx.store, id, to);
  ctx.emit?.({ type: 'campaign_status', id, from, to, reason: 'operator' });
  // ponytail: cancel/pause is a STATUS, not a cascade — tasks are left
  // as-is (the operator or pm closes them deliberately).
  return store.getCampaign(ctx.store, id);
}

async function campaignResume(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const id = String(op.id ?? '').trim();
  const c = store.getCampaign(ctx.store, id);
  if (!c) throw new OpError(404, `no campaign: ${id}`);
  if (c.status !== 'paused') throw new OpError(409, `campaign ${id} is ${c.status} — only paused campaigns resume`);
  // resume target: running if work exists, planning if the pm never decomposed
  const hasTasks = store.campaignTasks(ctx.store, id).length > 0;
  const to = hasTasks ? 'running' : 'planning';
  const from = c.status;
  store.setCampaignStatus(ctx.store, id, to);
  ctx.emit?.({ type: 'campaign_status', id, from, to, reason: 'operator resume' });
  return store.getCampaign(ctx.store, id);
}

async function teamDown(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const name = String(op.name ?? '');
  if (!validateTeamName(name)) throw new OpError(400, `invalid team name: ${name || '(empty)'}`);
  const { spec, file: teamFile } = readTeamSpec(ctx.store.home, name);
  const pods: SnapshotPod[] = [];
  const skipped: { role: string; reason: string }[] = [];
  const closed: Record<string, unknown> = {};
  for (const [role, s] of Object.entries(spec.pods)) {
    const pod = store.getPodByRole(ctx.store, role);
    const rt = pod ? podRuntime(pod.agent) : (s.agent === 'bash' ? 'bash' : 'pi');
    if (rt !== 'pi') {
      skipped.push({ role, reason: `runtime not snapshot-able: ${rt} (pi only in C14)` });
      continue;
    }
    // the session file: run-meta (exact identity) -> seat scan
    let session: string | null = null;
    if (pod) {
      session = store.latestSessionFile(ctx.store, role);
      if (!session || !fs.existsSync(session)) {
        const dir = seatPaths(ctx.store.home, role).sessionsDir;
        try {
          const files = fs.readdirSync(dir).filter((f) => f.endsWith(`_${role}.jsonl`)).sort();
          session = files.length ? path.join(dir, files[files.length - 1]) : null;
        } catch {
          session = null;
        }
      }
    }
    const manifest = s.agent ? (resolveAgent(s.agent, null)?.manifest ?? null) : null;
    pods.push({
      role,
      agent: pod?.agent ?? s.agent ?? null,
      manifestId: manifest?.id ?? (s.agent ?? null),
      profile: pod?.profile ?? s.profile ?? null,
      model: pod?.model ?? s.model ?? null,
      dir: pod?.dir ?? (s.dir ? (s.dir.startsWith('ws:') ? resolveWorkspaceRef(ctx.store.home, s.dir) : s.dir) : path.join(ctx.store.home, 'pods', role)),
      sessionFile: session,
      sessionCopy: null, // filled by saveSnapshot
      restorable: !!session,
    });
    // close the live pod (the snapshot copy was just made — order matters:
    // copy BEFORE close, the seat may be cleaned later)
    if (pod && pod.state === 'live') {
      try {
        await apply({ type: 'pod_close', role }, ctx);
        closed[role] = 'closed';
      } catch (e) {
        closed[role] = `close failed: ${e instanceof Error ? e.message : String(e)}`;
      }
    }
  }
  const snap = saveSnapshot(ctx.store.home, name, { teamFile, pods, skipped });
  ctx.emit?.({ type: 'team_down', team: name, snapshot: snap.id, pods: pods.length, skipped: skipped.length });
  return { team: name, snapshot: snap.id, savedAt: snap.savedAt, closed, skipped };
}

// team restore: spawn the team's pods from a snapshot, per-node honest.
// Rules (the prompt): refuse restore over a node that is already live or
// restoring; missing REQUIRED (session for a restore-able node) = hard
// failure for THAT node only; restore-safe replay (guidance) is idempotent.
async function teamRestore(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const name = String(op.name ?? '');
  if (!validateTeamName(name)) throw new OpError(400, `invalid team name: ${name || '(empty)'}`);
  const ref = String(op.restore ?? 'latest');
  const { snap, dir: snapDir } = readSnapshot(ctx.store.home, name, ref);
  const results: Record<string, { outcome: NodeOutcome; detail?: string }> = {};
  for (const p of snap.pods) {
    const existing = store.getPodByRole(ctx.store, p.role);
    // refuse-over-live: a node that is live or restoring is NOT overwritten
    if (existing && (existing.state === 'live' || existing.state === 'restoring')) {
      results[p.role] = { outcome: 'awaiting-decision', detail: `pod ${p.role} is ${existing.state} — close it first (flock pod close ${p.role}) to restore from ${snap.id}` };
      continue;
    }
    try {
      // the restore session: the snapshot copy (not the live seat file —
      // the snapshot is the checkpoint, the seat may have moved on)
      const sessionCopy = p.sessionCopy && fs.existsSync(p.sessionCopy) ? p.sessionCopy : null;
      if (!sessionCopy) {
        // required for a restorable node: hard failure for THIS node
        results[p.role] = { outcome: 'failed', detail: `no session in snapshot ${snap.id} for ${p.role} (required for restore)` };
        continue;
      }
      // copy into the NEW pod's seat (seat isolation, same as images)
      const sessionsDir = seatPaths(ctx.store.home, p.role).sessionsDir;
      fs.mkdirSync(sessionsDir, { recursive: true });
      const dest = path.join(sessionsDir, `from-snapshot-${snap.name}-${snap.id}-${path.basename(sessionCopy)}`);
      fs.copyFileSync(sessionCopy, dest);
      const res = (await apply({
        type: 'pod_spawn',
        role: p.role,
        agent: p.manifestId ?? undefined,
        model: p.model ?? undefined,
        profile: p.profile ?? undefined,
        dir: p.dir,
        resumeToken: dest, // C14: resume from the snapshot's session copy
      }, ctx)) as { run?: { id: string } };
      results[p.role] = { outcome: 'resumed', detail: `run ${res?.run?.id} (session from snapshot ${snap.id})` };
    } catch (e) {
      // per-node isolation: one broken node does not sink the team
      const msg = e instanceof Error ? e.message : String(e);
      const attention = /attention_required|failed resume/i.test(msg);
      results[p.role] = { outcome: attention ? 'attention_required' : 'failed', detail: msg };
    }
  }
  ctx.emit?.({ type: 'team_restore', team: name, snapshot: snap.id, results });
  return { team: name, snapshot: snap.id, results };
}

function resolveForkRef(ctx: CoreCtx, ref: string): string {
  if (ref.includes('/') || ref.includes('\\')) return ref;
  const pod = store.getPodByRole(ctx.store, ref);
  if (!pod) throw new OpError(404, `no pod to fork from: ${ref}`);
  if (podRuntime(pod.agent) !== 'pi') {
    throw new OpError(400, `pod ${ref}: fork is for pi pods only (runtime: ${podRuntime(pod.agent)})`);
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
  // nonce-ack). The core does not know which one it is — the adapter answers.
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
  if (ctx.caller?.kind === 'pod' && ctx.caller.role !== role && !isCoordinator(ctx)) {
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

// ---------- content layer (C12: packs / workspace / plugins) --------------

// Context packs: filesystem-canonical (no sqlite, no cache — assembly is a
// handful of small reads, done only on demand). A manifest's `packs: []`
// axis turns them into `pack:<name>` managed blocks in <pod>/AGENTS.md at
// spawn/relaunch (see spawnAgent).
function packLs(ctx: CoreCtx): unknown {
  return { packs: listPacks(ctx.store.home) };
}

function packShow(op: Record<string, unknown>, ctx: CoreCtx): unknown {
  const name = String(op.name ?? '').trim();
  if (!name) throw new OpError(400, 'name required');
  try {
    return { name, files: readPackMeta(ctx.store.home, name).files, bundle: buildPackBundle(ctx.store.home, name) };
  } catch (e) {
    throw new OpError(400, e instanceof Error ? e.message : String(e));
  }
}

function workspaceShow(ctx: CoreCtx): unknown {
  try {
    return { path: workspacePath(ctx.store.home), workspace: readWorkspace(ctx.store.home) };
  } catch (e) {
    throw new OpError(400, e instanceof Error ? e.message : String(e));
  }
}

async function pluginsLs(_ctx: CoreCtx): Promise<unknown> {
  return piList();
}

async function pluginsShow(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const source = String(op.source ?? '').trim();
  if (!source) throw new OpError(400, 'source required');
  const ls = await piList();
  if (!ls.ok) throw new OpError(500, ls.error ?? 'pi list failed');
  const entry = ls.entries.find((e) => e.source === source);
  if (!entry) throw new OpError(404, `extension not found: ${source}`);
  return { entry, detail: pluginShowDetail(entry) };
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
  // pod token: own pod only, unless the coordinator (pm) queues team work
  if (ctx.caller?.kind === 'pod' && ctx.caller.role !== role && !isCoordinator(ctx)) {
    throw new OpError(403, `pod token ${ctx.caller.role}: cannot add a task for pod ${role}`);
  }
  // 5.6: optional campaign container (validates: exists, not terminal)
  let campaignId: string | null = null;
  if (op.campaign_id !== undefined) {
    campaignId = String(op.campaign_id).trim();
    const camp = store.getCampaign(ctx.store, campaignId);
    if (!camp) throw new OpError(404, `no campaign: ${campaignId}`);
    if (camp.status === 'cancelled' || camp.status === 'done') {
      throw new OpError(409, `campaign ${campaignId} is ${camp.status} — add the task before closing it`);
    }
  }
  const id = store.newId('t');
  store.insertTask(ctx.store, {
    id,
    title,
    body: op.body ? String(op.body) : null,
    podRole: role,
    priority: op.priority !== undefined ? Number(op.priority) : undefined,
    campaignId,
  });
  ctx.emit?.({ type: 'task_added', taskId: id, pod: role });
  void notifyPm(ctx, { type: 'task_added', detail: `таск ${id} "${title.slice(0, 80)}" → pod ${role} (очередь)` }).catch(() => {});
  return store.getTask(ctx.store, id);
}

// fleet_handoff_accept: the REMOTE side of a two-phase cross-profile
// handoff (C15). Creates the successor task with the PRE-DETERMINED id
// (fixed by the sender in phase 1) — idempotent: a retry with an existing
// id returns the task as-is (the remote never duplicates the successor).
// The sender's local close is 'committed' only after this call succeeds,
// so the pending window is exactly 'local closed, remote not yet created'.
async function fleetHandoffAccept(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const id = String(op.id ?? '');
  if (!/^[a-z0-9_-]{1,64}$/.test(id)) throw new OpError(400, 'bad id');
  const pod = String(op.pod ?? '').trim();
  if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(pod)) throw new OpError(400, 'bad pod');
  const targetPod = store.getPodByRole(ctx.store, pod);
  if (!targetPod) throw new OpError(404, `no pod: ${pod} (spawn it first — the handoff stays pending until then)`);
  const existing = store.getTask(ctx.store, id);
  if (existing) return { ok: true, task: existing, created: false };
  const title = String(op.title ?? '(handoff)').trim() || '(handoff)';
  const from = String(op.from ?? 'operator');
  store.insertTask(ctx.store, {
    id,
    title,
    body: op.body ? String(op.body) : null,
    podRole: pod,
    priority: op.priority !== undefined ? Number(op.priority) : 0,
  });
  ctx.emit?.({ type: 'fleet_handoff_accepted', id, pod, from });
  return { ok: true, task: store.getTask(ctx.store, id), created: true };
}

// ---------- fleet (C15) ----------

async function fleetAdd(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const name = String(op.name ?? '').trim();
  const url = String(op.url ?? '').trim();
  if (!/^[a-z0-9][a-z0-9._-]{0,63}$/.test(name)) throw new OpError(400, `bad profile name: ${name}`);
  if (!/^https?:\/\/.+$/.test(url)) throw new OpError(400, `bad url: ${url} (expected http://host:port)`);
  if (name === 'local') throw new OpError(400, '"local" is reserved (it is this profile — address pods by bare role)');
  const cfg = fleet.loadFleet(ctx.store.home);
  cfg.profiles = cfg.profiles.filter((p) => p.name !== name);
  cfg.profiles.push({ name, url, token: op.token ? String(op.token) : undefined });
  fleet.saveFleet(ctx.store.home, cfg);
  ctx.emit?.({ type: 'fleet_profile_added', name, url });
  return { ok: true, profile: { name, url } };
}

async function fleetRm(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const name = String(op.name ?? '').trim();
  const cfg = fleet.loadFleet(ctx.store.home);
  const before = cfg.profiles.length;
  cfg.profiles = cfg.profiles.filter((p) => p.name !== name);
  if (cfg.profiles.length === before) throw new OpError(404, `no fleet profile: ${name}`);
  fleet.saveFleet(ctx.store.home, cfg);
  ctx.emit?.({ type: 'fleet_profile_removed', name });
  return { ok: true, removed: name };
}

async function fleetLs(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const cfg = fleet.loadFleet(ctx.store.home);
  const profiles = await Promise.all(
    cfg.profiles.map(async (p) => {
      const h = await fleet.remoteHealth(p);
      return { name: p.name, url: p.url, healthy: h.ok, detail: h.detail };
    }),
  );
  const handoffs = store.listPendingOutboundHandoffs(ctx.store).map((h) => ({
    id: h.id,
    to: `${h.to_profile}/${h.to_role}`,
    attempts: h.attempts,
    last_error: h.last_error,
    status: h.status,
  }));
  const messages = store
    .listPendingOutboxMessages(ctx.store)
    .filter((m) => m.to.includes('/'))
    .map((m) => ({ outboxId: m.id, to: m.to, attempts: m.attempts, from: m.from_ }));
  return { profiles, pending: { handoffs, messages } };
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
    await notifyPm(ctx, { type: trigger, subject: task.id, detail: `таск ${task.id} "${task.title.slice(0, 80)}" → ${to}${reason}` });
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
  // pod's work or cancel operator tasks); the coordinator manages all
  assertTaskScope(ctx, task.pod_role, 'task report');
  const by = op.registeredBy ? String(op.registeredBy) : 'cli';
  if (to === 'done') {
    // review gate: a gatted task cannot be closed as done by anyone but the
    // checker's verdict (task_verdict) — enforced here, not in prompts
    const g = store.pendingGate(task);
    if (g) {
      throw new OpError(409, `task ${id} is under review gate (checker: ${g.checker}) — close it with: flock task verdict ${id} pass|reject`);
    }
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
  // pod token: own pod's tasks only; the coordinator manages all
  assertTaskScope(ctx, task.pod_role, 'handoff');
  // C15: cross-profile handoff — two-phase (honest, not atomic).
  // ponytail: an atomic cross-DB handoff is impossible under the single-
  // writer invariant (two cores = two writers). The ceiling is an EVENTUAL
  // handoff with an explicit pending state; true atomicity = a shared
  // store (a different architecture, not now).
  //   phase 1 (local, committed): the task closes 'handed-off (outbound,
  //     pending)' + the outbound_handoffs row (one transaction).
  //   phase 2 (remote): fleet_handoff_accept on the remote core (idempotent:
  //     the successor's task id is fixed by us, created exactly once).
  //     Success -> 'committed'; failure -> 'pending', the fleet tick
  //     (60s) retries, N attempts -> attention_required + escalation.
  let addr: fleet.Address;
  try {
    addr = fleet.parseAddress(to);
  } catch (e) {
    throw new OpError(400, e instanceof Error ? e.message : String(e));
  }
  if (addr.profile) {
    if (addr.profile === 'local') throw new OpError(400, 'use the bare role for the local profile');
    if (ctx.caller?.kind === 'pod') {
      throw new OpError(403, `pod tokens cannot hand off to other profiles: ${to}`);
    }
    const by = op.registeredBy ? String(op.registeredBy) : 'cli';
    const handoffId = store.newId('t'); // = the remote successor task id
    const body = [task.body, `Передано (handoff) из таска ${id} (pod ${task.pod_role}) — продолжай с места остановки.`].filter(Boolean).join('\n') || null;
    store.insertOutboundHandoff(ctx.store, id, {
      id: handoffId,
      from_role: task.pod_role,
      to_profile: addr.profile,
      to_role: addr.role,
      title: task.title,
      body,
      priority: task.priority,
      reason: `handoff out->${to}`,
      closedJson: JSON.stringify({ reason: 'handed-off', target: to, at: new Date().toISOString(), by }),
    });
    const res = await fleet.remoteApply(ctx.store.home, addr.profile, {
      type: 'fleet_handoff_accept',
      id: handoffId,
      title: task.title,
      body,
      pod: addr.role,
      priority: task.priority,
      from: task.pod_role,
    });
    if (res.ok) {
      store.commitOutboundHandoff(ctx.store, handoffId, handoffId);
      ctx.emit?.({ type: 'task_handoff', taskId: id, from: task.pod_role, to, newTaskId: handoffId, crossProfile: true, status: 'committed' });
      return { closed: store.getTask(ctx.store, id), next: { id: handoffId, pod: `${addr.profile}/${addr.role}` }, status: 'committed' };
    }
    // honest: the local close is committed, the remote creation is pending.
    ctx.emit?.({ type: 'task_handoff', taskId: id, from: task.pod_role, to, newTaskId: handoffId, crossProfile: true, status: 'pending', error: res.error });
    return {
      closed: store.getTask(ctx.store, id),
      next: { id: handoffId, pod: `${addr.profile}/${addr.role}` },
      status: 'pending',
      error: res.error,
      note: 'remote creation pending — the fleet tick retries (60s); `flock fleet ls` shows the queue',
    };
  }

  if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(to)) throw new OpError(400, `bad target role: ${to}`);
  // review gate: a gatted task cannot be handed off before the verdict
  const gGate = store.pendingGate(task);
  if (gGate) {
    throw new OpError(409, `task ${id} is under review gate (checker: ${gGate.checker}) — resolve it first: flock task verdict ${id} pass|reject`);
  }
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

// review gate: the owner submits the active task for review by a SEPARATE
// checker pod. One transaction (store.openReviewGate): gate row + the
// checker's review task in the ordinary queue (the arbiter claims it like
// any task — the gate goes through queue/ops, not prompts).
function taskGate(op: Record<string, unknown>, ctx: CoreCtx): unknown {
  const id = String(op.id ?? '');
  const checker = String(op.checker ?? '').trim();
  const task = store.getTask(ctx.store, id);
  if (!task) throw new OpError(404, `no task: ${id}`);
  assertTaskScope(ctx, task.pod_role, 'set a review gate');
  if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(checker)) throw new OpError(400, `bad checker role: ${checker}`);
  if (checker === task.pod_role) throw new OpError(400, `checker must be a separate pod (task belongs to ${task.pod_role})`);
  if (!store.getPodByRole(ctx.store, checker)) throw new OpError(404, `no pod: ${checker} (spawn it first)`);
  if (task.status !== 'active') throw new OpError(409, `gate needs an active task (task is ${task.status})`);
  const by = op.registeredBy ? String(op.registeredBy) : (ctx.caller?.kind === 'pod' ? ctx.caller.role : 'cli');
  try {
    const { reviewTaskId } = store.openReviewGate(ctx.store, id, checker, by);
    ctx.emit?.({ type: 'task_gated', taskId: id, checker, reviewTaskId });
    return { task: store.getTask(ctx.store, id), reviewTask: store.getTask(ctx.store, reviewTaskId) };
  } catch (e) {
    throw new OpError(409, e instanceof Error ? e.message : String(e));
  }
}

// The checker's verdict — the only way to close a gatted task as done.
function taskVerdict(op: Record<string, unknown>, ctx: CoreCtx): unknown {
  const id = String(op.id ?? '');
  const verdict = String(op.verdict ?? '').trim();
  const task = store.getTask(ctx.store, id);
  if (!task) throw new OpError(404, `no task: ${id}`);
  const gate = store.pendingGate(task);
  if (!gate) throw new OpError(409, `task ${id} has no pending gate`);
  // pod token: only the designated checker pod; the operator is unrestricted
  if (ctx.caller?.kind === 'pod' && ctx.caller.role !== gate.checker) {
    throw new OpError(403, `pod token ${ctx.caller.role}: verdict belongs to the checker pod (${gate.checker})`);
  }
  if (verdict === 'pass') {
    const by = ctx.caller?.kind === 'pod' ? gate.checker : 'cli';
    const t = store.resolveReviewGate(ctx.store, id, 'passed', by);
    ctx.emit?.({ type: 'task_gate_verdict', taskId: id, verdict: 'passed', by });
    advanceWorkflow(ctx, id);
    return t;
  }
  if (verdict === 'reject') {
    const reason = String(op.reason ?? '').trim() || 'rejected by review';
    const by = ctx.caller?.kind === 'pod' ? gate.checker : 'cli';
    const t = store.resolveReviewGate(ctx.store, id, 'rejected', by, reason);
    ctx.emit?.({ type: 'task_gate_verdict', taskId: id, verdict: 'rejected', by, reason });
    return t;
  }
  throw new OpError(400, `bad verdict: ${verdict} (want pass|reject)`);
}

// ---------- messages (C4) ----------

// message_send: the durable coordination primitive. The inbox row is the
// record (survives restarts); the pane poke is a best-effort wake-up through
// the same verified transport as pod_send (a dead target keeps the message
// in its inbox — it is never lost).
//
// C15: cross-profile addressing <profile>/<role> — the message goes over
// HTTP to the remote core (which does the ordinary local path: inbox row +
// poke). Locally we keep ONLY the outbox row (the delivery ledger):
// pending=1 until the remote acks; the fleet tick retries while pending.
async function messageSend(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const to = String(op.to ?? '').trim();
  const text = String(op.text ?? '').trim();
  if (!text) throw new OpError(400, 'text required');
  const from = ctx.caller?.kind === 'pod' ? ctx.caller.role : (op.from !== undefined ? String(op.from) : 'operator');

  // C15: cross-profile (the core-to-core fleet call passes `from` so the
  // remote inbox row shows the true sender)
  let addr: fleet.Address;
  try {
    addr = fleet.parseAddress(to);
  } catch (e) {
    throw new OpError(400, e instanceof Error ? e.message : String(e));
  }
  if (addr.profile) {
    if (addr.profile === 'local') throw new OpError(400, 'use the bare role for the local profile');
    if (ctx.caller?.kind === 'pod') {
      // pod tokens address only their own profile (no fleet from pods)
      throw new OpError(403, `pod tokens cannot address other profiles: ${to}`);
    }
    const outboxId = store.insertOutboxMessage(ctx.store, { from, to, text, pending: 1 });
    const res = await fleet.remoteApply(ctx.store.home, addr.profile, {
      type: 'message_send',
      to: addr.role,
      from,
      text,
    });
    if (res.ok) {
      store.markOutboxForwarded(ctx.store, outboxId);
      ctx.emit?.({ type: 'fleet_message_delivered', outboxId, to, attempts: 1 });
      return { ok: true, outboxId, to, delivered: true, remote: res.result };
    }
    // durable: the row stays pending; the fleet tick (60s) retries. The
    // operator gets an honest answer, not a fake success.
    ctx.emit?.({ type: 'fleet_message_pending', outboxId, to, error: res.error });
    return { ok: true, outboxId, to, delivered: false, pending: true, error: res.error, note: 'will be retried by the fleet tick (60s)' };
  }

  if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(to)) throw new OpError(400, `bad target role: ${to}`);
  const targetPod = store.getPodByRole(ctx.store, to);
  if (!targetPod) throw new OpError(404, `no pod: ${to} (spawn it first)`);
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
        for (const p of pods) all.push(...store.listInboxMessages(ctx.store, p.role, true, limit));
        return all.sort((a, b) => (a.at < b.at ? 1 : -1)).slice(0, limit);
      })();
  return { to: to ?? '(all)', messages };
}

// broadcast: one durable row per target pod over the SAME inbox/poke path
// as message_send (no second contour). targets: explicit role list, or ALL
// pods when omitted (the team «chatroom»). The sender is the caller (pod
// token: pm only — a random pod cannot talk to the whole team) or 'operator'.
// A dead target keeps its row (read on relaunch / via message ls); pokes
// are best-effort. Unknown role = 404 before anything is written.
async function messageBroadcast(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const text = String(op.text ?? '').trim();
  if (!text) throw new OpError(400, 'text required');
  if (ctx.caller?.kind === 'pod' && !isCoordinator(ctx)) {
    throw new OpError(403, `pod token ${ctx.caller.role}: broadcast is an operator/pm action`);
  }
  const from = ctx.caller?.kind === 'pod' ? ctx.caller.role : (op.from !== undefined ? String(op.from) : 'operator');
  const wanted = op.roles !== undefined ? (Array.isArray(op.roles) ? op.roles.map(String) : String(op.roles).split(/[\s,]+/).filter(Boolean)) : null;
  let roles: string[];
  if (wanted) {
    for (const r of wanted) if (!store.getPodByRole(ctx.store, r)) throw new OpError(404, `no pod: ${r} (spawn it first)`);
    roles = [...new Set(wanted)];
  } else {
    roles = store.listPods(ctx.store).map((p) => p.role);
  }
  if (!roles.length) throw new OpError(404, 'no pods to broadcast to (spawn at least one)');
  const per: Record<string, boolean | 'target-not-live' | 'failed'> = {};
  const ids: number[] = [];
  for (const role of roles) {
    const { inboxId } = store.insertInboxMessage(ctx.store, { from, to: role, text });
    ids.push(inboxId);
    const pod = store.getPodByRole(ctx.store, role)!;
    if (pod.state !== 'live') { per[role] = 'target-not-live'; continue; }
    try {
      await apply({ type: 'pod_send', role, text: `[flock-broadcast from ${from} to ${roles.length} pods]\n${text}` }, ctx);
      per[role] = true;
    } catch { per[role] = 'failed'; }
  }
  ctx.emit?.({ type: 'message_broadcast', from, to: roles.join(','), count: roles.length, ids });
  return { ok: true, from, count: roles.length, inboxIds: ids, poked: per };
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
  // C14: a team NAME resolves to ~/.flock/teams/<name>.yaml; a path works
  // as before (the legacy `team up <file>` keeps working).
  const raw = String(op.file ?? 'pods.yaml');
  let file = path.resolve(raw);
  if (!raw.includes('/') && !fs.existsSync(file)) {
    const { file: named } = readTeamSpec(ctx.store.home, raw);
    file = named;
  }
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
  const results = await reconcileTeam(ctx, spec);
  ctx.emit?.({ type: 'team_up', file, pods: Object.keys(spec.pods) });
  return { file, results };
}

// Shared reconcile: live pods refreshed (guidance re-merged, idempotent),
// missing/closed pods spawned. Never kills a live pod. Used by both team_up
// (file) and topology_up (named preset) — one team path, no second contour.
async function reconcileTeam(ctx: CoreCtx, spec: { pods: Record<string, { agent?: string; model?: string; profile?: string; posture?: 'floor' | 'full_bypass' | string; guidance?: string; dir?: string }> }): Promise<Record<string, unknown>> {
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
        // C12: `dir: ws:<name>` resolves through the profile's workspace
        dir: s.dir ? (s.dir.startsWith('ws:') ? resolveWorkspaceRef(ctx.store.home, s.dir) : s.dir) : undefined,
      }, ctx)) as { run?: { id: string } };
      results[role] = { action: existing ? 'respawned' : 'spawned', run: res?.run?.id };
    } catch (e) {
      results[role] = { action: 'failed', error: e instanceof Error ? e.message : String(e) };
    }
  }
  return results;
}

// topology_up: a named preset from the catalog, reconciled through the team
// path. `dir` (optional) is applied to every pod that does not set its own.
async function topologyUp(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const name = String(op.name ?? '');
  let spec;
  try {
    spec = topologySpec(name);
  } catch (e) {
    throw new OpError(400, e instanceof Error ? e.message : String(e));
  }
  const dir = op.dir != null ? String(op.dir) : undefined;
  if (dir) for (const s of Object.values(spec.pods)) if (!s.dir) s.dir = dir;
  const results = await reconcileTeam(ctx, spec);
  ctx.emit?.({ type: 'topology_up', name, pods: Object.keys(spec.pods) });
  return { topology: name, yaml: renderTopologyYaml(name), results };
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
  store.insertWorkflowInstance(ctx.store, { id: instId, workflowId: wf.id, payload: op.payload ? String(op.payload) : null, });
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
