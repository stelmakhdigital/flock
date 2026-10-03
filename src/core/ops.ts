import fs from 'node:fs';
import path from 'node:path';
import { execFile, execFileSync } from 'node:child_process';
import { promisify } from 'node:util';
import * as store from './store.js';
import * as gitops from './gitops.js';
import { parseTeamYaml, TeamParseError } from './team.js';
import { listConflictResolutions } from './store.js';
import * as terminal from './terminal.js';
import { resolveAgent, loadAgents, firstUserModel, manifestRuntime, podRuntime } from './agent.js';
import { startPodSocket, stopPodSocket } from './http.js';
import { seatPaths, frameMessage, newNonce, validateResumeToken } from './runner-protocol.js';
import { claudeConfigDir, claudeProjectsDir, claudeTranscriptFp, waitForTranscriptGrowth, validateClaudeSessionToken, latestClaudeSession } from './claude-protocol.js';
import { getAdapter, mergeManagedBlock, pruneManagedBlocks, type PodBinding, type StartupFile } from './runtime-adapter.js';
import { WATCHDOG_POLICIES, validateSpec, terminateJob, type PolicyName } from './watchdog.js';
import { listAlerts } from './health.js';
import { validateIntent, applyIntents, pmDigest, pmNotify } from './pm.js';
import { withMergeLock, mergeQueueSize } from './merge-queue.js';
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
  return d.run(o, ctx);
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
  pod_spawn: { group: 'pod', scopes: ['operator'], summary: 'spawn a pod (agent manifest, optional --repo worktree)', run: (o, c) => podSpawn(o, c) },
  pod_relaunch: { group: 'pod', scopes: ['operator'], summary: 'new run on the same pod (honest resume, --fork)', run: (o, c) => podRelaunch(o, c) },
  pod_set_resume_token: { group: 'pod', scopes: ['operator'], summary: 'pin/reset the session used for resume', run: (o, c) => podSetResumeToken(o, c) },
  team_up: { group: 'team', scopes: ['operator'], summary: 'reconcile a pods.yaml team (spawn missing, refresh live)', run: (o, c) => teamUp(o, c) },
  resolver_ls: { group: 'team', scopes: ['operator'], summary: 'S5 conflict-resolution chains (opt-in FLOCK_RESOLVER_AGENT)', run: (o, c) => listConflictResolutions(c.store) },
  pod_merge_status: { group: 'pod', scopes: ['operator', 'pod'], summary: 'worktree pod: ahead/behind/dirty vs base', run: (o, c) => podMergeStatus(o, c) },
  pod_send: { group: 'pod', scopes: ['operator'], summary: 'send text to a live pod (transport, verified)', run: (o, c) => podSend(o, c) },
  pod_answer: { group: 'pod', scopes: ['operator'], summary: 'answer a pending dialog (gate) in a pod', run: (o, c) => podAnswer(o, c) },
  pod_capture: { group: 'pod', scopes: ['operator'], summary: 'capture pod pane text', run: (o, c) => podCapture(o, c) },
  pod_close: { group: 'pod', scopes: ['operator', 'pod'], summary: 'close a pod (kill window, state=closed; --purge drops a worktree)', run: (o, c) => podClose(o, c) },
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
  task_done: { group: 'task', scopes: ['operator', 'pod'], summary: 'report task done (pod-scoped: own pod only)', run: async (o, c) => { await pmNotifyMaybe(c, 'task_done', o, 'done'); return taskReport(o, c, 'done'); } },
  task_blocked: { group: 'task', scopes: ['operator', 'pod'], summary: 'report task blocked (pod-scoped: own pod only)', run: async (o, c) => { await pmNotifyMaybe(c, 'task_blocked', o, 'blocked'); return taskReport(o, c, 'blocked'); } },
  task_needs: { group: 'task', scopes: ['operator', 'pod'], summary: 'report task needs help (pod-scoped: own pod only)', run: async (o, c) => { await pmNotifyMaybe(c, 'task_needs', o, 'needs'); return taskReport(o, c, 'needs'); } },
  // -- pm / goal loop ---------------------------------------------------------
  pm_up: { group: 'pm', scopes: ['operator'], summary: 'start the pm pod (goal loop)', run: (_o, c) => pmUp(c) },
  pm_state: { group: 'pm', scopes: ['operator'], summary: 'pm digest snapshot + alerts', run: (_o, c) => ({ pm: pmDigest(c), alerts: listAlerts(c) }) },
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
  profile?: string | null;
  freshStart?: boolean;
  repo?: string | null;
  repoBase?: string | null;
  branch?: string | null;
  teamGuidance?: string; // team file: extra AGENTS.md block (managed, id team:<role>)
  mergePolicy?: string | null; // 5.4b S1: ff | squash | never (pod-level override of the manifest default)
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

    // Worktree pod (pi): two things the sandbox/guard combo cannot do:
    //  1) git — the main repo sits outside the bwrap-visible workspace
    //     (/home is masked), so the worktree's .git link dangles in-sandbox;
    //     2) bash-guard — its interactive prompt is a no-op in RPC mode.
    // Trust boundary for a worktree pod = git itself: the S0 merge gate is
    // fast-forward-only, unmerged work stays a branch, and bash-guard's
    // autonomous floor still blocks rm -rf / reset --hard / push --force.
    const childEnv: Record<string, string> = { ...(manifest.env ?? {}) };
    if (o.repo && adapter.runtime === 'pi') childEnv['BASH_GUARD_AUTO_ALLOW'] = '1';

    if (adapter.runtime === 'pi' && !model) model = firstUserModel();
    if (adapter.runtime === 'claude' && !model) {
      // pi config uses provider/model syntax; claude wants the bare model id
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
      // the base repo cannot be made visible inside the sandbox -> no bwrap
      trustLevel: o.repo && adapter.runtime === 'pi' ? 'off' : manifest.trustLevel,
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
    repo: o.repo ?? null,
    repoBase: o.repoBase ?? null,
    branch: o.branch ?? null,
    mergePolicy: o.mergePolicy ?? null,
  });
  const run = store.insertRun(ctx.store, { id: store.newId('run'), podRole: o.role, pid, meta: runMeta });
  startPodSocket(ctx, o.dir, o.role); // pod-local API socket (sandbox-visible)
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
  // worktree pod: --repo <path> [--base <ref>] — the pod works in its own
  // checkout on branch flock/<role>; task done = merge candidate (S0 ff)
  let dir = String(op.dir ?? path.join(ctx.store.home, 'pods', role));
  let repo: string | null = null;
  let repoBase: string | null = null;
  let branch: string | null = null;
  let branchStart: string | null = null;
  if (op.repo !== undefined) {
    const repoPath = path.resolve(String(op.repo));
    if (!gitops.isGitRepo(repoPath)) throw new OpError(400, `--repo: not a git repo: ${repoPath}`);
    repo = repoPath;
    repoBase = op.base ? String(op.base) : (await gitops.currentBranch(repoPath)) ?? 'HEAD';
    branch = gitops.branchName(role);
    if (op.branch) {
      // S5: explicit branch override (the conflict resolver works on the
      // origin pod's branch, not flock/<resolver-role>)
      const b = String(op.branch);
      if (!/^[A-Za-z0-9._/-]{1,60}$/.test(b)) throw new OpError(400, `bad branch: ${b}`);
      branch = b;
      if (op.branchStart) branchStart = String(op.branchStart);
    }
    dir = path.join(ctx.store.home, 'pods', role, 'work');
  } else if (existing?.repo && existing.repo_base && existing.branch) {
    // re-spawn of a previously closed worktree pod: same layout
    repo = existing.repo;
    repoBase = existing.repo_base;
    branch = existing.branch;
    if (op.dir === undefined) dir = path.join(ctx.store.home, 'pods', role, 'work');
  }
  if (repo) {
    try {
      await gitops.worktreeAttach(repo, dir, branch!, repoBase!, branchStart ?? undefined);
    } catch (e) {
      throw new OpError(500, `worktree attach failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  const { pod, run } = await spawnAgent(ctx, {
    role,
    dir,
    model: op.model ? String(op.model) : undefined,
    agentId: op.agent ? String(op.agent) : undefined,
    rawCmd: op.cmd ? String(op.cmd) : undefined,
    forkRef: op.fork ? resolveForkRef(ctx, String(op.fork)) : undefined,
    posture: op.posture === 'full_bypass' || op.posture === 'floor' ? op.posture : undefined,
    profile: op.profile ? String(op.profile) : null,
    repo,
    repoBase,
    branch,
    // 5.4b S1: merge policy — explicit flag wins, else the agent manifest
    // default, else null (= ff, the S0 behavior)
    mergePolicy: podMergePolicyFrom(op, op.agent ? String(op.agent) : undefined, op.profile ? String(op.profile) : undefined),
  });
  return { pod, run, repo: repo ?? undefined, branch: branch ?? undefined };
}

// 5.4b S1: resolve the merge policy for a new worktree pod: explicit
// --merge flag > agent manifest default > null (= ff).
function podMergePolicyFrom(op: Record<string, unknown>, agentId: string | undefined, profile: string | undefined): string | null {
  if (op.merge !== undefined) {
    const m = String(op.merge);
    if (!['ff', 'squash', 'never'].includes(m)) throw new OpError(400, `--merge: invalid policy ${m} (want ff | squash | never)`);
    return m;
  }
  if (agentId) {
    const r = resolveAgent(agentId, null, profile);
    if (r?.manifest.merge) return r.manifest.merge;
  }
  return null;
}

// relaunch: agent dies (or operator wants a fresh one) -> new run on the same
// pod. pi (v3.1): HONEST resume — the exact persisted session file is
// relaunched (--session <file>), never an interactive picker; a missing file
// is retry_fresh (recorded in the run meta, never silent). bash: fresh window.
const adapterEnv = (ctx: CoreCtx) => ({ home: ctx.store.home, token: ctx.store.token, runnerPath: path.join(import.meta.dirname, 'runner.js') });

export async function podRelaunch(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod) throw new OpError(404, `no pod: ${role}`);
  // A stale relaunch may have wiped the repo fields (pre-fix rows): the
  // worktree on disk is the source of truth — recover the binding from it.
  if (!pod.repo && !pod.branch && fs.existsSync(path.join(pod.dir, '.git'))) {
    const link = fs.readFileSync(path.join(pod.dir, '.git'), 'utf8').trim();
    const m = link.match(/^gitdir:\s*(.+)$/);
    if (m) {
      const wm = m[1].match(/^(.*)\.git\/(worktrees\/.+)$/);
      if (wm) {
        const repo = wm[1];
        const headBranch = (await gitops.git(pod.dir, 'branch', '--show-current').catch(() => '')) || null;
        store.setPodRepo(ctx.store, role, repo, null, headBranch);
      }
    }
  }
  const cur = store.getPodByRole(ctx.store, role);
  if (!cur) throw new OpError(404, `no pod: ${role}`);
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
  // pinned resume token wins; otherwise the latest session (honest resume).
  // Token shape is per-runtime: pi = session file path, claude = transcript uuid.
  const pinned = isPi || isClaude ? (cur.resume_token ?? undefined) : undefined;
  const tokenValid = isPi ? validateResumeToken(pinned!) : validateClaudeSessionToken(pinned!);
  if (pinned && !tokenValid) {
    throw new OpError(400, `pinned resume token is invalid: ${pinned} (flock pod resume-token ${role} reset)`);
  }
  let resumeToken: string | undefined;
  const forkRef = op.fork !== undefined ? resolveForkRef(ctx, String(op.fork)) : undefined;
  if (isPi) {
    resumeToken = forkRef ? undefined : pinned ?? store.latestSessionFile(ctx.store, role) ?? undefined;
  } else if (isClaude) {
    const adapter = getAdapter(resolved!.manifest, adapterEnv(ctx));
    resumeToken = forkRef ? undefined : pinned ?? (await adapter?.latestSessionToken?.({ role, cwd: cur.dir })) ?? undefined;
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
    // worktree pod: keep the repo/branch binding across relaunches (S0 merge
    // + bash-guard autonomy depend on these)
    repo: cur.repo,
    repoBase: cur.repo_base,
    branch: cur.branch,
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
  const raw = op.token === undefined || op.token === '' || op.token === 'reset' ? null : String(op.token);
  if (raw !== null && !validateResumeToken(raw)) {
    throw new OpError(400, `invalid resume token: ${raw}`);
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
  if (podRuntime(pod.agent) === 'claude') {
    // claude TUI: raw paste; the per-pod transcript is the typed ack (it grows
    // when the prompt lands, independent of turn duration).
    const projectsDir = claudeProjectsDir(claudeConfigDir(path.join(ctx.store.home, 'pods', pod.role)), pod.dir);
    const before = claudeTranscriptFp(projectsDir);
    const res = await terminal.send(pod.terminal_target!, text, { raw: true });
    const { grown } = await waitForTranscriptGrowth(projectsDir, before, 30000);
    if (!grown) throw new OpError(504, 'claude transcript did not grow after send (check the pod pane)');
    const run = store.currentRun(ctx.store, role);
    if (run && !run.ended_at) {
      store.appendRunMeta(ctx.store, run.id, {
        kind: 'sent',
        bytes: Buffer.byteLength(text),
        attempts: res.attempts,
        ack: 'transcript',
      });
    }
    return { ok: true, attempts: res.attempts, ack: 'transcript' };
  }
  if (podRuntime(pod.agent) === 'pi') {
    // runner bridge: framed message + raw paste, verified by the sidecar ack
    // (the visual probe breaks on long/wrapped lines in a TTY line editor).
    const nonce = newNonce();
    const wire = frameMessage(text, nonce);
    const res = await terminal.send(pod.terminal_target!, wire, { raw: true });
    const deadline = Date.now() + 5000;
    let acked = false;
    while (Date.now() < deadline) {
      const st = terminal.readRunnerState(ctx.store.home, pod.role);
      if (st?.lastPrompt?.nonce === nonce) {
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
  // --purge: remove the worktree (the BRANCH survives — it is the merge
  // candidate); default keeps the checkout as the pod's memory
  let purged: string | null = null;
  if (op.purge === true && pod.repo && pod.branch) {
    try {
      await gitops.worktreeRemove(pod.repo, pod.dir);
      purged = pod.dir;
    } catch (e) {
      throw new OpError(500, `worktree purge failed: ${e instanceof Error ? e.message : String(e)}`);
    }
  }
  ctx.emit?.({ type: 'pod_closed', role });
  return { ok: true, purged };
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
    priority: op.priority != null ? Math.max(0, Math.min(10, Math.trunc(Number(op.priority)))) : 0,
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
  // arbiter/CLI races must not 409 or re-run the merge gate)
  if (task.status === to) return task;
  // terminal tasks never re-open and never re-merge (a second `task done`
  // after the pod self-reported would double-squash the branch)
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
  // S1+S2 (5.4b): a worktree pod's done task is a merge candidate that goes
  // through the merge GATE (policy ff|squash|never, merge-tree dry-run,
  // quality gate). Conflict or gate-red: the work is done, but integration
  // failed -> the task is BLOCKED (merge conflict / tests failed) instead of
  // done; the branch + worktree survive for the operator (or S5 resolver).
  let mergeNote: string | null = null;
  let gateBlock: { reason: string; result: string } | null = null;
  if (to === 'done') {
    const outcome = await mergeWorktreeIfEligible(ctx, task.pod_role, task);
    mergeNote = outcome.note;
    if (outcome.conflict) {
      gateBlock = { reason: `merge conflict (S1)`, result: outcome.conflict.slice(0, 400) };
    } else if (outcome.gateFail) {
      gateBlock = { reason: `quality gate (S2): ${outcome.gateFail}`, result: outcome.gateFail };
    }
  }
  const finalTo = to === 'done' && gateBlock ? 'blocked' : to;
  const reason =
    gateBlock ? gateBlock.reason :
    (finalTo === 'blocked' || finalTo === 'needs') && op.reason ? String(op.reason).slice(0, 200) :
    finalTo === 'cancelled' ? 'cancelled' : `reported by ${by}`;
  // done: optional result text (carried into the next workflow step);
  // blocked-by-gate: the gate's detail goes into result (visible in UI/CLI)
  const result = gateBlock ? gateBlock.result :
    finalTo === 'blocked' || finalTo === 'needs' ? reason :
    finalTo === 'done' && op.result ? String(op.result).slice(0, 400) : null;
  const finalReason = mergeNote ? `${reason}${reason ? ' · ' : ''}${mergeNote}` : reason;
  try {
    store.setTaskStatus(ctx.store, id, finalTo, { reason: finalReason, result });
  } catch (e) {
    throw new OpError(409, e instanceof Error ? e.message : String(e));
  }
  ctx.emit?.({ type: `task_${finalTo}`, taskId: id, pod: task.pod_role, reason: finalReason, gate: gateBlock ? true : undefined });
  // S5: a merge conflict starts the resolver chain (opt-in via
  // FLOCK_RESOLVER_AGENT) — the task stays blocked in the meantime
  if (finalTo === 'blocked' && gateBlock?.reason.startsWith('merge conflict')) {
    void maybeStartResolver(ctx, task, gateBlock.result).catch((e) =>
      ctx.emit?.({ type: 'resolver_error', originTaskId: id, message: String(e instanceof Error ? e.message : e) }),
    );
  }
  advanceWorkflow(ctx, id);
  return store.getTask(ctx.store, id);
}

// S0: try an auto fast-forward merge for a worktree pod's finished task.
// Returns a short audit note (null = not a worktree pod / no merge action).
// Never throws: a merge problem must not fail the task report.
// S1+S2 (5.4b): extended into a merge GATE:
//   policy (pod.merge_policy, default ff): never -> skip (manual);
//   ff (S0): clean + ahead + not-behind -> ff-only merge;
//   squash: merge-tree dry-run FIRST (conflict -> task blocked (merge
//     conflict) with the CONFLICT summary — zero half-states), then the
//   quality gate (S2), then one flock(<task>): commit on base.
// S2 quality gate: when the workflow instance requires the test gate, the
// manifest's testCmd runs in the worktree BEFORE any merge: green -> merge,
// red -> task blocked (tests failed) + the log tail lands in run.meta.
// The merge is by GATE, not by agent self-assessment.
interface MergeOutcome {
  note: string | null; // audit note for the transition reason
  conflict?: string; // merge conflict -> task becomes blocked (S1)
  gateFail?: string; // quality gate red / timeout -> task blocked (S2)
}

// S2: run the manifest's testCmd in the worktree. Pure + hermetic.
export async function runQualityGate(
  worktree: string,
  cmd: string,
  timeoutMs = 600_000,
): Promise<{ ok: boolean; code: number | null; ms: number; tail: string; timedOut: boolean }> {
  const t0 = Date.now();
  const p = new Promise<{ code: number | null; out: string; timedOut: boolean }>((res) => {
    execFile('bash', ['-c', cmd], { cwd: worktree, timeout: timeoutMs, maxBuffer: 1_000_000 }, (err, stdout, stderr) => {
      const timedOut = (err as { killed?: boolean } | null)?.killed === true;
      res({ code: err ? ((err as NodeJS.ErrnoException).code === 'ETIMEDOUT' ? null : (err as { code?: number }).code ?? 1) : 0, out: `${stdout}\n${stderr}`.trim(), timedOut });
    });
  });
  const r = await p;
  const ms = Date.now() - t0;
  return { ok: r.code === 0, code: r.code, ms, tail: r.out.slice(-4000), timedOut: r.timedOut };
}

async function mergeWorktreeIfEligible(ctx: CoreCtx, role: string, task: store.Task, skipDefer = false): Promise<MergeOutcome> {
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod?.repo || !pod.branch) return { note: null };
  const policy = pod.merge_policy ?? 'ff';
  if (policy === 'never') return { note: 'merge policy: never (branch stays a manual merge candidate)' };
  let base = pod.repo_base;
  if (!base) {
    // no recorded base (recovered row): the base repo's checked-out branch
    // is the obvious target — record it and proceed
    base = await gitops.currentBranch(pod.repo).catch(() => null);
    if (base && base !== 'HEAD') store.setPodRepo(ctx.store, role, pod.repo, base, pod.branch);
  }
  if (!base || base === 'HEAD') return { note: 'merge skipped (no base branch recorded)' };
  // local consts: property narrowing does not survive into the merge-lock
  // closures below
  const repo = pod.repo;
  const branch = pod.branch;
  const wt = pod.dir; // worktree pod: pod.dir IS the worktree checkout
  try {
    if (!gitops.isGitWorkdir(wt)) return { note: 'merge skipped (worktree missing)' };
    // S3 (review gate): if this step is followed by more steps (e.g. review),
    // the merge is DEFERRED — it runs when the instance finishes (after the
    // review verdict). The branch stays a merge candidate in the meantime.
    if (task.workflow_instance_id && task.workflow_step && !skipDefer) {
      const inst = store.getWorkflowInstance(ctx.store, task.workflow_instance_id);
      const wf = inst ? store.getWorkflow(ctx.store, inst.workflow_id) : null;
      if (inst && wf) {
        const steps = (JSON.parse(wf.spec) as { steps: WfStep[] }).steps;
        const idx = steps.findIndex((s) => s.id === task.workflow_step);
        if (idx >= 0 && idx < steps.length - 1) {
          return { note: `merge deferred (step ${steps[idx + 1].id} follows — S3 review gate)` };
        }
      }
    }
    const st = await gitops.worktreeStatus(pod.repo, wt, pod.branch, base);
    if (st.ahead === 0) return { note: 'merge skipped (no commits ahead of base)' };
    if (st.dirty) return { note: `merge skipped (worktree dirty: commit first) — ${st.ahead} commit(s) unmerged` };
    // already-merged guard: the branch is ahead (its wip commits survive the
    // squash) but the DIFF vs base is empty — a re-report must not double-
    // squash an empty commit onto main
    const noDiff = await gitops.gitRaw(pod.repo, 'diff', '--quiet', base, pod.branch).then((r) => r.code === 0);
    if (noDiff) return { note: 'merge skipped (no diff vs base — already merged)' };
    if (policy === 'ff') {
      if (st.behind > 0) return { note: `merge skipped (base moved: ${st.behind} behind) — rebase/merge by hand` };
      // S4: the mutating git runs under the merge queue (FIFO) — with >=2
      // worktree pods the base repo is re-verified at claim time, not at
      // request time (a base moved while waiting fails cleanly as non-ff)
      const waiters = mergeQueueSize();
      ctx.emit?.({ type: 'merge_queued', role, policy, queue: waiters });
      const r = await withMergeLock(() => gitops.ffMerge(repo, branch, base));
      ctx.emit?.({ type: 'merge_done', role, policy, ok: r.ok });
      return { note: r.ok ? `auto-merged (ff, ${st.ahead} commit(s))` : `merge skipped (${r.error})` };
    }
    // policy === 'squash': S1 dry-run BEFORE anything (conflict is cheap to
    // detect and must not pay for a test run)
    const dry = await gitops.mergeTreeCheck(pod.repo, base, pod.branch);
    if (!dry.clean) {
      return { note: null, conflict: `merge conflict on ${base}: ${dry.info}` };
    }
    // S2: quality gate (only when the workflow instance requires it)
    const inst = task.workflow_instance_id ? store.getWorkflowInstance(ctx.store, task.workflow_instance_id) : null;
    if (inst?.require_test) {
      const manifest = pod.agent ? resolveAgent(pod.agent, null, pod.profile ?? undefined)?.manifest : undefined;
      if (!manifest?.testCmd) {
        // config gap: the gate is required but no command is defined — do
        // NOT merge silently; flag it in the audit note, merge proceeds
        // (the operator sees it in the transition reason)
        ctx.emit?.({ type: 'quality_gate', role, ok: null, note: 'no testCmd in manifest (gate skipped)' });
      } else {
        const timeoutMs = Number(process.env.FLOCK_TEST_TIMEOUT_S ?? 600) * 1000;
        const g = await runQualityGate(wt, manifest.testCmd, timeoutMs);
        const run = store.currentRun(ctx.store, role);
        store.appendRunMeta(ctx.store, run?.id ?? '', { kind: 'quality_gate', cmd: manifest.testCmd, ok: g.ok, code: g.code, ms: g.ms, tail: g.tail.slice(-2000) });
        ctx.emit?.({ type: 'quality_gate', role, ok: g.ok, code: g.code, ms: g.ms });
        if (!g.ok) {
          return { note: null, gateFail: g.timedOut ? `tests timed out (${Math.round(timeoutMs / 1000)}s)` : `tests failed (exit ${g.code})` };
        }
      }
    }
    // gate green (or not required) -> one squash commit on base (S4: under
    // the merge queue — the squash re-checks the base tree at claim time)
    const msg = `flock(${task.id}): ${task.title}`.slice(0, 200);
    ctx.emit?.({ type: 'merge_queued', role, policy: 'squash', queue: mergeQueueSize() });
    const r = await withMergeLock(() => gitops.squashMerge(repo, branch, base, msg, wt));
    ctx.emit?.({ type: 'merge_done', role, policy: 'squash', ok: r.ok });
    if (r.ok) {
      return { note: `squash-merged (1 commit on ${base}${inst?.require_test ? ', tests green' : ''}, ${st.ahead} commit(s) collapsed, branch advanced)` };
    }
    // the dry-run said clean but the real merge failed (race: base moved
    // between the check and the merge) — same blocked semantics
    return { note: null, conflict: `squash merge failed on ${base}: ${r.error}` };
  } catch (e) {
    return { note: `merge error: ${e instanceof Error ? e.message : String(e)}` };
  }
}

// merge-status: ahead/behind/dirty of a worktree pod vs its base branch
async function podMergeStatus(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod?.repo || !pod.branch || !pod.repo_base) throw new OpError(400, `pod ${role} is not a worktree pod (spawn with --repo)`);
  const wt = pod.dir; // worktree pod: pod.dir IS the worktree checkout
  if (!gitops.isGitWorkdir(wt)) throw new OpError(409, 'worktree missing on disk');
  const st = await gitops.worktreeStatus(pod.repo, wt, pod.branch, pod.repo_base);
  return { role, repo: pod.repo, base: pod.repo_base, branch: pod.branch, ...st };
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
        repo: s.repo,
        base: s.base,
        posture: s.posture,
        merge: s.merge,
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

// ---------- S5: conflict resolver ----------
// Opt-in by actual pain: FLOCK_RESOLVER_AGENT (e.g. 'pi') enables it, the
// hard attempt limit is FLOCK_RESOLVER_MAX_ATTEMPTS (default 2). When a merge
// conflict blocks a task, a dedicated LLM pod works on a <branch>-resolve
// fork (the origin branch is checked out in the origin worktree, so the
// resolver cannot share it), merges base into the fork and resolves. Core
// then ff-applies the fork onto the origin branch and re-runs the merge gate.
// Exhausted attempts -> the task stays blocked for the operator (no
// unbounded money-burning loop).

function resolverOpts(): { agent: string; maxAttempts: number } {
  return {
    agent: process.env.FLOCK_RESOLVER_AGENT ?? '',
    maxAttempts: Math.max(1, Math.min(5, Math.trunc(Number(process.env.FLOCK_RESOLVER_MAX_ATTEMPTS ?? 2)))),
  };
}

function resolverTaskBody(pod: store.Pod, conflictInfo: string): string {
  const base = pod.repo_base ?? 'base';
  const resolveBranch = `${pod.branch}-resolve`;
  return [
    `Конфликт-резолв: ветка ${pod.branch} не смержилась с ${base} (S1 dry-run):`,
    conflictInfo,
    '',
    `Твоя ветка: ${resolveBranch} (форк ${pod.branch}). Инструкции:`,
    `1. git merge ${base}   (конфликт проявится в этой worktree)`,
    '2. Разрешай конфликты СОБЛИТЕЛЬНО: сохрани И работу ветки, И новые изменения base (не выбрасывай стороны!).',
    '   Если сторона ветки — дубль/отладка и не несёт нового смысла, допустимо взять версию base (обоснуй в комментарии к решению).',
    '   add/add конфликты: сравни содержимое обеих версий, объедини осмысленно.',
    '3. git add . && git commit -m "resolve: merge base into branch"',
    '4. Когда готово: flock task done <id>. Если не можешь разрешить честно — flock task blocked <id> \'<почему>\'.',
    'Не трогай ветку base, не делай force-push, не удаляй файлы без причины.',
  ].join('\n');
}

// Spawn (if needed) the resolver pod + first resolver task for a conflicted
// origin task. Called from the merge-conflict block path of taskReport.
export async function maybeStartResolver(ctx: CoreCtx, task: store.Task, conflictInfo: string): Promise<void> {
  const opts = resolverOpts();
  if (!opts.agent) return; // disabled
  const pod = store.getPodByRole(ctx.store, task.pod_role);
  if (!pod?.repo || !pod.branch) return;
  if (store.getConflictResolutionByTask(ctx.store, task.id)) return; // one chain per task
  const resolverRole = `resolver-${task.pod_role}`.slice(0, 20);
  const resolveBranch = `${pod.branch}-resolve`;
  const r = store.getPodByRole(ctx.store, resolverRole);
  if (!r || r.state === 'closed') {
    try {
      await apply(
        {
          type: 'pod_spawn',
          role: resolverRole,
          agent: opts.agent,
          repo: pod.repo,
          base: pod.repo_base ?? undefined,
          branch: resolveBranch,
          branchStart: pod.branch, // fork from the origin branch, not the base
          merge: 'never', // the resolver never auto-merges; core applies the result
        },
        ctx,
      );
    } catch (e) {
      ctx.emit?.({ type: 'resolver_error', originTaskId: task.id, message: `resolver spawn failed: ${e instanceof Error ? e.message : String(e)}` });
      return;
    }
  }
  const resolverTaskId = store.newId('t');
  store.insertTask(ctx.store, {
    id: resolverTaskId,
    title: `[S5] resolve conflict ${pod.branch} vs ${pod.repo_base ?? 'base'}`,
    body: resolverTaskBody(pod, conflictInfo),
    podRole: resolverRole,
  });
  const cr = store.newId('cr');
  store.insertConflictResolution(ctx.store, { id: cr, originTaskId: task.id, originRole: task.pod_role, resolverRole, resolverTaskId });
  ctx.emit?.({ type: 'resolver_started', originTaskId: task.id, resolverRole, resolverTaskId });
}

// Apply the resolver's fork onto the origin branch (inside the origin
// worktree — it owns the branch): clean tree required, ff-only.
async function applyResolutionToOrigin(pod: store.Pod): Promise<boolean> {
  const resolveBranch = `${pod.branch}-resolve`;
  try {
    const dirty = await gitops.git(pod.dir, 'status', '--porcelain', '--untracked-files=no');
    if (dirty) return false; // the origin pod is still working — retry next tick
    const r = await gitops.gitRaw(pod.dir, 'merge', '--ff-only', resolveBranch);
    return r.code === 0;
  } catch {
    return false;
  }
}

// 30s tick: advance running resolution chains (resolver task finished ->
// apply + re-run the merge gate; failed resolver -> next attempt or exhaust).
export async function tickConflictResolvers(ctx: CoreCtx): Promise<void> {
  const opts = resolverOpts();
  for (const res of store.listConflictResolutions(ctx.store, 'running')) {
    const resolverTask = res.resolver_task_id ? store.getTask(ctx.store, res.resolver_task_id) : null;
    if (!resolverTask) {
      store.setConflictResolution(ctx.store, res.id, { status: 'failed' });
      continue;
    }
    if (resolverTask.status !== 'done' && resolverTask.status !== 'blocked' && resolverTask.status !== 'cancelled') continue;
    if (resolverTask.status === 'done') {
      const pod = store.getPodByRole(ctx.store, res.origin_role);
      if (!pod?.repo || !pod.branch) {
        store.setConflictResolution(ctx.store, res.id, { status: 'failed' });
        continue;
      }
      if (!(await applyResolutionToOrigin(pod))) {
        // origin worktree busy/dirty/diverged — retry, but not forever
        if (res.apply_attempts + 1 >= 20) {
          store.setConflictResolution(ctx.store, res.id, { status: 'failed' });
          ctx.emit?.({ type: 'resolver_failed', originTaskId: res.origin_task_id, reason: 'cannot apply resolution to origin worktree (busy?) after 20 tries' });
        } else {
          store.setConflictResolution(ctx.store, res.id, { applyAttempts: res.apply_attempts + 1 });
        }
        continue;
      }
      const originTask = store.getTask(ctx.store, res.origin_task_id);
      if (!originTask || originTask.status !== 'blocked') {
        store.setConflictResolution(ctx.store, res.id, { status: originTask?.status === 'done' ? 'resolved' : 'failed' });
        continue;
      }
      const outcome = await mergeWorktreeIfEligible(ctx, res.origin_role, originTask, true);
      // 'no diff vs base' after applying the resolution = the resolution was
      // fully absorbed (e.g. the resolver adopted the base version of the
      // only conflicted file) — a successful resolution, not a failure
      const noDiffLeft = /no diff vs base/.test(outcome.note ?? '');
      const merged = (outcome.note && !/skipped|error/i.test(outcome.note)) || noDiffLeft;
      if (merged) {
        store.setTaskStatus(ctx.store, originTask.id, 'done', { reason: `S5 resolver: conflict resolved in ${res.attempts} attempt(s)${noDiffLeft ? ' (resolution absorbed into base, no remaining diff)' : ` · ${outcome.note}`}`, result: null });
        store.setConflictResolution(ctx.store, res.id, { status: 'resolved' });
        ctx.emit?.({ type: 'conflict_resolved', originTaskId: originTask.id, note: outcome.note, attempts: res.attempts });
        advanceWorkflow(ctx, originTask.id);
        continue;
      }
      if (outcome.conflict && res.attempts < opts.maxAttempts) {
        // the resolution did not fix it — another attempt (fresh task, same pod)
        const podNow = store.getPodByRole(ctx.store, res.origin_role)!;
        const t = store.newId('t');
        store.insertTask(ctx.store, { id: t, title: `[S5] resolve conflict (attempt ${res.attempts + 1}) ${podNow.branch}`, body: resolverTaskBody(podNow, outcome.conflict), podRole: res.resolver_role });
        store.setConflictResolution(ctx.store, res.id, { attempts: res.attempts + 1, resolverTaskId: t });
        ctx.emit?.({ type: 'resolver_retry', originTaskId: res.origin_task_id, attempt: res.attempts + 1 });
        continue;
      }
      store.setConflictResolution(ctx.store, res.id, { status: outcome.conflict ? 'exhausted' : 'failed' });
      ctx.emit?.({ type: 'resolver_exhausted', originTaskId: res.origin_task_id, attempts: res.attempts, detail: outcome.conflict ?? outcome.note ?? 'merge still failing' });
      continue;
    }
    // resolver task failed (blocked/cancelled): next attempt or exhaust
    if (res.attempts < opts.maxAttempts) {
      const pod = store.getPodByRole(ctx.store, res.origin_role);
      const t = store.newId('t');
      store.insertTask(ctx.store, { id: t, title: `[S5] resolve conflict (attempt ${res.attempts + 1}) ${pod?.branch ?? res.origin_role}`, body: pod ? resolverTaskBody(pod, `previous attempt: ${resolverTask.result ?? resolverTask.status}`) : 'resolver pod unavailable', podRole: res.resolver_role });
      store.setConflictResolution(ctx.store, res.id, { attempts: res.attempts + 1, resolverTaskId: t });
      ctx.emit?.({ type: 'resolver_retry', originTaskId: res.origin_task_id, attempt: res.attempts + 1, reason: resolverTask.result ?? resolverTask.status });
    } else {
      store.setConflictResolution(ctx.store, res.id, { status: 'exhausted' });
      ctx.emit?.({ type: 'resolver_exhausted', originTaskId: res.origin_task_id, attempts: res.attempts, detail: `resolver task ${resolverTask.status}: ${resolverTask.result ?? ''}` });
    }
  }
}

// ---------- workflows ----------

interface WfStep {
  id: string;
  role: string;
  title?: string;
  // 5.4a: economy/reliability knobs
  priority?: number; // 0..10, added to the instance priority for the step task
  timeoutMin?: number; // 1..10080, active-task TTL -> blocked (step timeout)
  retry?: number; // 0..5, auto re-queue on blocked/cancelled
  // 5.4b S3: review gate — the role of the worktree pod whose branch this
  // step reviews (fresh-context pod gets the git diff + checklist in the
  // task body; reports done = approve / blocked = reject)
  review?: string;
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
    if (s.priority != null && (!Number.isInteger(s.priority) || s.priority < 0 || s.priority > 10)) {
      throw new OpError(400, `step ${s.id}: priority must be an integer 0..10`);
    }
    if (s.timeoutMin != null && (!Number.isInteger(s.timeoutMin) || s.timeoutMin < 1 || s.timeoutMin > 10080)) {
      throw new OpError(400, `step ${s.id}: timeoutMin must be an integer 1..10080`);
    }
    if (s.retry != null && (!Number.isInteger(s.retry) || s.retry < 0 || s.retry > 5)) {
      throw new OpError(400, `step ${s.id}: retry must be an integer 0..5`);
    }
    if (s.review != null && typeof s.review !== 'string') {
      throw new OpError(400, `step ${s.id}: review must be a pod role (string)`);
    }
  }
  return steps;
}

// S3: build the review step task body — the fresh-context reviewer gets the
// diff of the worktree pod's branch vs its base (the actual changes, not the
// agent's self-description) + a short checklist. Truncated: a review task
// must stay small (pod-socket transport). Synchronous: body construction
// happens once per step enqueue (git diff of a worktree is fast).
function reviewDiffBody(ctx: CoreCtx, role: string): string | null {
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod?.repo || !pod.branch || !pod.repo_base) return null;
  try {
    const st = ((): { ahead: number; dirty: boolean } => {
      const ahead = Number(
        execFileSync('git', ['-C', pod.repo!, 'rev-list', '--count', `${pod.repo_base}..${pod.branch}`], { timeout: 15_000, encoding: 'utf8' }).trim(),
      );
      const dirty = execFileSync('git', ['-C', pod.dir, 'status', '--porcelain', '--untracked-files=no'], { timeout: 15_000, encoding: 'utf8' }).trim() !== '';
      return { ahead, dirty };
    })();
    if (st.ahead === 0) return `Под ${role}: нет новых коммитов против base (${pod.repo_base}) — ревьювать нечего.`;
    let d: string;
    try {
      d = execFileSync('git', ['-C', pod.repo!, 'diff', `${pod.repo_base}...${pod.branch}`], { timeout: 15_000, encoding: 'utf8', maxBuffer: 8_000_000 });
    } catch {
      d = '(diff недоступен)';
    }
    const body = d.slice(0, 4000) + (d.length > 4000 ? '\n… (diff обрезан)' : '');
    return [
      `Ревью ветки ${pod.branch} (base: ${pod.repo_base}, ${st.ahead} commit(s) ahead, dirty: ${st.dirty}):`,
      '--- git diff base...branch ---',
      body,
      '---',
      'Чек-лист: 1) код делает то, что обещает заголовок таска; 2) нет side-effects вне задачи; 3) нет отладочного мусора/секретов.',
      'Вердикт: `flock task done <id>` (approve) или `flock task blocked <id> \'<что не так>\'` (reject).',
    ].join('\n');
  } catch {
    return null;
  }
}

function enqueueStepTask(ctx: CoreCtx, inst: store.WorkflowInstance, wf: store.Workflow, step: WfStep, prevResult?: string): void {
  const bodyLines = [
    `Workflow ${wf.name}: шаг ${step.id}`,
    inst.payload ? `Payload: ${inst.payload}` : '',
    prevResult ? `Результат предыдущего шага: ${prevResult}` : '',
  ].filter(Boolean);
  if (step.review) {
    const r = reviewDiffBody(ctx, step.review);
    if (r) bodyLines.push(r);
    else bodyLines.push(`Ревью под ${step.review}: worktree-привязка не найдена (под закрыт или без --repo) — запроси diff у оператора.`);
  }
  store.insertTask(ctx.store, {
    id: store.newId('t'),
    title: `[wf ${wf.name}] ${step.title ?? step.id}`,
    body: bodyLines.join('\n') || null,
    podRole: step.role,
    workflowInstanceId: inst.id,
    workflowStep: step.id,
    priority: (inst.priority ?? 0) + (step.priority ?? 0),
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
      // S3: the instance finished (review approved / last step done) — the
      // deferred merge runs NOW, on the worktree pod of the first step.
      // Merge conflict / gate-red: the instance becomes blocked (not done).
      // Fire-and-forget: the merge (git + optional test run) outlives the
      // report call; the instance state settles on its own.
      void deferredMerge(ctx, inst, wf, steps).catch((e) =>
        ctx.emit?.({ type: 'workflow_merge_error', instanceId: inst.id, message: e instanceof Error ? e.message : String(e) }),
      );
      return;
    }
    enqueueStepTask(ctx, inst, wf, next, task.result ?? undefined);
    store.setWorkflowInstanceState(ctx.store, inst.id, 'running', next.id);
    ctx.emit?.({ type: 'workflow_step', instanceId: inst.id, step: next.id, role: next.role });
  } else {
    // failure (blocked/cancelled): retry budget per step, then stop the instance
    const attempts = store.wfStepAttempts(ctx.store, inst.id, task.workflow_step!);
    const retryBudget = steps.find((s) => s.id === task.workflow_step)?.retry ?? 0;
    if (task.status !== 'cancelled' && attempts < retryBudget) {
      store.bumpWfStepAttempts(ctx.store, inst.id, task.workflow_step!);
      enqueueStepTask(ctx, inst, wf, steps.find((s) => s.id === task.workflow_step)!);
      ctx.emit?.({ type: 'workflow_retry', instanceId: inst.id, step: task.workflow_step, attempt: store.wfStepAttempts(ctx.store, inst.id, task.workflow_step!), reason: task.result ?? task.status });
      return;
    }
    const state = task.status === 'cancelled' ? 'cancelled' : 'blocked';
    store.setWorkflowInstanceState(ctx.store, inst.id, state);
    ctx.emit?.({ type: `workflow_${state}`, instanceId: inst.id, reason: task.result ?? task.status });
  }
}

// S3: deferred merge for a finished workflow instance — the worktree pod of
// the first step carries the branch; the instance's done build-task supplies
// the squash message + the require_test gate. A failed merge (conflict / red
// gate) blocks the INSTANCE (the operator sees it in workflow status), not a
// re-queued agent task — the work is done, integration failed.
async function deferredMerge(ctx: CoreCtx, inst: store.WorkflowInstance, wf: store.Workflow, steps: WfStep[]): Promise<void> {
  const firstRole = steps[0]?.role;
  if (!firstRole) return;
  const pod = store.getPodByRole(ctx.store, firstRole);
  if (!pod?.repo || !pod.branch) return;
  // the build step's done task (its id/title feed the squash commit message)
  const buildTask = store
    .listTasks(ctx.store)
    .filter((t) => t.workflow_instance_id === inst.id && t.workflow_step === steps[0].id && t.status === 'done')
    .sort((a, b) => (a.created_at < b.created_at ? 1 : -1))[0];
  if (!buildTask) return; // nothing merged yet (e.g. ff skip) — nothing to defer
  const outcome = await mergeWorktreeIfEligible(ctx, firstRole, buildTask, true);
  if (outcome.conflict) {
    store.setWorkflowInstanceState(ctx.store, inst.id, 'blocked');
    ctx.emit?.({ type: 'workflow_blocked', instanceId: inst.id, reason: `merge conflict: ${outcome.conflict}` });
    return;
  }
  if (outcome.gateFail) {
    store.setWorkflowInstanceState(ctx.store, inst.id, 'blocked');
    ctx.emit?.({ type: 'workflow_blocked', instanceId: inst.id, reason: `quality gate: ${outcome.gateFail}` });
    return;
  }
  if (outcome.note && !/skipped/.test(outcome.note)) {
    ctx.emit?.({ type: 'workflow_merged', instanceId: inst.id, role: firstRole, note: outcome.note });
  }
}

// 5.4a: step timeout/TTL — an active workflow step task that outlived the
// step's timeoutMin is blocked (step timeout); advanceWorkflow then applies
// the retry budget (re-queue) or stops the instance. Plain tasks (no
// workflow) have no TTL: a long human-in-the-loop task must not be killed.
export function checkWorkflowTimeouts(ctx: CoreCtx): void {
  const now = Date.now();
  for (const task of store.listTasks(ctx.store, 'active')) {
    if (!task.workflow_instance_id || !task.workflow_step) continue;
    const inst = store.getWorkflowInstance(ctx.store, task.workflow_instance_id);
    if (!inst || inst.state !== 'running') continue;
    const wf = store.getWorkflow(ctx.store, inst.workflow_id);
    if (!wf) continue;
    const steps: WfStep[] = (JSON.parse(wf.spec) as { steps: WfStep[] }).steps;
    const step = steps.find((s) => s.id === task.workflow_step);
    if (!step?.timeoutMin || !task.claimed_at) continue;
    const claimed = Date.parse(task.claimed_at);
    if (Number.isNaN(claimed)) continue;
    if (now - claimed > step.timeoutMin * 60_000) {
      try {
        store.setTaskStatus(ctx.store, task.id, 'blocked', { reason: `step ${step.id} timeout (${step.timeoutMin} min)`, result: `step ${step.id} timeout (${step.timeoutMin} min)` });
        ctx.emit?.({ type: 'task_blocked', taskId: task.id, pod: task.pod_role, reason: `step ${step.id} timeout` });
        advanceWorkflow(ctx, task.id);
      } catch {
        // already transitioned by a concurrent op; harmless
      }
    }
  }
}

async function workflowDefine(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const name = String(op.name ?? '').trim();
  if (!/^[a-z0-9][a-z0-9-]{0,30}$/.test(name)) throw new OpError(400, `bad workflow name: ${name || '(empty)'}`);
  const steps = parseSteps(op.steps);
  const existing = store.getWorkflow(ctx.store, name);
  if (existing) throw new OpError(409, `workflow exists: ${name}`);
  // 5.4b S2: instance-level quality gate flag (default: off, per-start flag can override)
  const requireTest = op.requireTest === true;
  const id = store.newId('wf');
  store.insertWorkflow(ctx.store, { id, name, spec: JSON.stringify({ steps, requireTest }) });
  ctx.emit?.({ type: 'workflow_defined', name, steps: steps.length, requireTest });
  return store.getWorkflow(ctx.store, id)!;
}

async function workflowStart(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const wf = store.getWorkflow(ctx.store, String(op.name ?? ''));
  if (!wf) throw new OpError(404, `no workflow: ${String(op.name ?? '')}`);
  const steps: WfStep[] = (JSON.parse(wf.spec) as { steps: WfStep[] }).steps;
  const rawPrio = op.priority;
  const priority = rawPrio != null ? Math.max(0, Math.min(10, Math.trunc(Number(rawPrio)))) : 0;
  if (rawPrio != null && (!Number.isInteger(Number(rawPrio)) || Number(rawPrio) < 0 || Number(rawPrio) > 10)) {
    throw new OpError(400, `priority must be an integer 0..10: ${String(rawPrio)}`);
  }
  // 5.4b S2: require_test — explicit start flag wins, else the workflow default
  const specReqs = (JSON.parse(wf.spec) as { requireTest?: boolean }).requireTest === true;
  const requireTest = op.requireTest === true || op.requireTest === false
    ? (op.requireTest as boolean)
    : specReqs;
  const instId = store.newId('wfi');
  store.insertWorkflowInstance(ctx.store, { id: instId, workflowId: wf.id, payload: op.payload ? String(op.payload) : null, priority, requireTest });
  store.setWorkflowInstanceState(ctx.store, instId, 'running', steps[0].id);
  enqueueStepTask(ctx, { id: instId, workflow_id: wf.id, payload: op.payload ? String(op.payload) : null, state: 'running', current_step: steps[0].id, created_at: '', finished_at: null, priority, require_test: requireTest ? 1 : 0 }, wf, steps[0]);
  ctx.emit?.({ type: 'workflow_started', instanceId: instId, workflow: wf.name, requireTest });
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
