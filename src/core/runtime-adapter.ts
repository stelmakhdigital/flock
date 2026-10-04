// RuntimeAdapter — the contract core uses to launch, project and check a
// runtime (5-method contract):
//
//   listInstalled  — is the runtime usable at all? (clear error at spawn)
//   project        — per-pod config projection (isolation, CLI snapshot)
//   deliverStartup — startup files: guidance_merge (managed blocks into the
//                    pod AGENTS.md) / send_text (first prompt after ready)
//   launchHarness  — pending sidecar -> command -> window -> ready gate ->
//                    session-identity verification (honest resume, fork rule)
//   checkReady     — live readiness: sidecar + foreground-pane guard
//                    (a ready sidecar with the pane at the shell = STALE)
//
// New runtime = new adapter + a manifest entry. pi = RPC bridge (typed
// session identity); bash = plain window (no ready gate, no resume).

import fs from 'node:fs';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import * as terminal from './terminal.js';
import {
  RUNNER_EXIT_MARKER,
  RUNNER_READY_MARKER,
  RUNNER_ERROR_MARKER,
  buildPendingState,
  buildRunnerCommand,
  frameMessage,
  newNonce,
  runLaunchId,
  LaunchPosture,
  resolveLaunchMode,
  resolveTrust,
  ForkSource,
  parseRunnerState,
  seatPaths,
  readActivity,
  detectGate,
  type RunnerState,
  type ActivityLine,
} from './bridge-protocol.js';

import {
  AgentManifest,
  installPodCli,
  manifestRuntime,
  projectPodConfig,
  userPiAgentDir,
} from './agent.js';

const execFileP = promisify(execFile);

// ── Types ───────────────────────────────────────────────────────────────────

export interface AdapterEnv {
  home: string; // FLOCK_HOME
  token: string;
  runnerPath: string; // dist/core/pi-bridge.js
}

// The pod's launch coordinates. tmux target is
// derived from the role (terminal.winTarget) — no separate field needed.
export interface PodBinding {
  role: string;
  cwd: string; // pod dir (workspace)
  model?: string;
  launchPosture?: LaunchPosture;
  permissionMode?: string;
  extraEnv?: Record<string, string>; // flock-managed env for the harness child
  seatRoot?: string; // canonical pod state dir (home/pods/<role>); != cwd for worktree pods
  trustLevel?: string; // sandbox trust level (overrides manifest; e.g. off for worktree pods)
  // C10: unified child-args (form a) — RAW passthrough into the child
  // argv/env, runtime-agnostic (pi/codex bridges). Appended last by the
  // bridge (explicit flags win by position).
  child?: { args?: string[]; env?: Record<string, string> };
  // pi first-class config axes (T1): mapped to pi CLI flags by the runner.
  // Ignored by non-pi runtimes.
  pi?: {
    thinking?: string;
    tools?: string[];
    excludeTools?: string[];
    skills?: string[];
    noSkills?: boolean;
    extensions?: string[];
    noExtensions?: boolean;
    systemPrompt?: string;
    appendSystemPrompt?: string[];
    noContextFiles?: boolean;
    // MCP servers -> written to the pod's <PI_CODING_AGENT_DIR>/mcp.json by
    // launchHarness (pod-level, replaces any previous mcp.json).
    mcp?: Record<string, { command?: string; args?: string[]; env?: Record<string, string>; url?: string; headers?: Record<string, string> }>;
  };
}

export interface StartupFile {
  path: string; // logical id: block id for guidance_merge, label for send_text
  content: string;
  deliveryHint: 'guidance_merge' | 'send_text';
  appliesOn?: ('fresh_start' | 'restore')[]; // default: both
  required?: boolean;
}

export interface StartupResult {
  delivered: number;
  failed: { path: string; error: string }[];
}

export type LaunchResult =
  | {
      ok: true;
      mode: 'fresh' | 'resume' | 'fork';
      target: string; // tmux window target (spawnPod result)
      pid: number | null; // pane pid (runkeeper liveness)
      trust?: 'approve' | 'no-approve'; // applied resource trust (observability)
      sessionFile?: string; // pi: typed session identity
      sessionId?: string;
      resumeToken?: string; // pi: the session file to persist for relaunch
      // (generic slot: a future runtime may carry a different token shape)
      resumeType?: 'pi_session_file';
    }
  | { ok: false; error: string; recovery?: 'attention_required'; evidence?: string };

export interface ReadyResult {
  ready: boolean;
  reason?: string;
  code?: string;
}

// ── Runtime-agnostic signal contract ────────────────────────────────────────
// The CORE asks every runtime the same three questions and does not know
// HOW the answer was produced (typed sidecar? transcript? pane scrape?).
// A runtime that cannot answer a question simply does not implement the
// method, and the core DEGRADES instead of erroring:
//
//   liveness()     absent -> generic pid check (bash/cmd: window process)
//   sendVerified() absent -> legacy visual probe (capture-based verify)
//   healthProbe()  absent -> core skips health for the pod (bash/cmd)
//
// The questions:
//
//   liveness(binding, run) — "is the agent of THIS run still alive?"
//     { alive: false, reason } — core writes `reason` into runs.exit_state
//     (keep the reason in the existing shape: 'clean' or 'crashed(...)';
//     the core treats a 'crashed…' state as a crash event + pm notification,
//     'clean' as a quiet end).
//
//   sendVerified(binding, text) — "deliver this text and PROVE it landed."
//     { ok: false, detail } — the send failed (op 504/503).
//     attempts — terminal attempts used (run.meta audit, unchanged).
//     The ack mechanism is the adapter's business (sidecar nonce, transcript
//     growth, …) — the core only needs ok/attempts + the ack name for the
//     run.meta audit line.
//
//   healthProbe(binding) — "what is the agent doing right now?"
//     ready:            is the agent at a usable state (sidecar ready /
//                       prompt)?
//     busy:             is it working a turn (streaming / in-turn marker)?
//     lastActivityAt:   ISO timestamp of the last observed activity (prompt
//                       landed / transcript written) — the idle ladder's clock.
//     gate:             a dialog that needs a HUMAN (permission prompt).
//                       channel 'answer' — answerable via `flock pod answer`
//                       (a typed channel exists); 'attach' — the operator
//                       must watch the pane (tmux attach).
//                       at — when the gate was observed (the core keeps the
//                       policy "a gate is live only for the current run").
//     null — no probe available (sidecar missing) — the core clears/skips.

export interface RunLike {
  id: string;
  pid?: number | null;
  meta?: string | null; // run-meta JSON (launchId lives in the 'created' entry)
  started_at: string;
}

export interface LivenessResult { alive: boolean; reason?: string }

export interface SendVerifiedResult { ok: boolean; attempts?: number; detail?: string; ack?: string }

export interface HealthProbe {
  ready: boolean;
  busy: boolean;
  lastActivityAt?: string;
  gate?: { id: string; title: string; channel: 'answer' | 'attach'; at?: string };
}

export interface RuntimeAdapter {
  readonly runtime: string;
  listInstalled(binding: PodBinding): Promise<{ installed: boolean; version?: string; detail?: string }>;
  project(binding: PodBinding): void;
  deliverStartup(
    files: StartupFile[],
    binding: PodBinding,
    phase: 'pre_launch' | 'post_ready',
  ): Promise<StartupResult>;
  launchHarness(
    binding: PodBinding,
    opts: { launchId: string; resumeToken?: string; forkSource?: ForkSource },
  ): Promise<LaunchResult>;
  checkReady(binding: PodBinding): Promise<ReadyResult>;
  // ── the three questions (see the contract above) ──
  liveness?(binding: PodBinding, run: RunLike): Promise<LivenessResult>;
  sendVerified?(binding: PodBinding, text: string): Promise<SendVerifiedResult>;
  healthProbe?(binding: PodBinding): Promise<HealthProbe | null>;
}

// ── Managed blocks (guidance_merge) ─────────────────────────────────────────
// Idempotent replace-or-append of a marked block inside a markdown file.
// Boot refresh and pod spawns both call it — content converges, never
// duplicates; user-authored text outside the blocks is never touched.

const blockStart = (id: string) => `<!-- BEGIN MANAGED BLOCK: ${id} -->`;
const blockEnd = (id: string) => `<!-- END MANAGED BLOCK: ${id} -->`;
const esc = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

export function mergeManagedBlock(
  targetPath: string,
  blockId: string,
  content: string,
  opts?: { replaceBlockIds?: string[] },
): void {
  const block = `${blockStart(blockId)}\n${content.trimEnd()}\n${blockEnd(blockId)}`;
  let existing: string | null = null;
  try {
    existing = fs.readFileSync(targetPath, 'utf8');
  } catch {
    existing = null;
  }
  if (existing === null) {
    fs.mkdirSync(path.dirname(targetPath), { recursive: true });
    fs.writeFileSync(targetPath, block + '\n');
    return;
  }
  const ids = Array.from(new Set([blockId, ...(opts?.replaceBlockIds ?? [])]));
  let updated = existing;
  for (const id of ids) {
    const re = new RegExp(`^${esc(blockStart(id))}\\n[\\s\\S]*?${esc(blockEnd(id))}\\n?`, 'm');
    // function replacement: block content may contain $-patterns
    updated = id === blockId ? updated.replace(re, () => block + '\n') : updated.replace(re, '');
  }
  if (!updated.includes(blockStart(blockId))) {
    const sep = updated.endsWith('\n\n') ? '' : updated.endsWith('\n') ? '\n' : '\n\n';
    updated = updated + sep + block + '\n';
  }
  fs.writeFileSync(targetPath, updated);
}

// Remove managed blocks whose id is not in `keep` (e.g. guidance that a
// previous manifest/profile wrote but the current one no longer provides).
export function pruneManagedBlocks(targetPath: string, keep: Set<string>): void {
  let existing: string | null = null;
  try {
    existing = fs.readFileSync(targetPath, 'utf8');
  } catch {
    return; // nothing to prune
  }
  if (existing === null) return;
  const re = /<!-- BEGIN MANAGED BLOCK: (\S+) -->[\s\S]*?<!-- END MANAGED BLOCK: \1 -->\n?/g;
  let updated = '';
  let last = 0;
  let m: RegExpExecArray | null;
  let changed = false;
  while ((m = re.exec(existing)) !== null) {
    if (!keep.has(m[1])) {
      updated += existing.slice(last, m.index);
      changed = true;
      last = re.lastIndex;
    }
  }
  if (changed) {
    updated += existing.slice(last);
    fs.writeFileSync(targetPath, updated);
  }
}

// ── Pi runtime ──────────────────────────────────────────────────────────────

// ---------- live session probe (C6) ----------
// The session file must exist and be non-empty; a corrupt sidecar (parse
// failure) means the last runner never reached ready — resuming against an
// assumed session is exactly what C6 forbids. ponytail: the sidecar is
// written by our own bridge, so parse-failure IS the staleness signal.
function probePiSession(sessionFile: string): { ok: true } | { ok: false; detail: string } {
  if (!fs.existsSync(sessionFile)) return { ok: false, detail: `session file missing: ${sessionFile}` };
  if (fs.statSync(sessionFile).size === 0) return { ok: false, detail: `session file is empty: ${sessionFile}` };
  const parent = path.dirname(sessionFile);
  // …/pods/<role>/.pi/sessions/*.jsonl -> seat root is two levels up
  if (path.basename(parent) === 'sessions') {
    const statePath = path.join(path.dirname(parent), 'runner-state.json');
    if (fs.existsSync(statePath)) {
      const st = parseRunnerState(fs.readFileSync(statePath, 'utf8'));
      if (!st) return { ok: false, detail: 'runner-state sidecar is corrupt (last launch never reached ready)' };
    }
  }
  return { ok: true };
}

export class PiRuntimeAdapter implements RuntimeAdapter {
  readonly runtime = 'pi';
  constructor(
    private m: AgentManifest,
    private env: AdapterEnv,
  ) {}

  async listInstalled(): Promise<{ installed: boolean; version?: string; detail?: string }> {
    try {
      const { stdout } = await execFileP(this.m.command, ['--version'], { timeout: 5000 });
      return { installed: true, version: stdout.trim().split('\n')[0] };
    } catch (e) {
      return { installed: false, detail: `${this.m.command} --version failed: ${e instanceof Error ? e.message : String(e)}` };
    }
  }

  // Config isolation (symlinked PI_CODING_AGENT_DIR) + in-pod CLI snapshot.
  project(binding: PodBinding): void {
    const paths = seatPaths(this.env.home, binding.role);
    fs.mkdirSync(paths.agentDir, { recursive: true });
    fs.mkdirSync(paths.sessionsDir, { recursive: true });
    projectPodConfig(userPiAgentDir(), paths.agentDir);
    installPodCli(binding.cwd, this.env.token);
  }

  async deliverStartup(
    files: StartupFile[],
    binding: PodBinding,
    phase: 'pre_launch' | 'post_ready',
  ): Promise<StartupResult> {
    const delivered: string[] = [];
    const failed: { path: string; error: string }[] = [];
    for (const f of files) {
      try {
        if (phase === 'pre_launch' && f.deliveryHint === 'guidance_merge') {
          // Must land BEFORE launch: pi loads the context file at process start.
          mergeManagedBlock(path.join(binding.cwd, 'AGENTS.md'), f.path, f.content);
          delivered.push(f.path);
        } else if (phase === 'post_ready' && f.deliveryHint === 'send_text') {
          // One prompt through the typed delivery path (frame + raw paste).
          await terminal.send(terminal.winTarget(binding.role), frameMessage(f.content), { raw: true, attempts: 1 });
          delivered.push(f.path);
        }
      } catch (e) {
        const error = e instanceof Error ? e.message : String(e);
        if (f.required) failed.push({ path: f.path, error });
      }
    }
    return { delivered: delivered.length, failed };
  }

  async launchHarness(
    binding: PodBinding,
    opts: { launchId: string; resumeToken?: string; forkSource?: ForkSource },
  ): Promise<LaunchResult> {
    if (binding.permissionMode) {
      return {
        ok: false,
        error: `pi runtime: permissionMode "${binding.permissionMode}" is rejected — pi resource trust is a separate mechanism (use trust / launchPosture)`,
      };
    }
    const mode = resolveLaunchMode(opts);
    if (mode.mode === 'error') return { ok: false, error: mode.error, recovery: mode.recovery };

    if (mode.mode === 'resume') {
      // C6 strict honest resume: a missing session file is attention_required
      // (the operator decides: --fresh or fix the pin) — never a silent fresh.
      if (!fs.existsSync(mode.sessionFile)) {
        return { ok: false, error: `resume: session file no longer exists: ${mode.sessionFile}`, recovery: 'attention_required' };
      }
      // probe the live session before committing to it (sidecar must agree
      // on the launchId scope; a stale sidecar from a dead process is a
      // failed resume, not an assumption)
      const probe = probePiSession(mode.sessionFile);
      if (!probe.ok) {
        return { ok: false, error: `resume: live session probe failed: ${probe.detail}`, recovery: 'attention_required' };
      }
    }
    if (mode.mode === 'fork' && (mode.forkRef.includes('/') || fs.existsSync(mode.forkRef))) {
      // Looks like a path (role refs are resolved to paths by the caller).
      if (!fs.existsSync(mode.forkRef)) {
        return { ok: false, error: `fork: parent session file not found: ${mode.forkRef}`, recovery: 'attention_required' };
      }
    }

    const trust = resolveTrust(this.m.trust, binding.launchPosture);
    const paths = seatPaths(this.env.home, binding.role);
    fs.mkdirSync(paths.agentDir, { recursive: true });
    fs.mkdirSync(paths.sessionsDir, { recursive: true });
    // Pending record (launchId-scoped) BEFORE the launch: a dead runner is
    // distinguishable from a missing one.
    fs.mkdirSync(path.dirname(paths.runnerStatePath), { recursive: true });
    fs.writeFileSync(paths.runnerStatePath, JSON.stringify(buildPendingState(opts.launchId, new Date().toISOString()), null, 2));
    // T1: pod-level MCP config — the pod's <PI_CODING_AGENT_DIR>/mcp.json is
    // replaced on every launch (a profile/manifest switch must not leave
    // another agent's servers behind).
    // ponytail: the pod sees ONLY this pod-level mcp.json; the user's
    // ~/.pi/agent/mcp.json is invisible to the pod (PI_CODING_AGENT_DIR is
    // redirected). That is the isolation we want, not a bug to fix.
    writePodMcpConfig(paths.agentDir, binding.pi?.mcp);

    const cmd = buildRunnerCommand({
      runnerPath: this.env.runnerPath,
      stateRoot: this.env.home,
      role: binding.role,
      cwd: binding.cwd,
      launchId: opts.launchId,
      trust,
      trustOption: this.m.trustOption,
      trustLevel: binding.trustLevel ?? this.m.trustLevel,
      model: binding.model,
      sessionFile: mode.mode === 'resume' ? mode.sessionFile : undefined,
      forkRef: mode.mode === 'fork' ? mode.forkRef : undefined,
      extraEnv: binding.extraEnv ? Object.entries(binding.extraEnv).map(([k, v]) => `${k}=${v}`) : undefined,
      // C10: raw child args/env (manifest `child`) + mapped pi axes ride
      // together in the single --child-args JSON flag
      child: binding.child,
      pi: binding.pi,
    });
    // PERSISTENT PANE: the window outlives the runner.
    // Alive -> typed-stop the old foreground (C-c -> runner writes the
    // sidecar `exited`, pane returns to the shell); gone -> create it.
    // On failure we do NOT kill the window: the operator keeps the pane
    // (scrollback, capture) and can relaunch into it.
    const target = terminal.winTarget(binding.role);
    if (await terminal.windowExists(binding.role)) {
      await terminal.stopWindowProcess(binding.role);
      // tmux settles the pane/window close asynchronously; give it a moment
      // before the recheck (a still-listed dying window would swallow the paste).
      await terminal.sleep(400);
    }
    // The stop can still leave no window (legacy windows die with the
    // command, a human typed exit): in that case create a fresh pane.
    if (!(await terminal.windowExists(binding.role))) {
      await terminal.spawnPod({ role: binding.role, dir: binding.cwd });
    }
    await terminal.launchInWindow(binding.role, cmd, binding.cwd, this.m.env);
    const ready = await terminal.waitForRunnerReady(this.env.home, binding.role, opts.launchId, 25000);
    if (!ready.ok) {
      const evidence = (await terminal.capture(target, 30).catch(() => '')).slice(-800);
      return {
        ok: false,
        error: ready.reason === 'exited'
          ? `agent exited during launch (code ${ready.code ?? '?'})`
          : `agent did not report ready in 25s${ready.detail ? ` (${ready.detail})` : ''}`,
        recovery: 'attention_required',
        evidence: evidence || undefined,
      };
    }
    const sessionFile = ready.state.sessionFile;
    if (!sessionFile) {
      const evidence = (await terminal.capture(target, 30).catch(() => '')).slice(-800);
      return { ok: false, error: 'runner ready but reported no session file', recovery: 'attention_required', evidence: evidence || undefined };
    }    // Fork rule: the captured token must be the NEW post-fork session, never
    // the parent's.
    if (mode.mode === 'fork' && sessionFile === mode.forkRef) {
      return { ok: false, error: 'fork reported the parent session file — refused', recovery: 'attention_required' };
    }
    if (mode.mode === 'resume' && sessionFile !== mode.sessionFile) {
      return { ok: false, error: `ready state is not the requested session file (got ${sessionFile})`, recovery: 'attention_required' };
    }
    const panePid = await terminal.panePid(target).catch(() => null);
    return {
      ok: true,
      mode: mode.mode,
      target,
      pid: panePid,
      trust,
      sessionFile,
      sessionId: ready.state.sessionId,
      resumeToken: sessionFile,
      resumeType: 'pi_session_file',
    };
  }

  // ── the three questions (pi answers with typed signals) ─────────────────
  // (1) liveness — moved byte-for-byte from the core runkeeper (checkRunLiveness):
  //     step 1 typed sidecar exit (launchId-scoped), step 2 foreground guard
  //     (sidecar alive but the pane fell back to the shell = untyped death).
  async liveness(binding: PodBinding, run: RunLike): Promise<LivenessResult> {
    const launchId = runLaunchId(run);
    const st = terminal.readRunnerState(this.env.home, binding.role);
    if (launchId && st?.exited && st.launchId === launchId) {
      const ex = st.exited;
      return { alive: false, reason: ex.code === 0 && !ex.signal ? 'clean' : `crashed(${ex.signal ? `signal ${ex.signal}` : `code ${ex.code}`})` };
    }
    if (launchId && st && st.launchId === launchId && !st.exited) {
      const fg = await terminal.paneCommand(terminal.winTarget(binding.role)).catch(() => '');
      if (terminal.SHELL_COMMANDS.has(fg)) {
        return { alive: false, reason: 'crashed(runner gone, pane at shell)' };
      }
    }
    return { alive: true };
  }

  // (2) sendVerified — flockmsg v2: framed message (nonce), raw paste, ack =
  //     the sidecar's lastPrompt carrying our nonce (5s deadline, 300ms poll).
  async sendVerified(binding: PodBinding, text: string): Promise<SendVerifiedResult> {
    const nonce = newNonce();
    const wire = frameMessage(text, nonce);
    const res = await terminal.send(terminal.winTarget(binding.role), wire, { raw: true });
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      const st = terminal.readRunnerState(this.env.home, binding.role);
      if (st?.lastPrompt?.nonce === nonce) {
        return { ok: true, attempts: res.attempts, ack: 'sidecar' };
      }
      await new Promise((r) => setTimeout(r, 300));
    }
    return { ok: false, attempts: res.attempts, detail: 'runner did not ack the message (check the pod pane)' };
  }

  // (3) healthProbe — sidecar (ready/streaming/lastPrompt.at) + the typed
  //     activity log's auto-denied dialog (gate, channel 'attach': C10 —
  //     the operator answer channel is gone, the lever is the pane).
  async healthProbe(binding: PodBinding): Promise<HealthProbe | null> {
    const state = terminal.readRunnerState(this.env.home, binding.role);
    const activity = readActivity(this.env.home, binding.role);
    const gateRaw = detectGate(activity);
    const gateAt = gateRaw && typeof gateRaw.at === 'string' ? Date.parse(gateRaw.at) : 0;
    const gate = gateRaw && typeof gateRaw.id === 'string' && Number.isFinite(gateAt)
      ? { id: gateRaw.id, title: typeof gateRaw.title === 'string' ? gateRaw.title : '(без заголовка)', channel: 'attach' as const, at: new Date(gateAt).toISOString() }
      : undefined;
    if (!state) {
      // sidecar missing: nothing typed to report (a dead runner has no
      // response path for its stale dialogs either)
      return { ready: false, busy: false, gate };
    }
    return {
      ready: !!state.ready,
      busy: !!state.streaming,
      lastActivityAt: state.lastPrompt?.at,
      gate,
    };
  }

  async checkReady(binding: PodBinding): Promise<ReadyResult> {
    const target = terminal.winTarget(binding.role);
    const state = terminal.readRunnerState(this.env.home, binding.role);
    if (state?.exited) return { ready: false, reason: `runner exited (code ${state.exited.code ?? '?'})`, code: 'runner_exited' };
    const cmd = await terminal.paneCommand(target).catch(() => '');
    const atShell = terminal.SHELL_COMMANDS.has(cmd);
    if (state?.ready) {
      if (atShell) return { ready: false, reason: 'stale: pane is at the shell', code: 'stale_ready' };
      return { ready: true, reason: 'sidecar ready' };
    }
    // Secondary signals (runner-authored markers in scrollback) only when the
    // sidecar has no answer — a stale marker from a prior launch cannot
    // override a live sidecar. Negative markers first.
    if (!state) {
      const out = await terminal.capture(target, 80).catch(() => '');
      if (out.includes(RUNNER_ERROR_MARKER)) return { ready: false, reason: 'runner error marker in pane', code: 'runner_error' };
      if (out.includes(RUNNER_EXIT_MARKER)) return { ready: false, reason: 'runner exit marker in pane', code: 'runner_exited' };
      if (out.includes(RUNNER_READY_MARKER)) {
        if (atShell) return { ready: false, reason: 'READY marker is stale scrollback; pane at the shell', code: 'stale_ready' };
        return { ready: true, reason: 'ready marker (no sidecar)' };
      }
    }
    return { ready: false, reason: 'awaiting runtime', code: 'awaiting_runtime' };
  }
}


// T1: pod-level MCP config. Writes <agentDir>/mcp.json when servers are
// given, REMOVES it otherwise — a relaunch with a manifest that has no mcp
// must not keep the previous agent's servers.
export function writePodMcpConfig(
  agentDir: string,
  mcp: Record<string, { command?: string; args?: string[]; env?: Record<string, string>; url?: string; headers?: Record<string, string> }> | undefined,
): void {
  const p = path.join(agentDir, 'mcp.json');
  try {
    if (mcp && Object.keys(mcp).length > 0) {
      fs.writeFileSync(p, JSON.stringify({ mcpServers: mcp }, null, 2));
    } else if (fs.existsSync(p)) {
      fs.rmSync(p);
    }
  } catch (e) {
    // MCP is best-effort: a broken mcp.json should degrade the pod (no
    // servers), not kill the launch.
    console.warn(`[flock] mcp.json write failed: ${e instanceof Error ? e.message : e}`);
  }
}

// ── Bash runtime (plain window) ─────────────────────────────────────────────

export class BashRuntimeAdapter implements RuntimeAdapter {
  readonly runtime = 'bash';
  constructor(
    private m: AgentManifest,
    private env: AdapterEnv,
  ) {}

  async listInstalled(): Promise<{ installed: boolean; version?: string; detail?: string }> {
    try {
      const { stdout } = await execFileP(this.m.command, ['--version'], { timeout: 5000 });
      return { installed: true, version: stdout.trim().split('\n')[0] };
    } catch (e) {
      return { installed: false, detail: `${this.m.command} not found: ${e instanceof Error ? e.message : String(e)}` };
    }
  }

  project(binding: PodBinding): void {
    installPodCli(binding.cwd, this.env.token);
  }

  async deliverStartup(
    files: StartupFile[],
    binding: PodBinding,
    phase: 'pre_launch' | 'post_ready',
  ): Promise<StartupResult> {
    const delivered: string[] = [];
    for (const f of files) {
      if (phase === 'post_ready' && f.deliveryHint === 'send_text') {
        await terminal.send(terminal.winTarget(binding.role), f.content, { raw: true, attempts: 1 });
        delivered.push(f.path);
      } else if (phase === 'pre_launch' && f.deliveryHint === 'guidance_merge') {
        mergeManagedBlock(path.join(binding.cwd, 'AGENTS.md'), f.path, f.content);
        delivered.push(f.path);
      }
    }
    return { delivered: delivered.length, failed: [] };
  }

  async launchHarness(
    binding: PodBinding,
    opts: { launchId: string; resumeToken?: string; forkSource?: ForkSource },
  ): Promise<LaunchResult> {
    if (opts.resumeToken || opts.forkSource) {
      return { ok: false, error: 'bash runtime: resume/fork not supported (plain window has no session)', recovery: 'attention_required' };
    }
    // bash: the agent IS the shell — its life is the window's life (no
    // persistent pane needed: there is no session to outlive).
    if (await terminal.windowExists(binding.role)) {
      await terminal.killWindow(binding.role).catch(() => {});
    }
    const { target, pid } = await terminal.spawnPod({
      role: binding.role,
      dir: binding.cwd,
      cmd: [this.m.command, ...(this.m.args ?? [])].join(' '),
      env: this.m.env,
    });
    return { ok: true, mode: 'fresh', target, pid };
  }

  async checkReady(binding: PodBinding): Promise<ReadyResult> {
    const alive = await terminal.paneAlive(binding.role).catch(() => false);
    return alive ? { ready: true, reason: 'pane alive' } : { ready: false, reason: 'pane dead', code: 'pane_dead' };
  }
}


// ── Registry ────────────────────────────────────────────────────────────────

// 'cmd' (raw --cmd window) has no adapter — the caller handles it.
export function getAdapter(m: AgentManifest, env: AdapterEnv): RuntimeAdapter | null {
  const rt = manifestRuntime(m);
  if (rt === 'pi') return new PiRuntimeAdapter(m, env);
  if (rt === 'bash') return new BashRuntimeAdapter(m, env);
  // C12b: claude/codex adapters removed — a manifest with a removed runtime
  // must NOT produce an adapter; the caller fails with "runtime not
  // supported". The contract (RuntimeAdapter + bridge-protocol) is the
  // extension point: a new runtime = one adapter, zero core lines.
  return null;
}
