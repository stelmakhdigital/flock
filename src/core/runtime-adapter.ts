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
  seatPaths,
  readActivity,
  detectGate,
  type RunnerState,
  type ActivityLine,
} from './runner-protocol.js';
import {
  CLAUDE_BOOT_DIALOGS,
  CLAUDE_FIXED_ENV,
  buildClaudeArgs,
  claudeConfigDir,
  claudePaneBusy,
  claudePaneReady,
  claudeProjectsDir,
  claudeTranscriptFp,
  detectClaudeGate,
  latestClaudeSession,
  seedClaudeConfig,
  validateClaudeSessionToken,
  waitForTranscriptGrowth,
} from './claude-protocol.js';
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
  runnerPath: string; // dist/core/runner.js
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
      resumeType?: 'pi_session_file' | 'claude_session_uuid';
    }
  | { ok: false; error: string; recovery?: 'retry_fresh' | 'attention_required'; evidence?: string };

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
  // The session token of the pod's last run (honest-relaunch default). pi uses
  // run-meta (store.latestSessionFile) and does not implement this; claude
  // scans its per-pod transcripts.
  latestSessionToken?(binding: PodBinding): Promise<string | null>;
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

    if (mode.mode === 'resume' && !fs.existsSync(mode.sessionFile)) {
      return { ok: false, error: `resume: session file no longer exists: ${mode.sessionFile}`, recovery: 'retry_fresh' };
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
  //     activity log's open dialog (gate, channel 'answer').
  async healthProbe(binding: PodBinding): Promise<HealthProbe | null> {
    const state = terminal.readRunnerState(this.env.home, binding.role);
    const activity = readActivity(this.env.home, binding.role);
    const gateRaw = detectGate(activity);
    const gateAt = gateRaw && typeof gateRaw.at === 'string' ? Date.parse(gateRaw.at) : 0;
    const gate = gateRaw && typeof gateRaw.id === 'string' && Number.isFinite(gateAt)
      ? { id: gateRaw.id, title: typeof gateRaw.title === 'string' ? gateRaw.title : '(без заголовка)', channel: 'answer' as const, at: new Date(gateAt).toISOString() }
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


// ── Claude Code runtime (interactive TUI, persistent pane) ─────────────────
// A claude pod runs the claude-code TUI as the persistent pane's foreground.
// No RPC bridge: delivery = raw paste, typed signals = the per-pod config
// dir transcripts (<config>/projects/<slug>/<uuid>.jsonl grow on every turn:
// delivery ack + session identity). Liveness = pane foreground (the
// runkeeper foreground guard; there is no sidecar in this runtime).
// First-launch dialogs (theme/API key/security/folder trust) are answered
// deterministically by the ready-wait loop (observed live, per config dir +
// per cwd, one-time).
export class ClaudeRuntimeAdapter implements RuntimeAdapter {
  readonly runtime = 'claude';
  constructor(
    private m: AgentManifest,
    private env: AdapterEnv,
  ) {}

  // transcripts are keyed by the CWD slug, config lives in the seat
  projectsDir(configDir: string, cwd: string): string {
    return claudeProjectsDir(configDir, cwd);
  }

  async listInstalled(): Promise<{ installed: boolean; version?: string; detail?: string }> {
    try {
      const { stdout } = await execFileP(this.m.command, ['--version'], { timeout: 5000 });
      return { installed: true, version: stdout.trim().split('\n')[0] };
    } catch (e) {
      return { installed: false, detail: `${this.m.command} --version failed: ${e instanceof Error ? e.message : String(e)}` };
    }
  }

  project(binding: PodBinding): void {
    // Per-pod config home = full config+sessions isolation (and it avoids a
    // root-owned ~/.claude). Auth is env-based (manifest env: ANTHROPIC_*).
    // Pre-seed onboarding state: a fresh config hard-fails the first-launch
    // api.anthropic.com connectivity check (observed live).
    const cfg = claudeConfigDir(binding.seatRoot ?? binding.cwd);
    fs.mkdirSync(cfg, { recursive: true });
    seedClaudeConfig(cfg, this.m.env?.ANTHROPIC_API_KEY);
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
          // Claude reads CLAUDE.md (not AGENTS.md); it must land BEFORE launch.
          mergeManagedBlock(path.join(binding.cwd, 'CLAUDE.md'), f.path, f.content);
          delivered.push(f.path);
        } else if (phase === 'post_ready' && f.deliveryHint === 'send_text') {
          // Raw paste + Enter into the TUI input.
          await terminal.send(terminal.winTarget(binding.role), f.content, { raw: true, attempts: 1 });
          delivered.push(f.path);
        }
      } catch (e) {
        const error = e instanceof Error ? e.message : String(e);
        if (f.required) failed.push({ path: f.path, error });
      }
    }
    return { delivered: delivered.length, failed };
  }

  // The session token of the last run on this pod (newest transcript).
  async latestSessionToken(binding: PodBinding): Promise<string | null> {
    return latestClaudeSession(this.projectsDir(claudeConfigDir(binding.seatRoot ?? binding.cwd), binding.cwd))?.token ?? null;
  }

  async launchHarness(
    binding: PodBinding,
    opts: { launchId: string; resumeToken?: string; forkSource?: ForkSource },
  ): Promise<LaunchResult> {
    // claude tokens are transcript uuids (resumeToken and forkSource share the
    // same shape; fork just adds --fork-session)
    const mode: 'fresh' | 'resume' | 'fork' = opts.forkSource ? 'fork' : opts.resumeToken ? 'resume' : 'fresh';
    const token = mode === 'fresh' ? undefined : (mode === 'resume' ? opts.resumeToken : opts.forkSource!.value);
    if (mode !== 'fresh' && (!token || !validateClaudeSessionToken(token))) {
      return { ok: false, error: `invalid claude session token: ${token ?? '(empty)'}`, recovery: 'attention_required' };
    }
    if (mode !== 'fresh' && !fs.existsSync(path.join(this.projectsDir(claudeConfigDir(binding.seatRoot ?? binding.cwd), binding.cwd), `${token}.jsonl`))) {
      return { ok: false, error: `resume: transcript not found: ${token}`, recovery: 'retry_fresh' };
    }
    // permission axis (claude-native): manifest permissionMode, binding
    // override, full_bypass forces bypassPermissions
    let pm = this.m.permissionMode;
    if (binding.permissionMode) pm = binding.permissionMode;
    if (binding.launchPosture === 'full_bypass') pm = 'bypassPermissions';

    const args = [...(this.m.args ?? []), ...buildClaudeArgs({
      model: binding.model,
      resumeToken: token,
      fork: mode === 'fork',
      permissionMode: pm,
    })];
    const cmd = [this.m.command, ...args].map(shellQuote).join(' ');

    // PERSISTENT PANE (same invariant as pi): typed-stop the old foreground,
    // launch into the same window; on failure keep the window.
    if (await terminal.windowExists(binding.role)) {
      await this.stopClaude(binding.role);
      await terminal.sleep(400);
    }
    if (!(await terminal.windowExists(binding.role))) {
      await terminal.spawnPod({ role: binding.role, dir: binding.cwd });
    }
    const cfg = claudeConfigDir(binding.seatRoot ?? binding.cwd);
    const extraEnv = { ...CLAUDE_FIXED_ENV, CLAUDE_CONFIG_DIR: cfg, ...(this.m.env ?? {}) };
    await terminal.launchInWindow(binding.role, cmd, binding.cwd, extraEnv);

    const target = terminal.winTarget(binding.role);
    const ready = await this.waitForClaudeReady(target, 60000);
    if (!ready) {
      const evidence = (await terminal.capture(target, 40).catch(() => '')).slice(-800);
      return { ok: false, error: 'claude did not reach the prompt in 60s', recovery: 'attention_required', evidence: evidence || undefined };
    }
    const panePid = await terminal.panePid(target).catch(() => null);
    if (mode === 'fork') {
      // Unlike pi (new session file appears at fork time), claude's fork
      // transcript materialises on the FIRST TURN - so there is no
      // synchronous identity check here. The "fork never stays on the parent"
      // rule is guaranteed by the runtime: --resume X --fork-session creates a
      // new session id and never writes into X's transcript. Nothing to do.
    }
    return { ok: true, mode, target, pid: panePid, resumeToken: mode === 'fresh' ? undefined : token, resumeType: 'claude_session_uuid' };
  }

  // Ready-wait: poll the pane, answer the one-time boot dialogs (each once),
  // done at the idle prompt with no open confirmation AND the foreground is
  // the TUI (a dead launch falls back to the shell, whose prompt can look
  // similar under some themes).
  private async waitForClaudeReady(target: string, timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    const answered = new Set<number>();
    const ready = async (): Promise<boolean> => {
      const out = await terminal.capture(target, 60).catch(() => '');
      const cmd = await terminal.paneCommand(target).catch(() => '');
      return claudePaneReady(out) && !!cmd && !terminal.SHELL_COMMANDS.has(cmd);
    };
    while (Date.now() < deadline) {
      const out = await terminal.capture(target, 60).catch(() => '');
      for (let i = 0; i < CLAUDE_BOOT_DIALOGS.length; i++) {
        const d = CLAUDE_BOOT_DIALOGS[i];
        if (!answered.has(i) && out.includes(d.marker)) {
          for (const k of d.keys) await terminal.sendKey(target, k);
          answered.add(i);
          await terminal.sleep(400);
          break;
        }
      }
      if (await ready()) return true;
      await terminal.sleep(500);
    }
    return ready();
  }

  // Typed stop for the claude TUI: C-c interrupts a running turn / clears the
  // input; an idle prompt needs /exit (observed: a single C-c does not exit).
  private async stopClaude(role: string): Promise<void> {
    const target = terminal.winTarget(role);
    const cmd0 = await terminal.paneCommand(target).catch(() => '');
    if (!cmd0 || terminal.SHELL_COMMANDS.has(cmd0)) return;
    await terminal.sendKey(target, 'C-c');
    await terminal.sleep(400);
    let c = await terminal.paneCommand(target).catch(() => '');
    if (c && !terminal.SHELL_COMMANDS.has(c)) {
      await terminal.send(target, '/exit', { raw: true, attempts: 1 });
      const deadline = Date.now() + 8000;
      while (Date.now() < deadline) {
        await terminal.sleep(250);
        c = await terminal.paneCommand(target).catch(() => '');
        if (!c || terminal.SHELL_COMMANDS.has(c)) return;
      }
    }
    // last resort: SIGKILL the pane's children (the shell stays — it IS the pane)
    const pid = await terminal.panePid(target);
    if (pid) {
      try {
        await execFileP('pkill', ['-9', '-P', String(pid)], { timeout: 3000 });
      } catch {
        /* the launch paste will surface the failure */
      }
    }
  }

  // ── the three questions (claude answers with pane + transcript signals) ──
  // (1) liveness — foreground guard: the TUI exited -> the pane is back at the
  //     shell. (A missing/unknown foreground is NOT a crash here — the generic
  //     pid check catches a dead window.)
  async liveness(binding: PodBinding, _run: RunLike): Promise<LivenessResult> {
    const fg = await terminal.paneCommand(terminal.winTarget(binding.role)).catch(() => '');
    if (fg && terminal.SHELL_COMMANDS.has(fg)) {
      return { alive: false, reason: 'crashed(claude exited, pane at shell)' };
    }
    return { alive: true };
  }

  // (2) sendVerified — raw paste + Enter; ack = the per-pod transcript GROWS
  //     (a user entry lands within seconds, independent of turn duration).
  async sendVerified(binding: PodBinding, text: string): Promise<SendVerifiedResult> {
    const projectsDir = claudeProjectsDir(claudeConfigDir(binding.seatRoot ?? binding.cwd), binding.cwd);
    const before = claudeTranscriptFp(projectsDir);
    const res = await terminal.send(terminal.winTarget(binding.role), text, { raw: true });
    const { grown } = await waitForTranscriptGrowth(projectsDir, before, 30000);
    if (!grown) return { ok: false, attempts: res.attempts, detail: 'claude transcript did not grow after send (check the pod pane)' };
    return { ok: true, attempts: res.attempts, ack: 'transcript' };
  }

  // (3) healthProbe — pane scrape: ready = idle prompt, busy = in-turn marker,
  //     lastActivityAt = newest transcript mtime, gate = an open permission
  //     prompt (detectClaudeGate: the idle footer is NOT a gate; channel
  //     'attach' — there is no /answer channel for the TUI). at = now: the
  //     pane is a LIVE scrape, so the core's "gate is live only for the
  //     current run" rule sees a fresh timestamp, not a stale one.
  async healthProbe(binding: PodBinding): Promise<HealthProbe | null> {
    const target = terminal.winTarget(binding.role);
    const out = await terminal.capture(target, 60).catch(() => '');
    const gate = detectClaudeGate(out);
    const gated = gate ? { ...gate, at: new Date().toISOString() } : undefined;
    const last = latestClaudeSession(this.projectsDir(claudeConfigDir(binding.seatRoot ?? binding.cwd), binding.cwd));
    let lastActivityAt: string | undefined;
    if (last) {
      try {
        lastActivityAt = new Date(fs.statSync(last.file).mtimeMs).toISOString();
      } catch {
        lastActivityAt = undefined;
      }
    }
    return {
      ready: claudePaneReady(out),
      busy: claudePaneBusy(out),
      lastActivityAt,
      gate: gated,
    };
  }

  async checkReady(binding: PodBinding): Promise<ReadyResult> {
    const target = terminal.winTarget(binding.role);
    const alive = await terminal.paneAlive(binding.role).catch(() => false);
    if (!alive) return { ready: false, reason: 'window gone', code: 'window_gone' };
    const cmd = await terminal.paneCommand(target).catch(() => '');
    if (!cmd || terminal.SHELL_COMMANDS.has(cmd)) return { ready: false, reason: 'stale: pane is at the shell', code: 'stale_ready' };
    const out = await terminal.capture(target, 40).catch(() => '');
    if (claudePaneBusy(out)) return { ready: true, reason: 'working (in turn)' };
    if (claudePaneReady(out)) return { ready: true, reason: 'at prompt' };
    return { ready: false, reason: 'no prompt marker (boot dialog?)', code: 'awaiting_runtime' };
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

// minimal quoting for the one-line paste (our builders emit [A-Za-z0-9._:@/-]
// tokens; anything exotic gets JSON-quoted)
function shellQuote(s: string): string {
  return /^[A-Za-z0-9._:@/=-]*$/.test(s) ? s : JSON.stringify(s);
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
  if (rt === 'claude') return new ClaudeRuntimeAdapter(m, env);
  return null;
}
