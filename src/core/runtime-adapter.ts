// RuntimeAdapter — the contract core uses to launch, project and check a
// runtime (OpenRig's 5-method contract, flock vocabulary):
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
  LaunchPosture,
  resolveLaunchMode,
  resolveTrust,
  ForkSource,
  seatPaths,
  type RunnerState,
} from './runner-protocol.js';
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

// The pod's launch coordinates (OpenRig NodeBinding analogue). tmux target is
// derived from the role (terminal.winTarget) — no separate field needed.
export interface PodBinding {
  role: string;
  cwd: string; // pod dir (workspace)
  model?: string;
  launchPosture?: LaunchPosture;
  permissionMode?: string;
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
      resumeType?: 'pi_session_file';
    }
  | { ok: false; error: string; recovery?: 'retry_fresh' | 'attention_required'; evidence?: string };

export interface ReadyResult {
  ready: boolean;
  reason?: string;
  code?: string;
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

    const cmd = buildRunnerCommand({
      runnerPath: this.env.runnerPath,
      stateRoot: this.env.home,
      role: binding.role,
      cwd: binding.cwd,
      launchId: opts.launchId,
      trust,
      trustOption: this.m.trustOption,
      trustLevel: this.m.trustLevel,
      model: binding.model,
      sessionFile: mode.mode === 'resume' ? mode.sessionFile : undefined,
      forkRef: mode.mode === 'fork' ? mode.forkRef : undefined,
    });
    // PERSISTENT PANE (OpenRig seat model): the window outlives the runner.
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
    // override a live sidecar. Negative markers first (OpenRig ordering).
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
  return null;
}
