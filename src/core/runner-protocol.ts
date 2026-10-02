// flock-runner protocol: the shared PURE contract between the pane-hosted
// runner (runner.ts) and core. No side effects — constants, builders, parsers
// — so tests can assert it hermetically (borrowed from OpenRig's
// pi-runner-protocol design).
//
// What the runner does (in the pod's tmux pane):
//   pane stdin (flockmsg / human typing)  -> pi RPC prompt / steer / follow_up
//   pi RPC events (typed JSONL)           -> (a) human-readable pane mirror
//                                            (b) runner-state.json sidecar
//                                            (c) activity.jsonl (durable audit)
// Core reads ONLY runner-authored surfaces (sidecar, markers) — never
// screen-scrapes the TUI.

import path from 'node:path';

export const RUNNER_READY_MARKER = '[flock-runner] READY';
export const RUNNER_EXIT_MARKER = '[flock-runner] EXITED';
export const RUNNER_ERROR_MARKER = '[flock-runner] ERROR';

// Delivery framing: core pastes `flockmsg <base64>` + Enter. One wire line
// = one message block, multi-line safe (improvement over TTY paste
// semantics: tmux paste of raw newlines would submit each line separately).
export const FLOCKMSG_PREFIX = 'flockmsg';
export function frameMessage(text: string): string {
  return `${FLOCKMSG_PREFIX} ${Buffer.from(text, 'utf8').toString('base64')}`;
}
export function unframeMessage(line: string): string | null {
  if (!line.startsWith(`${FLOCKMSG_PREFIX} `)) return null;
  try {
    return Buffer.from(line.slice(FLOCKMSG_PREFIX.length + 1), 'base64').toString('utf8');
  } catch {
    return null;
  }
}

export interface SeatPaths {
  seatRoot: string;
  agentDir: string; // PI_CODING_AGENT_DIR (config isolation)
  sessionsDir: string; // PI_CODING_AGENT_SESSION_DIR (per-pod session store)
  runnerStatePath: string; // typed sidecar
  activityPath: string; // durable typed activity log (audit)
}

export function seatPaths(stateRoot: string, role: string): SeatPaths {
  const seatRoot = path.join(stateRoot, 'pods', role);
  return {
    seatRoot,
    agentDir: path.join(seatRoot, '.pi', 'agent'),
    sessionsDir: path.join(seatRoot, '.pi', 'sessions'),
    runnerStatePath: path.join(seatRoot, '.pi', 'runner-state.json'),
    activityPath: path.join(seatRoot, '.pi', 'activity.jsonl'),
  };
}

export interface RunnerState {
  ready: boolean;
  launchId: string;
  updatedAt: string;
  sessionFile?: string;
  sessionId?: string;
  streaming?: boolean; // typed busy flag (improvement: arbiter/health use it)
  lastPrompt?: { text: string; at: string }; // delivery ack (improvement over fire-and-forget)
  exited?: { code: number | null; signal?: string | null; at: string };
}

// Pending record for a new launch attempt: scoped by launchId, so stale
// ready/exited records from prior instances are ignored by readers.
export function buildPendingState(launchId: string, updatedAt: string): RunnerState {
  return { ready: false, launchId, updatedAt };
}

export function parseRunnerState(raw: string): RunnerState | null {
  try {
    const s = JSON.parse(raw) as RunnerState;
    if (typeof s !== 'object' || s === null || typeof s.ready !== 'boolean' || typeof s.launchId !== 'string' || typeof s.updatedAt !== 'string') {
      return null;
    }
    return s;
  } catch {
    return null;
  }
}

// ── Child env: deny-by-default allowlist (OpenRig BR-3 analogue). Only
// baseline vars + flock identity cross the boundary; config isolation via
// PI_CODING_AGENT_DIR / PI_CODING_AGENT_SESSION_DIR; provider key only if
// the model's declared provider needs one (env, not copied files).
export const ENV_BASELINE = ['PATH', 'HOME', 'TERM', 'LANG', 'LC_ALL', 'SHELL', 'TMPDIR'];
const FLOCK_VARS = ['FLOCK_HOME', 'FLOCK_PORT', 'FLOCK_POD_ROLE'];

export function buildPiChildEnv(
  source: NodeJS.ProcessEnv,
  opts: { agentDir: string; sessionsDir: string; trustFile?: string },
): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const name of [...ENV_BASELINE, ...FLOCK_VARS]) {
    if (source[name] !== undefined) env[name] = source[name];
  }
  env.PI_CODING_AGENT_DIR = opts.agentDir;
  env.PI_CODING_AGENT_SESSION_DIR = opts.sessionsDir;
  if (opts.trustFile) env.PI_SANDBOX_TRUST_FILE = opts.trustFile;
  return env;
}

// ── Command builders ─────────────────────────────────────────────────────────

export interface RunnerArgs {
  runnerPath: string; // dist/core/runner.js
  stateRoot: string; // FLOCK_HOME
  role: string;
  cwd: string;
  launchId: string;
  trust: 'approve' | 'no-approve';
  model?: string;
  sessionFile?: string; // exact-file resume
  forkRef?: string; // fork from a session file/id
  trustOption?: string; // fallback: option substring for an unexpected trust dialog
  trustLevel?: string; // sandbox trust level pre-seeded for the pod dir
}

// The command typed into the pod's tmux pane (shell-quoted).
export function buildRunnerCommand(o: RunnerArgs): string {
  const q = (s: string) => `'${s.replace(/'/g, `'\\''`)}'`;
  const parts = [
    'node',
    q(o.runnerPath),
    '--state-root', q(o.stateRoot),
    '--role', q(o.role),
    '--cwd', q(o.cwd),
    '--launch-id', q(o.launchId),
    o.trust === 'approve' ? '--approve' : '--no-approve',
  ];
  if (o.trustOption) parts.push('--trust-option', q(o.trustOption));
  if (o.trustLevel) parts.push('--trust-level', q(o.trustLevel));
  if (o.model) parts.push('--model', q(o.model));
  if (o.sessionFile) parts.push('--session-file', q(o.sessionFile));
  if (o.forkRef) parts.push('--fork-ref', q(o.forkRef));
  return parts.join(' ');
}

export interface PiChildArgs {
  sessionsDir: string;
  role: string;
  model?: string;
  trust: 'approve' | 'no-approve';
  sessionFile?: string;
  forkRef?: string;
  agentsMdPath?: string; // pod protocol file (loaded explicitly; --no-context-files)
}

// argv for the `pi --mode rpc` child the RUNNER spawns (no shell).
// - fresh: --session-id <role> — deterministic session per pod; relaunch =
//   memory.
// - resume (--session <file>) and fork (--fork <ref>) are MUTUALLY
//   INCOMPATIBLE with --session-id in pi 1.0.0 (verified live: "Error:
//   --session-id cannot be combined with --session"). The file/fork decides
//   the session; fork yields a NEW session file (uuid), resume keeps the
//   exact file — never an interactive picker.
// - --no-context-files + --append-system-prompt <pod AGENTS.md>: full
//   isolation from ancestor/home context files (improvement over OpenRig).
export function buildPiChildArgs(o: PiChildArgs): string[] {
  const args = [
    '--mode', 'rpc',
    '--session-dir', o.sessionsDir,
    o.trust === 'approve' ? '--approve' : '--no-approve',
    '--no-context-files',
  ];
  if (o.agentsMdPath) args.push('--append-system-prompt', o.agentsMdPath);
  if (o.model) args.push('--model', o.model);
  if (o.sessionFile) args.push('--session', o.sessionFile);
  else if (o.forkRef) args.push('--fork', o.forkRef);
  else args.push('--session-id', o.role);
  return args;
}

// ── Window launch command ─────────────────────────────────────────────────────
// The persistent shell pane (OpenRig seat model): the window outlives the
// runner; each launch is a new foreground process pasted into the pane with
// the env prefix (flock CLI on PATH, instance identity).
export function buildWindowLaunchCmd(
  cmd: string,
  o: { role: string; dir: string; home: string; port: string; basePath: string; extraEnv?: Record<string, string> },
): string {
  const env: Record<string, string> = {
    PATH: `${o.home}/bin:${o.dir}/bin:${o.basePath}`,
    FLOCK_HOME: o.home,
    FLOCK_PORT: o.port,
    FLOCK_POD_ROLE: o.role,
    ...(o.extraEnv ?? {}),
  };
  const prefix = Object.entries(env).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(' ');
  return `${prefix} ${cmd}`;
}

// ── Launch posture + resource trust ─────────────────────────────────────────
// pi --approve/--no-approve governs RESOURCE TRUST (context files), not a
// permission policy (that distinction is OpenRig's, we keep it). The resolved
// launch posture is authoritative: full_bypass forces full resource trust
// (their YOLO semantics); floor respects the configured value.
export type LaunchPosture = 'floor' | 'full_bypass';

export function resolveTrust(
  configured: 'approve' | 'no-approve' | undefined,
  posture: LaunchPosture | undefined,
): 'approve' | 'no-approve' {
  if (posture === 'full_bypass') return 'approve';
  // flock default: managed, per-pod-isolated pods -> approve (OpenRig's seat
  // default is no-approve; ours are sandboxes, not user projects).
  return configured ?? 'approve';
}

// ── Resume token = the persisted pi session file ────────────────────────────
// HONEST resume (OpenRig BR-6): relaunch with the exact file, never an
// interactive picker. A missing file is retry_fresh — the caller decides,
// never a silent fresh start.
const MAX_RESUME_TOKEN_LEN = 512;
const RESUME_TOKEN_CHARSET = /^[A-Za-z0-9._@/-]+$/;

export type ResumeTokenCheck = { ok: true; token: string } | { ok: false; error: string };

export function validateResumeToken(raw: unknown): ResumeTokenCheck {
  if (typeof raw !== 'string') return { ok: false, error: 'resume token is missing or not a string' };
  const token = raw.trim();
  if (token.length === 0) return { ok: false, error: 'resume token is empty' };
  if (token.length > MAX_RESUME_TOKEN_LEN) return { ok: false, error: `resume token too long (max ${MAX_RESUME_TOKEN_LEN})` };
  if (!token.startsWith('/')) return { ok: false, error: 'resume token must be an absolute path' };
  if (token.split('/').includes('..')) return { ok: false, error: 'resume token must not contain a ".." segment' };
  if (!RESUME_TOKEN_CHARSET.test(token)) return { ok: false, error: 'resume token has disallowed characters' };
  if (!token.endsWith('.jsonl')) return { ok: false, error: 'resume token must end with ".jsonl"' };
  return { ok: true, token };
}

// ── Fork source ─────────────────────────────────────────────────────────────
// v1: kind "native_id" only (parent session file path or session id),
// as in OpenRig's pi adapter. The captured resumeToken after a fork is the
// NEW post-fork session, never the parent's (the adapter enforces this).
export interface ForkSource {
  kind: 'native_id' | 'artifact_path' | 'name' | 'last';
  value?: string;
}

export type LaunchMode =
  | { mode: 'fresh' }
  | { mode: 'resume'; sessionFile: string }
  | { mode: 'fork'; forkRef: string }
  | { mode: 'error'; error: string; recovery?: 'retry_fresh' };

// The fresh/resume/fork decision — pure, testable. resumeToken and
// forkSource are mutually exclusive: the adapter refuses, never guesses.
export function resolveLaunchMode(opts: { resumeToken?: string; forkSource?: ForkSource }): LaunchMode {
  const { resumeToken, forkSource } = opts;
  if (resumeToken && forkSource) {
    return { mode: 'error', error: 'resumeToken and forkSource are mutually exclusive — pick one' };
  }
  if (resumeToken) {
    const v = validateResumeToken(resumeToken);
    if (!v.ok) return { mode: 'error', error: `pi resume: ${v.error}`, recovery: 'retry_fresh' };
    return { mode: 'resume', sessionFile: v.token };
  }
  if (forkSource) {
    if (forkSource.kind !== 'native_id') {
      return { mode: 'error', error: `pi fork: kind "${forkSource.kind}" is not supported in v1; use kind "native_id" with the parent session file path` };
    }
    const ref = forkSource.value?.trim();
    if (!ref) return { mode: 'error', error: 'pi fork: value is required (parent session file path)' };
    return { mode: 'fork', forkRef: ref };
  }
  return { mode: 'fresh' };
}
