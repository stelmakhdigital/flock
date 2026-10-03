// flock-runner protocol: the shared PURE contract between the pane-hosted
// runner (runner.ts) and core. No side effects — constants, builders, parsers
// — so tests can assert it hermetically.
//
// What the runner does (in the pod's tmux pane):
//   pane stdin (flockmsg / human typing)  -> pi RPC prompt / steer / follow_up
//   pi RPC events (typed JSONL)           -> (a) human-readable pane mirror
//                                            (b) runner-state.json sidecar
//                                            (c) activity.jsonl (durable audit)
// Core reads ONLY runner-authored surfaces (sidecar, markers) — never
// screen-scrapes the TUI.

import fs from 'node:fs';
import path from 'node:path';

export const RUNNER_READY_MARKER = '[flock-runner] READY';
export const RUNNER_EXIT_MARKER = '[flock-runner] EXITED';
export const RUNNER_ERROR_MARKER = '[flock-runner] ERROR';

// Delivery framing: core pastes `flockmsg <base64>` + Enter. One wire line
// = one message block, multi-line safe (improvement over TTY paste
// semantics: tmux paste of raw newlines would submit each line separately).
// v2 adds a nonce: the ack is a nonce match, so a REPEATED identical message
// can never false-positive against the previous one's ack.
export const FLOCKMSG_PREFIX = 'flockmsg';
export const FLOCKMSG_V2_PREFIX = 'flockmsg v2';

export function newNonce(): string {
  return Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
}

export function frameMessage(text: string, nonce?: string): string {
  if (nonce === undefined) {
    return `${FLOCKMSG_PREFIX} ${Buffer.from(text, 'utf8').toString('base64')}`;
  }
  return `${FLOCKMSG_V2_PREFIX} ${Buffer.from(JSON.stringify({ n: nonce, t: text }), 'utf8').toString('base64')}`;
}

export interface FramedMessage {
  text: string;
  nonce?: string;
}

export function unframeMessage(line: string): FramedMessage | null {
  // v2 first: a v2 line also starts with the v1 prefix
  if (line.startsWith(`${FLOCKMSG_V2_PREFIX} `)) {
    try {
      const obj = JSON.parse(Buffer.from(line.slice(FLOCKMSG_V2_PREFIX.length + 1), 'base64').toString('utf8')) as { n?: string; t?: string };
      if (typeof obj.t !== 'string') return null;
      return { text: obj.t, nonce: typeof obj.n === 'string' ? obj.n : undefined };
    } catch {
      return null;
    }
  }
  if (line.startsWith(`${FLOCKMSG_PREFIX} `)) {
    try {
      return { text: Buffer.from(line.slice(FLOCKMSG_PREFIX.length + 1), 'base64').toString('utf8') };
    } catch {
      return null;
    }
  }
  return null;
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
  lastPrompt?: { nonce?: string; text: string; at: string }; // delivery ack (nonce-match in v2)
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

// ── Child env: deny-by-default allowlist. Only
// baseline vars + flock identity cross the boundary; config isolation via
// PI_CODING_AGENT_DIR / PI_CODING_AGENT_SESSION_DIR; provider key only if
// the model's declared provider needs one (env, not copied files).
export const ENV_BASELINE = ['PATH', 'HOME', 'TERM', 'LANG', 'LC_ALL', 'SHELL', 'TMPDIR'];
const FLOCK_VARS = ['FLOCK_HOME', 'FLOCK_PORT', 'FLOCK_POD_ROLE'];

export function buildPiChildEnv(
  source: NodeJS.ProcessEnv,
  opts: { agentDir: string; sessionsDir: string; trustFile?: string; extra?: Record<string, string> },
): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const name of [...ENV_BASELINE, ...FLOCK_VARS]) {
    if (source[name] !== undefined) env[name] = source[name];
  }
  // flock-managed pod env (manifest env, e.g. BASH_GUARD_AUTO_ALLOW for
  // worktree pods): wins over the operator's own shell environment
  for (const [k, v] of Object.entries(opts.extra ?? {})) env[k] = v;
  env.PI_CODING_AGENT_DIR = opts.agentDir;
  env.PI_CODING_AGENT_SESSION_DIR = opts.sessionsDir;
  if (opts.trustFile) env.PI_SANDBOX_TRUST_FILE = opts.trustFile;
  return env;
}

// ── Command builders ─────────────────────────────────────────────────────────

export interface PiChildConfig {
  // pi first-class config axes (T1) — carried to the runner in one JSON flag
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
}

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
  extraEnv?: string[]; // flock-managed env for the pi child (K=V, wins over operator shell)
  pi?: PiChildConfig; // first-class pi axes (serialized as one --pi-config JSON flag)
}

export const RUNNER_PI_CONFIG_FLAG = '--pi-config';

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
  for (const kv of o.extraEnv ?? []) {
    const i = kv.indexOf('=');
    if (i > 0) parts.push('--env', q(kv.slice(0, i)), q(kv.slice(i + 1)));
  }
  // T1: the whole pi config block in ONE JSON flag — the free-form runner
  // format grows by exactly one flag, no matter how many axes appear.
  if (o.pi && Object.keys(o.pi).length > 0) parts.push(RUNNER_PI_CONFIG_FLAG, q(JSON.stringify(o.pi)));
  return parts.join(' ');
}

// Parse (and validate) the --pi-config JSON flag back into a PiChildConfig.
// Unknown/extra keys are dropped; the runner must never pass garbage to pi.
export function parsePiConfig(raw: string | undefined): PiChildConfig {
  if (!raw) return {};
  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return {}; // corrupt flag: degrade to defaults, pi still launches
  }
  if (typeof obj !== 'object' || obj === null) return {};
  const str = (v: unknown) => (typeof v === 'string' ? v : undefined);
  const strArr = (v: unknown) => (Array.isArray(v) && v.every((x) => typeof x === 'string') ? (v as string[]) : undefined);
  const bool = (v: unknown) => (typeof v === 'boolean' ? v : undefined);
  return {
    thinking: str(obj.thinking),
    tools: strArr(obj.tools),
    excludeTools: strArr(obj.excludeTools),
    skills: strArr(obj.skills),
    noSkills: bool(obj.noSkills),
    extensions: strArr(obj.extensions),
    noExtensions: bool(obj.noExtensions),
    systemPrompt: str(obj.systemPrompt),
    appendSystemPrompt: strArr(obj.appendSystemPrompt),
    noContextFiles: bool(obj.noContextFiles),
  };
}

export interface PiChildArgs {
  autoAllow?: boolean; // worktree pods: --bash-guard-auto-allow
  sessionsDir: string;
  role: string;
  model?: string;
  trust: 'approve' | 'no-approve';
  sessionFile?: string;
  forkRef?: string;
  agentsMdPath?: string; // pod protocol file (loaded explicitly; --no-context-files)
  // T1: first-class pi config axes (from --pi-config)
  pi?: PiChildConfig;
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
//   isolation from ancestor/home context files.
// - T1 axes map 1:1 to pi flags (verified against `pi --help`):
//   thinking -> --thinking; tools/excludeTools -> --tools/--exclude-tools
//   (comma-joined); skills/extensions/appendSystemPrompt -> repeatable
//   --skill/--extension/--append-system-prompt; noSkills/noExtensions/
//   noContextFiles -> --no-skills/--no-extensions/--no-context-files;
//   systemPrompt -> --system-prompt.
export function buildPiChildArgs(o: PiChildArgs): string[] {
  const args = [
    '--mode', 'rpc',
    '--session-dir', o.sessionsDir,
    o.trust === 'approve' ? '--approve' : '--no-approve',
    '--no-context-files',
    // worktree pods: routine git (commit/pull) is the job — bash-guard's
    // interactive prompt is a no-op in RPC mode, so run autonomous
    // (--bash-guard-disabled: its hard floor still blocks rm -rf /
    // reset --hard / push --force)
    ...(o.autoAllow ? ['--bash-guard-disabled'] : []),
  ];
  const pi = o.pi ?? {};
  if (pi.thinking) args.push('--thinking', pi.thinking);
  if (pi.tools?.length) args.push('--tools', pi.tools.join(','));
  if (pi.excludeTools?.length) args.push('--exclude-tools', pi.excludeTools.join(','));
  for (const s of pi.skills ?? []) args.push('--skill', s);
  if (pi.noSkills) args.push('--no-skills');
  for (const e of pi.extensions ?? []) args.push('--extension', e);
  if (pi.noExtensions) args.push('--no-extensions');
  if (pi.systemPrompt) args.push('--system-prompt', pi.systemPrompt);
  for (const a of pi.appendSystemPrompt ?? []) args.push('--append-system-prompt', a);
  if (pi.noContextFiles) args.push('--no-context-files');
  if (o.agentsMdPath) args.push('--append-system-prompt', o.agentsMdPath);
  if (o.model) args.push('--model', o.model);
  if (o.sessionFile) args.push('--session', o.sessionFile);
  else if (o.forkRef) args.push('--fork', o.forkRef);
  else args.push('--session-id', o.role);
  return args;
}

// ── Window launch command ─────────────────────────────────────────────────────
// The persistent shell pane: the window outlives the runner; each launch is a
// new foreground process pasted into the pane with env prefix (flock CLI on
// PATH, instance identity) + the runtime command. One line, JSON-quoted.
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
// permission policy. The resolved launch posture is authoritative: full_bypass forces full resource trust
// (their YOLO semantics); floor respects the configured value.
export type LaunchPosture = 'floor' | 'full_bypass';

export function resolveTrust(
  configured: 'approve' | 'no-approve' | undefined,
  posture: LaunchPosture | undefined,
): 'approve' | 'no-approve' {
  if (posture === 'full_bypass') return 'approve';
  // flock default: managed, per-pod-isolated pods -> approve. Our pods are
  // sandboxes, not user projects, so the floor trusts their own context.
  return configured ?? 'approve';
}

// ── Resume token = the persisted pi session file ────────────────────────────
// HONEST resume: relaunch with the exact file, never an
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
// v1: kind "native_id" only (parent session file path or session id).
// The captured resumeToken after a fork is the NEW post-fork session,
// never the parent's (the adapter enforces this).
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

// ---------------------------------------------------------------------------
// Extension dialogs (permission gates) in RPC mode
//
// pi RPC mode: blocking dialogs (select/confirm/input/editor) are emitted as
// extension_ui_request and wait for the client's extension_ui_response —
// INDEFINITELY when the extension passes no timeout. The pod pane is a text
// mirror (no TUI), so the operator answers via a pane line: `/answer <arg>`.
// `custom` dialogs (bash-guard, ask-user-question) are no-ops in RPC mode and
// never block (pi resolves them to undefined; extensions default to abort).

export interface PendingDialog {
  id: string;
  index: number; // 1-based, per runner lifetime
  method: 'select' | 'confirm' | 'input' | 'editor';
  title: string;
  options?: string[]; // select
  at: string; // ISO — when the request arrived
}

export type AnswerArg = { kind: 'index'; n: number } | { kind: 'value'; v: string };

/** Parse an operator answer line: `/answer 2` | `/answer run` | `/answer any text`. */
export function parseAnswerLine(line: string): AnswerArg | null {
  const m = line.trim().match(/^\/answer(?:\s+(.*))?$/);
  if (!m) return null;
  const rest = (m[1] ?? '').trim();
  if (!rest) return { kind: 'index', n: 1 }; // bare /answer = first option / yes
  if (/^\d+$/.test(rest)) return { kind: 'index', n: parseInt(rest, 10) };
  return { kind: 'value', v: rest };
}

/**
 * Build the extension_ui_response payload for a pending dialog.
 * pi response shapes (from rpc-mode parseResponse): select/input/editor carry
 * {value} (or {cancelled}), confirm carries {confirmed}. Returns null when the
 * argument does not match (bad index, no such option).
 */
export function dialogResponse(dialog: PendingDialog, arg: AnswerArg): { value?: string; confirmed?: boolean } | null {
  if (dialog.method === 'select') {
    const opts = dialog.options ?? [];
    if (arg.kind === 'index') {
      const o = opts[arg.n - 1];
      return o !== undefined ? { value: o } : null;
    }
    const exact = opts.find((x) => x.toLowerCase() === arg.v.toLowerCase());
    if (exact !== undefined) return { value: exact };
    // unambiguous substring match (options often carry "name — description")
    const partial = opts.filter((x) => x.toLowerCase().includes(arg.v.toLowerCase()));
    return partial.length === 1 ? { value: partial[0] } : null;
  }
  if (dialog.method === 'confirm') {
    if (arg.kind === 'index') return { confirmed: arg.n === 1 };
    return { confirmed: !/^(no|нет|abort|cancel)$/i.test(arg.v) };
  }
  // input / editor: free text (index form = the value of /n/ is the text itself)
  return { value: arg.kind === 'value' ? arg.v : '' };
}

// ── Typed activity log (pure) ───────────────────────────────────────────────
// The pod's durable activity log (seat activityPath): one JSON event per line
// (ext_dialog_unanswered/answered, prompts, …). Parsed here — pure, so the
// adapters (healthProbe) and the core (health) share ONE reader. (Moved out
// of health.ts for the runtime-agnostic refactor: health is a CORE consumer
// of the probes, not the place where the protocol lives.)

export interface ActivityLine {
  at?: string;
  event?: string;
  id?: string;
  via?: string;
  method?: string;
  title?: string;
  [k: string]: unknown;
}

// Parse (tail of) the activity log: newest last, non-JSON lines skipped.
export function parseActivity(raw: string, maxLines = 400): ActivityLine[] {
  const lines = raw.split('\n').filter(Boolean).slice(-maxLines);
  const out: ActivityLine[] = [];
  for (const l of lines) {
    try {
      out.push(JSON.parse(l) as ActivityLine);
    } catch {
      /* not json — skip */
    }
  }
  return out;
}

// Read + parse the pod's activity log. Missing file = [].
export function readActivity(stateRoot: string, role: string, maxLines = 2000): ActivityLine[] {
  let raw: string;
  try {
    raw = fs.readFileSync(seatPaths(stateRoot, role).activityPath, 'utf8');
  } catch {
    return [];
  }
  return parseActivity(raw, maxLines);
}

// The currently-open dialog gate: any ext_dialog_unanswered without an
// ext_dialog_answered for the same id after it; the most recent one wins.
export function detectGate(activity: ActivityLine[]): ActivityLine | null {
  const openIdx = new Map<string, number>(); // dialog id -> index of its unanswered event
  activity.forEach((a, i) => {
    if (a.event === 'ext_dialog_unanswered' && typeof a.id === 'string') openIdx.set(a.id, i);
    else if (a.event === 'ext_dialog_answered' && typeof a.id === 'string') openIdx.delete(a.id);
  });
  let best: ActivityLine | null = null;
  let bestIdx = -1;
  for (const [id, idx] of openIdx) {
    if (idx > bestIdx) {
      bestIdx = idx;
      best = activity[idx];
    }
  }
  return best;
}

// The launchId of the run a sidecar state belongs to (from the 'created'
// run-meta entry). Pure: runkeeper + the pi adapter's liveness both need it.
export function runLaunchId(run: { meta?: string | null }): string | null {
  try {
    const arr = JSON.parse(run.meta || '[]');
    if (!Array.isArray(arr)) return null;
    const created = arr.find((e: unknown) => (e as { kind?: string })?.kind === 'created');
    return created && typeof (created as { launchId?: string }).launchId === 'string' ? (created as { launchId: string }).launchId : null;
  } catch {
    return null;
  }
}
