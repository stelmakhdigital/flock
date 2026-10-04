// codex runtime protocol: the shared PURE contract between the pane-hosted
// codex bridge (codex-bridge.ts), the CodexRuntimeAdapter and the in-core
// OpenAI-responses shim. No side effects — constants, builders, parsers —
// so tests can assert it hermetically.
//
// Why a bridge (like the pi runner)? The codex TUI is a client of a shared
// app-server daemon (observed live: C-c "disconnects" the task, work keeps
// running in the daemon; trust persistence can break across daemon restarts).
// `codex exec` is standalone and typed: one process per turn, JSONL events on
// stdout, a thread_id that `exec resume <id>` / `exec fork <id>` continue.
// The bridge keeps a thread alive across turns and exposes the same typed
// surfaces the core reads for pi (sidecar, markers, activity log).
//
// Why a shim? codex speaks the OpenAI /v1/responses API but sends its
// system prompt as a `developer`-role message inside `input`. vLLM's
// /v1/responses accepts `instructions` but REJECTS the developer role
// (observed live: 400 "Unexpected message role."). The shim (in-core,
// 127.0.0.1) merges developer messages into `instructions` and proxies the
// rest verbatim.

// codex thread ids are UUID v7 (observed: 01a1059d-5279-77b1-a79f-...).
const CODEX_THREAD_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export type CodexTokenCheck = { ok: true; token: string } | { ok: false; error: string };

export function validateCodexSessionToken(raw: unknown): CodexTokenCheck {
  if (typeof raw !== 'string') return { ok: false, error: 'resume token is missing or not a string' };
  const token = raw.trim();
  if (token.length === 0) return { ok: false, error: 'resume token is empty' };
  if (!CODEX_THREAD_RE.test(token)) return { ok: false, error: `not a codex thread id (UUID): ${token}` };
  return { ok: true, token: token.toLowerCase() };
}

// The in-core shim port: FLOCK_PORT + 11 (profiles already shift FLOCK_PORT,
// so shim ports do not collide across flock instances).
export function codexShimPort(port?: number): number {
  return (port ?? Number(process.env.FLOCK_PORT ?? 7460)) + 11;
}

// The per-pod CODEX_HOME: full config+sessions isolation per pod (and it
// keeps the daemon state pod-scoped).
export function codexHome(seatRoot: string): string {
  return `${seatRoot}/.codex`;
}

export interface CodexConfigOpts {
  model?: string; // unset: codex picks its default (local deploys pass one)
  shimPort: number;
  keyEnv?: string; // env var name holding the API key (vLLM: a dummy)
}

// Per-pod config.toml. The provider ALWAYS points at the in-core shim —
// the pod never talks to the upstream directly (the developer-role merge
// must happen for every request, including resume/fork continuations).
export function buildCodexConfig(o: CodexConfigOpts): string {
  const keyEnv = o.keyEnv ?? 'FLOCK_VLLM_KEY';
  return [
    ...(o.model ? [`model = "${o.model}"`] : []),
    `model_provider = "flock-vllm"`,
    `approval_policy = "never"`,
    `sandbox_mode = "danger-full-access"`,
    ``,
    `[model_providers.flock-vllm]`,
    `name = "flock-vllm"`,
    `base_url = "http://127.0.0.1:${o.shimPort}/v1"`,
    `wire_api = "responses"`,
    `env_key = "${keyEnv}"`,
    ``,
  ].join('\n');
}

// ---- codex exec JSONL events (observed live against codex-cli 0.159) -------
//
//   {"type":"thread.started","thread_id":"..."}
//   {"type":"item.completed","item":{"type":"agent_message","text":"..."}}
//   {"type":"item.completed","item":{"type":"error","message":"..."}}
//   {"type":"item.completed","item":{"type":"command_execution",...}}
//   {"type":"turn.started"}
//   {"type":"turn.completed","usage":{input_tokens,cached_input_tokens,
//                                      cache_write_input_tokens,output_tokens,
//                                      reasoning_output_tokens}}
//   {"type":"turn.failed","error":{...}}
//   {"type":"error","message":"..."}
// Non-JSON lines (PATH-alias warnings, "Reading additional input from stdin")
// are noise: mirrored verbatim, never fatal.

export type CodexEvent =
  | { kind: 'thread'; threadId: string }
  | { kind: 'turn_start' }
  | { kind: 'turn_complete'; usage?: { input: number; output: number; cacheRead: number; cacheWrite: number } }
  | { kind: 'turn_failed'; error: string }
  | { kind: 'agent_message'; text: string }
  | { kind: 'tool'; name: string; failed?: boolean }
  | { kind: 'error'; message: string }
  | { kind: 'noise'; text: string };

function asStr(v: unknown): string | undefined {
  return typeof v === 'string' && v ? v : undefined;
}

export function parseCodexEvent(rawLine: string): CodexEvent {
  const line = rawLine.trim();
  if (!line) return { kind: 'noise', text: '' };
  let d: Record<string, unknown>;
  try {
    const p = JSON.parse(line);
    if (typeof p !== 'object' || p === null || Array.isArray(p)) return { kind: 'noise', text: line };
    d = p;
  } catch {
    return { kind: 'noise', text: line };
  }
  switch (d.type) {
    case 'thread.started': {
      const threadId = asStr(d.thread_id) ?? asStr(d.threadId);
      return threadId ? { kind: 'thread', threadId } : { kind: 'noise', text: line };
    }
    case 'turn.started':
      return { kind: 'turn_start' };
    case 'turn.completed': {
      const u = (d.usage ?? {}) as Record<string, unknown>;
      const num = (v: unknown) => (typeof v === 'number' && Number.isFinite(v) ? v : 0);
      const usage = {
        input: num(u.input_tokens),
        output: num(u.output_tokens),
        cacheRead: num(u.cached_input_tokens),
        cacheWrite: num(u.cache_write_input_tokens),
      };
      return { kind: 'turn_complete', usage: usage.input + usage.output > 0 ? usage : undefined };
    }
    case 'turn.failed': {
      const e = d.error as Record<string, unknown> | string | undefined;
      const message = typeof e === 'string' ? e : asStr(e?.message) ?? 'turn failed';
      return { kind: 'turn_failed', error: message };
    }
    case 'item.completed': {
      const item = (d.item ?? {}) as Record<string, unknown>;
      if (item.type === 'agent_message') return { kind: 'agent_message', text: asStr(item.text) ?? '' };
      if (item.type === 'error') return { kind: 'error', message: asStr(item.message) ?? 'codex item error' };
      if (item.type === 'command_execution') {
        // command_execution items carry the executed command in output; the
        // mirror line just needs the tool name (observed shape: no stable
        // single field across versions — keep it minimal)
        return { kind: 'tool', name: 'command' };
      }
      return { kind: 'noise', text: line };
    }
    case 'error':
      return { kind: 'error', message: asStr(d.message) ?? 'codex error' };
    default:
      return { kind: 'noise', text: line };
  }
}

// ---- the shim: request rewrite ---------------------------------------------
// Merge `developer`-role messages out of `input` into `instructions`
// (vLLM accepts instructions, rejects the developer role in input).
// Returns { body, changed } — a non-JSON or developer-free body passes
// through byte-for-byte (changed: false).

export function rewriteResponsesRequest(rawBody: string): { body: string; changed: boolean } {
  let d: Record<string, unknown>;
  try {
    d = JSON.parse(rawBody) as Record<string, unknown>;
    if (typeof d !== 'object' || d === null || Array.isArray(d)) return { body: rawBody, changed: false };
  } catch {
    return { body: rawBody, changed: false };
  }
  const input = Array.isArray(d.input) ? (d.input as unknown[]) : [];
  const dev: string[] = [];
  const kept: unknown[] = [];
  let changed = false;
  for (const m of input) {
    if (m !== null && typeof m === 'object' && (m as Record<string, unknown>).role === 'developer') {
      const c = (m as Record<string, unknown>).content;
      const text = typeof c === 'string' ? c : Array.isArray(c)
        ? c.filter((x): x is Record<string, unknown> => x !== null && typeof x === 'object').map((x) => asStr(x.text) ?? '').join(' ')
        : '';
      if (text) dev.push(text);
      changed = true;
    } else {
      kept.push(m);
    }
  }
  if (!changed) return { body: rawBody, changed: false };
  d.input = kept;
  d.instructions = `${asStr(d.instructions) ?? ''}\n\n${dev.join('\n\n')}`.replace(/^\n\n+/, '');
  return { body: JSON.stringify(d), changed: true };
}

// ---- bridge command + child env ---------------------------------------------

export interface CodexBridgeArgs {
  bridgePath: string; // dist/core/codex-bridge.js
  stateRoot: string; // FLOCK_HOME
  role: string;
  cwd: string;
  launchId: string;
  command: string; // the codex binary (manifest command)
  shimPort: number;
  model?: string;
  resumeThread?: string; // thread id to continue (honest resume)
  forkRef?: string; // thread id to fork (new thread, parent context)
  keyEnv?: Record<string, string>; // extra env for the codex child (K=V)
}

export const CODEX_BRIDGE_READY_MARKER = '[flock-codex] READY';

// The one-line command pasted into the pod's persistent pane.
export function buildCodexBridgeCommand(o: CodexBridgeArgs): string {
  const q = (s: string) => `'${s.replace(/'/g, `'\\''`)}'`;
  const parts = [
    'node',
    q(o.bridgePath),
    '--state-root', q(o.stateRoot),
    '--role', q(o.role),
    '--cwd', q(o.cwd),
    '--launch-id', q(o.launchId),
    '--command', q(o.command),
    '--shim-port', String(o.shimPort),
  ];
  if (o.model) parts.push('--model', q(o.model));
  if (o.resumeThread) parts.push('--resume-thread', q(o.resumeThread));
  if (o.forkRef) parts.push('--fork-ref', q(o.forkRef));
  for (const [k, v] of Object.entries(o.keyEnv ?? {})) parts.push('--env', q(k), q(v));
  return parts.join(' ');
}

// The codex child env: baseline + the pod's CODEX_HOME + the provider key.
// Same deny-by-default principle as buildPiChildEnv (pi vars excluded —
// codex does not read them).
export const CODEX_CHILD_ENV_BASELINE = ['PATH', 'HOME', 'TERM', 'LANG', 'LC_ALL', 'SHELL', 'TMPDIR'];
const CODEX_FLOCK_VARS = ['FLOCK_HOME', 'FLOCK_PORT', 'FLOCK_POD_ROLE'];

export function buildCodexChildEnv(
  source: NodeJS.ProcessEnv,
  o: { codexHome: string; keyEnv?: Record<string, string> },
): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = {};
  for (const name of [...CODEX_CHILD_ENV_BASELINE, ...CODEX_FLOCK_VARS]) {
    if (source[name] !== undefined) env[name] = source[name];
  }
  for (const [k, v] of Object.entries(o.keyEnv ?? {})) env[k] = v;
  env.CODEX_HOME = o.codexHome;
  return env;
}
