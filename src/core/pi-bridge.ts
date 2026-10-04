// flock-runner — pane-hosted bridge for the pi runtime. Spawns `pi --mode rpc`,
// mirrors a human-readable transcript to the pane, keeps a typed sidecar
// (runner-state.json) + a durable activity log (activity.jsonl), and forwards
// pane stdin to pi RPC (prompt / steer / follow_up / abort).
//
// Node builtins + runner-protocol only: runnable standalone as
//   node dist/core/runner.js --state-root ... --role ... --cwd ... --launch-id ...
// so it stays importable by tests without core dependencies.

import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import { spawn } from 'node:child_process';
import {
  seatPaths,
  buildPendingState,
  parseRunnerState,
  buildPiChildEnv,
  buildPiChildArgs,
  parseChildArgs,
  unframeMessage,
  RUNNER_READY_MARKER,
  RUNNER_EXIT_MARKER,
  RUNNER_ERROR_MARKER,
  type RunnerState,
  strictestOption,
} from './bridge-protocol.js';

interface ParsedArgs {
  stateRoot: string;
  role: string;
  cwd: string;
  launchId: string;
  trust: 'approve' | 'no-approve';
  model?: string;
  sessionFile?: string;
  forkRef?: string;
  trustOption: string;
  trustLevel: string;
  extraEnv: Record<string, string>;
  childArgs: ReturnType<typeof parseChildArgs>;
}

function parseArgs(argv: string[]): ParsedArgs {
  const get = (name: string) => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  const args: ParsedArgs = {
    stateRoot: get('--state-root') ?? '',
    role: get('--role') ?? '',
    cwd: get('--cwd') ?? process.cwd(),
    launchId: get('--launch-id') ?? '',
    trust: argv.includes('--no-approve') ? 'no-approve' : 'approve',
    model: get('--model'),
    sessionFile: get('--session-file'),
    forkRef: get('--fork-ref'),
    trustOption: get('--trust-option') ?? 'untrusted',
    trustLevel: get('--trust-level') ?? 'dev',
    extraEnv: Object.fromEntries(
      argv.flatMap((a, i) => (a === '--env' ? [[argv[i + 1], argv[i + 2]] as [string, string]] : [])),
    ) as Record<string, string>,
    // C10: raw child args/env + mapped pi axes arrive in ONE JSON flag
    childArgs: parseChildArgs(get('--child-args')),
  };
  if (!args.stateRoot || !args.role || !args.launchId) {
    throw new Error('missing required args: --state-root --role --launch-id');
  }
  return args;
}

const SHELL_QUOTE = (s: string) => `'${s.replace(/'/g, `'\\''`)}'`;

interface ActivityEvent {
  event: string;
  [k: string]: unknown;
}

export interface RunnerIO {
  mirrorLine: (line: string) => void;
  mirrorAppend: (text: string) => void;
  writeSidecar: (state: RunnerState) => void;
  appendActivity: (ev: ActivityEvent) => void;
  sendRpc: (cmd: unknown) => void;
  now: () => string;
}

const GET_STATE_ID = 'flock-get-state';

export class RunnerCore {
  private io: RunnerIO;
  private paths: ReturnType<typeof seatPaths>;
  private launchId: string;
  private ready = false;
  private streaming = false;
  private sessionFile: string | undefined;
  private sessionId: string | undefined;
  private exited = false;
  private trustOption: string;
  private lastPrompt: { nonce?: string; text: string; at: string } | undefined;
  // extension dialogs waiting for a client response (select/confirm/input/editor).
  // pi has no timeout — these block the agent forever until answered (/answer)
  // or the runner dies. Cap: newest 10.

  constructor(io: RunnerIO, paths: ReturnType<typeof seatPaths>, launchId: string, trustOption = 'untrusted') {
    this.io = io;
    this.paths = paths;
    this.launchId = launchId;
    this.trustOption = trustOption;
  }

  private baseState(): RunnerState {
    return {
      ready: this.ready,
      launchId: this.launchId,
      updatedAt: this.io.now(),
      // protocol marker: lets the core tell a pre-v2 (v1-only) runner apart
      // and refuse to paste v2 frames to it (binary garbage otherwise)
      bridge: 'v2',
      ...(this.sessionFile ? { sessionFile: this.sessionFile } : {}),
      ...(this.sessionId ? { sessionId: this.sessionId } : {}),
      streaming: this.streaming,
      ...(this.lastPrompt ? { lastPrompt: this.lastPrompt } : {}),
    };
  }

  start(): void {
    this.io.sendRpc({ type: 'get_state', id: GET_STATE_ID });
  }

  handlePiLine(rawLine: string): void {
    const line = rawLine.trim();
    if (!line) return;
    let record: Record<string, unknown>;
    try {
      const parsed = JSON.parse(line);
      if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return;
      record = parsed;
    } catch {
      this.io.mirrorLine(line); // non-JSON noise: mirror verbatim, nothing hides
      return;
    }
    if (record.type === 'response') {
      this.handleResponse(record);
      return;
    }
    this.handleEvent(record);
  }

  private handleResponse(record: Record<string, unknown>): void {
    const command = record.command;
    if (command === 'get_state' && record.success) {
      const data = (record.data ?? {}) as Record<string, unknown>;
      this.sessionFile = typeof data.sessionFile === 'string' ? data.sessionFile : undefined;
      this.sessionId = typeof data.sessionId === 'string' ? data.sessionId : undefined;
      this.streaming = data.isStreaming === true;
      if (!this.ready) {
        this.ready = true;
        this.io.writeSidecar(this.baseState());
        this.io.mirrorLine(RUNNER_READY_MARKER);
        this.io.appendActivity({ event: 'ready', sessionFile: this.sessionFile, sessionId: this.sessionId });
      }
      return;
    }
    if (record.success === false) {
      const error = typeof record.error === 'string' ? record.error : String(record.error ?? 'rpc error');
      this.io.mirrorLine(`${RUNNER_ERROR_MARKER} ${command}: ${error.split('\n')[0]}`);
      this.io.appendActivity({ event: 'rpc_error', command, error: error.slice(0, 300) });
    }
  }

  private setStreaming(v: boolean): void {
    if (this.streaming === v) return;
    this.streaming = v;
    this.io.writeSidecar(this.baseState());
  }

  private handleEvent(record: Record<string, unknown>): void {
    switch (record.type) {
      case 'agent_start':
        this.setStreaming(true);
        this.io.appendActivity({ event: 'agent_start' });
        break;
      case 'agent_end': {
        this.setStreaming(false);
        const stopReason = typeof record.stopReason === 'string' ? record.stopReason : undefined;
        if (stopReason === 'error') this.io.mirrorLine(`${RUNNER_ERROR_MARKER} agent ended with an error`);
        this.io.appendActivity({ event: 'agent_end', stopReason });
        break;
      }
      case 'message_update': {
        const ev = (record.assistantMessageEvent ?? {}) as Record<string, unknown>;
        if (ev.type === 'text_delta' && typeof ev.delta === 'string') {
          this.io.mirrorAppend(ev.delta);
        }
        break;
      }
      case 'message_end': {
        const message = (record.message ?? {}) as Record<string, unknown>;
        if (message.role === 'assistant') {
          this.io.mirrorLine(''); // terminate the streamed line
          if (message.stopReason === 'error') {
            const err = typeof message.errorMessage === 'string' ? message.errorMessage : 'assistant message error';
            this.io.mirrorLine(`${RUNNER_ERROR_MARKER} ${err.split('\n')[0]}`);
          }
          // usage: per-message tokens (pi carries them on the assistant
          // message) — the economy signal for cost per task/run/pipeline
          const u = message.usage as Record<string, number> | undefined;
          if (u && typeof u.input === 'number') {
            this.io.appendActivity({
              event: 'usage',
              input: u.input,
              output: u.output ?? 0,
              cacheRead: u.cacheRead ?? 0,
              cacheWrite: u.cacheWrite ?? 0,
              totalTokens: u.totalTokens ?? u.input + (u.output ?? 0),
              model: typeof message.model === 'string' ? message.model : undefined,
            });
          }
        }
        break;
      }
      case 'tool_execution_start': {
        const tool = (typeof record.toolName === 'string' && record.toolName) || (typeof record.name === 'string' && record.name) || 'tool';
        this.io.mirrorLine(`  ⚙ ${tool} …`);
        this.io.appendActivity({ event: 'tool_start', tool });
        break;
      }
      case 'tool_execution_end': {
        const tool = (typeof record.toolName === 'string' && record.toolName) || (typeof record.name === 'string' && record.name) || 'tool';
        const failed = record.isError === true || record.error != null;
        this.io.mirrorLine(`  ⚙ ${tool} ${failed ? 'FAILED' : 'done'}`);
        this.io.appendActivity({ event: 'tool_end', tool, failed });
        break;
      }
      case 'compaction_start':
        this.io.mirrorLine('[pi] compacting context…');
        this.io.appendActivity({ event: 'compaction_start' });
        break;
      case 'compaction_end':
        this.io.mirrorLine('[pi] compaction done');
        break;
      case 'auto_retry_start':
        this.io.mirrorLine('[pi] transient error — retrying');
        break;
      case 'auto_retry_end':
        if (record.success === false) {
          const err = typeof record.finalError === 'string' ? record.finalError : 'auto retry failed';
          this.io.mirrorLine(`${RUNNER_ERROR_MARKER} ${err.split('\n')[0]}`);
        }
        break;
      case 'extension_error': {
        const message = typeof record.message === 'string' ? record.message : 'extension error';
        this.io.mirrorLine(`${RUNNER_ERROR_MARKER} extension: ${message.split('\n')[0]}`);
        this.io.appendActivity({ event: 'extension_error', message: message.slice(0, 300) });
        break;
      }
      case 'extension_ui_request':
        this.handleExtensionUi(record as Record<string, unknown>);
        break;
      default:
        break;
    }
  }

  // Extension dialogs in RPC mode: trust dialog is auto-answered (configurable
  // option substring, default the strictest); any other dialog is tracked +
  // mirrored + logged and waits for the operator's `/answer` (stage 4.1 gate).
  private handleExtensionUi(record: Record<string, unknown>): void {
    const method = typeof record.method === 'string' ? record.method : '';
    const id = typeof record.id === 'string' ? record.id : '';
    const title = typeof record.title === 'string' ? record.title : '';
    const options = Array.isArray(record.options) ? (record.options as string[]) : [];
    const DIALOGS = new Set(['select', 'confirm', 'input', 'editor']);
    if (!DIALOGS.has(method)) {
      if (method === 'setStatus' && typeof record.statusKey === 'string') {
        this.io.mirrorLine(`[ext] ${record.statusKey}`);
        this.io.appendActivity({ event: 'ext_ui', method, statusKey: record.statusKey });
      } else if (method === 'notify' && typeof record.message === 'string') {
        this.io.appendActivity({ event: 'ext_notify', message: record.message.slice(0, 200), notifyType: record.notifyType });
      }
      // setWidget (per-second extension status chrome) is NOT an audit event:
      // it floods activity.jsonl and would shrink the health tail window,
      // dropping long-open dialogs out of gate detection.
      return;
    }
    if (method === 'select' && /доверя|trust/i.test(title) && id && options.length) {
      const wanted = this.trustOption.toLowerCase();
      const chosen = options.find((o) => o.toLowerCase().includes(wanted)) ?? options[options.length - 1];
      this.io.sendRpc({ type: 'extension_ui_response', id, value: chosen });
      this.io.mirrorLine(`[ext] trust → ${chosen}`);
      this.io.appendActivity({ event: 'ext_dialog_trust', title, chosen, id });
      this.io.appendActivity({ event: 'ext_dialog_answered', id, via: 'auto_trust' });
      return;
    }
    // C10: there is no operator /answer channel anymore — an unanswered
    // dialog would block the agent's turn FOREVER. The strictest option
    // (last = the deny path in pi's option ordering) is auto-answered and
    // the decision is mirrored + logged LOUDLY; the operator's real lever
    // is attaching to the pane (or fixing the trust config).
    const chosen = strictestOption(options);
    if (chosen !== null) this.io.sendRpc({ type: 'extension_ui_response', id, value: chosen });
    const optsHint = options.length ? ` [${options.map((o) => o).join(' | ')}]` : '';
    this.io.mirrorLine(`[ext] AUTO-DENIED (no operator channel; attach to the pane) [${method}] ${title.slice(0, 140)}${optsHint}${chosen !== null ? ` → ${chosen}` : ''}`);
    this.io.appendActivity({ event: 'ext_dialog_auto_denied', id, method, title: title.slice(0, 200), chosen: chosen ?? undefined });
  }

  handleUserBlock(rawBlock: string): void {
    const block = rawBlock.trim();
    if (!block) return;
    if (block === '/abort') {
      this.io.sendRpc({ type: 'abort' });
      this.io.mirrorLine('[flock-runner] abort sent');
      return;
    }
    if (block.startsWith('/followup ')) {
      const message = block.slice('/followup '.length).trim();
      if (message) {
        this.io.sendRpc({ type: 'follow_up', message });
        this.io.mirrorLine(`you (follow-up) ▸ ${firstLine(message)}`);
      }
      return;
    }
    const framed = unframeMessage(block);
    const text = framed?.text ?? rawBlock;
    const kind = this.streaming ? 'steer' : 'prompt';
    this.io.sendRpc({ type: kind, message: text });
    this.io.mirrorLine(`you ${kind === 'steer' ? '(steer) ' : ''}▸ ${firstLine(text)}`);
    // delivery ack: core verifies against this (survives streaming toggles);
    // nonce (v2 frame) makes the ack unambiguous for repeated messages
    this.lastPrompt = { text: text.slice(0, 2000), nonce: framed?.nonce, at: this.io.now() };
    this.io.writeSidecar(this.baseState());
    this.io.appendActivity({ event: kind, bytes: text.length });
  }

  handlePiExit(code: number | null, signal?: string | null): void {
    if (this.exited) return;
    this.exited = true;
    this.ready = false;
    this.streaming = false;
    this.io.mirrorLine(`${RUNNER_EXIT_MARKER} pi exited (code ${code ?? 'null'}${signal ? `, signal ${signal}` : ''})`);
    const state = this.baseState();
    state.exited = { code, ...(signal ? { signal } : {}), at: this.io.now() };
    this.io.writeSidecar(state);
    this.io.appendActivity({ event: 'pi_exit', code });
  }
}

const firstLine = (s: string) => (s.split('\n')[0] ?? '').slice(0, 200);

export async function main(argv: string[] = process.argv.slice(2)): Promise<void> {
  let args: ParsedArgs;
  try {
    args = parseArgs(argv);
  } catch (e) {
    console.error(`${RUNNER_ERROR_MARKER} ${e instanceof Error ? e.message : String(e)}`);
    process.exitCode = 2;
    return;
  }
  const paths = seatPaths(args.stateRoot, args.role);
  fs.mkdirSync(paths.agentDir, { recursive: true });
  fs.mkdirSync(paths.sessionsDir, { recursive: true });

  // Reset the sidecar for THIS launch (scoped by launchId); core only trusts
  // records stamped with the launchId it requested.
  fs.writeFileSync(paths.runnerStatePath, JSON.stringify(buildPendingState(args.launchId, new Date().toISOString())));

  const agentsMd = path.join(args.cwd, 'AGENTS.md');
  // C10: raw child.args (manifest `child` field) are appended LAST — they
  // are the operator's explicit flags and win by position over the mapped
  // axes; raw child.env is merged over the flock-managed env.
  const baseArgs = buildPiChildArgs({
    sessionsDir: paths.sessionsDir,
    role: args.role,
    model: args.model,
    trust: args.trust,
    sessionFile: args.sessionFile,
    forkRef: args.forkRef,
    agentsMdPath: fs.existsSync(agentsMd) ? agentsMd : undefined,
    autoAllow: args.extraEnv['BASH_GUARD_AUTO_ALLOW'] === '1',
    pi: args.childArgs.pi && Object.keys(args.childArgs.pi).length ? args.childArgs.pi : undefined,
  });
  const childArgs = [...baseArgs, ...(args.childArgs.args ?? [])];
  // Pre-seed the per-pod sandbox trust store: the project-trust DIALOG in RPC
  // mode kills the session (pi exits after the dialog resolves), so we never
  // let it appear. Per-pod file — the user's shared trust store is untouched.
  const trustFile = path.join(paths.seatRoot, '.pi', 'sandbox-trust.json');
  try {
    const store = fs.existsSync(trustFile) ? (JSON.parse(fs.readFileSync(trustFile, 'utf8')) as Record<string, { level: string; at: number }>) : {};
    store[args.cwd] = { level: args.trustLevel, at: Date.now() };
    fs.writeFileSync(trustFile, JSON.stringify(store, null, 2));
  } catch {
    /* best-effort: the dialog auto-answer fallback remains */
  }
  const childEnv = buildPiChildEnv(process.env, { agentDir: paths.agentDir, sessionsDir: paths.sessionsDir, trustFile, extra: { ...args.extraEnv, ...(args.childArgs.env ?? {}) } });

  console.log(`[flock-runner] starting pi --mode rpc (pod ${args.role}, launch ${args.launchId})`);
  console.log(`[flock-runner] input: plain lines; "/abort" cancels; "/followup <text>" queues after the turn`);

  const child = spawn('pi', childArgs, {
    cwd: args.cwd,
    env: childEnv,
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  const now = () => new Date().toISOString();
  const io: RunnerIO = {
    mirrorLine: (line) => process.stdout.write(`${line}\n`),
    mirrorAppend: (text) => process.stdout.write(text),
    writeSidecar: (state) => {
      try {
        fs.writeFileSync(paths.runnerStatePath, JSON.stringify(state));
      } catch {
        /* best-effort; pane markers remain */
      }
    },
    appendActivity: (ev) => {
      try {
        fs.appendFileSync(paths.activityPath, `${JSON.stringify({ ...ev, at: now() })}\n`);
      } catch {
        /* best-effort audit trail */
      }
    },
    sendRpc: (cmd) => {
      try {
        child.stdin.write(`${JSON.stringify(cmd)}\n`);
      } catch {
        /* exit handler reports */
      }
    },
    now,
  };

  const core = new RunnerCore(io, paths, args.launchId, args.trustOption);
  readline.createInterface({ input: child.stdout }).on('line', (line) => core.handlePiLine(line));
  readline.createInterface({ input: child.stderr }).on('line', (line) => {
    if (line.trim()) process.stdout.write(`[pi:err] ${line}\n`);
  });
  const input = readline.createInterface({ input: process.stdin, terminal: process.stdin.isTTY === true, crlfDelay: Infinity });
  input.on('line', (line) => core.handleUserBlock(line));

  child.on('error', (err) => {
    console.error(`${RUNNER_ERROR_MARKER} failed to spawn pi: ${err.message}`);
    core.handlePiExit(null);
    input.close();
    process.exitCode = 1;
  });
  child.on('exit', (code, signal) => {
    core.handlePiExit(code, signal);
    input.close();
    process.exitCode = code ?? 1;
  });

  // Typed exit on signals: C-c / SIGTERM (relaunch in a persistent pane) must
  // leave the sidecar `exited` behind — an untyped death is the stale-ready
  // case the foreground guard exists for, but a typed exit is cleaner.
  const onSignal = (sig: string) => {
    core.handlePiExit(null, sig);
    try {
      child.kill('SIGTERM');
    } catch {
      /* already dead */
    }
    input.close();
    setTimeout(() => process.exit(sig === 'SIGINT' ? 130 : 143), 1500).unref();
  };
  process.on('SIGINT', () => onSignal('SIGINT'));
  process.on('SIGTERM', () => onSignal('SIGTERM'));

  core.start();
}

// Compiled-entry guard (main only when executed directly, not imported).
import { pathToFileURL } from 'node:url';
const invokedDirectly = (() => {
  try {
    const entry = process.argv[1];
    return entry ? import.meta.url === pathToFileURL(path.resolve(entry)).href : false;
  } catch {
    return false;
  }
})();
if (invokedDirectly) {
  void main();
}
