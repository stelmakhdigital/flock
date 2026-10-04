// flock-codex — pane-hosted bridge for the codex runtime. Spawns
// `codex exec --json` (one process per turn, thread continued via
// `exec resume <id>` / `exec fork <id>`), mirrors a human-readable
// transcript to the pane, keeps the same typed sidecar (runner-state.json)
// + activity log the core reads for pi, and forwards pane stdin (flockmsg
// frames / human typing) as queued turns.
//
// Node builtins + codex-protocol only: runnable standalone as
//   node dist/core/codex-bridge.js --state-root ... --role ... --cwd ... --launch-id ... --command codex
// so it stays importable by tests without core dependencies.
//
// ponytail: the bridge is per-turn exec, not a long-lived RPC — a new codex
// process per turn re-reads the config and costs a process spawn; for a
// local 27B model that is noise next to the turn itself. If codex ever gets
// a stable client protocol we swap the child, the contract stays.

import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import { spawn, type ChildProcess } from 'node:child_process';
import {
  seatPaths,
  buildPendingState,
  unframeMessage,
  RUNNER_ERROR_MARKER,
  RUNNER_EXIT_MARKER,
  type RunnerState,
} from './bridge-protocol.js';
import {
  CODEX_BRIDGE_READY_MARKER,
  buildCodexChildEnv,
  codexHome,
  parseCodexEvent,
  validateCodexSessionToken,
} from './codex-protocol.js';

interface Args {
  stateRoot: string;
  role: string;
  cwd: string;
  launchId: string;
  command: string;
  shimPort: number;
  model?: string;
  resumeThread?: string;
  forkRef?: string;
  keyEnv: Record<string, string>;
  // C10: raw child args/env (manifest `child` field)
  childArgs: string[];
  childEnv: Record<string, string>;
}

function parseArgs(argv: string[]): Args {
  const get = (name: string) => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  const args: Args = {
    stateRoot: get('--state-root') ?? '',
    role: get('--role') ?? '',
    cwd: get('--cwd') ?? process.cwd(),
    launchId: get('--launch-id') ?? '',
    command: get('--command') ?? 'codex',
    shimPort: Number(get('--shim-port') ?? 0),
    model: get('--model'),
    resumeThread: get('--resume-thread'),
    forkRef: get('--fork-ref'),
    keyEnv: Object.fromEntries(
      argv.flatMap((a, i) => (a === '--env' ? [[argv[i + 1], argv[i + 2]] as [string, string]] : [])),
    ) as Record<string, string>,
    childArgs: (() => {
      const raw = get('--child-args');
      try {
        const v = raw ? JSON.parse(raw) : [];
        return Array.isArray(v) && v.every((x) => typeof x === 'string') ? (v as string[]) : [];
      } catch {
        return [];
      }
    })(),
    childEnv: (() => {
      const raw = get('--child-env');
      try {
        const v = raw ? (JSON.parse(raw) as Record<string, unknown>) : {};
        const out: Record<string, string> = {};
        for (const [k, val] of Object.entries(v ?? {})) if (typeof val === 'string') out[k] = val;
        return out;
      } catch {
        return {};
      }
    })(),
  };
  if (!args.stateRoot || !args.role || !args.launchId || !args.shimPort) {
    throw new Error('missing required args: --state-root --role --launch-id --shim-port');
  }
  return args;
}

interface QueuedMessage {
  text: string;
  nonce?: string;
}

interface BridgeIO {
  mirror: (line: string) => void;
  writeSidecar: (state: RunnerState) => void;
  appendActivity: (ev: Record<string, unknown>) => void;
  now: () => string;
}

const firstLine = (s: string) => (s.split('\n')[0] ?? '').slice(0, 200);

export class CodexBridgeCore {
  private io: BridgeIO;
  private args: Args;
  private ready = false;
  private exited = false;
  private busy = false;
  private threadId: string | undefined;
  private queue: QueuedMessage[] = [];
  private child: ChildProcess | null = null;
  private forkPending: string | undefined;
  private lastPrompt: { nonce?: string; text: string; at: string } | undefined;
  private stopRequested = false;

  constructor(io: BridgeIO, args: Args) {
    this.io = io;
    this.args = args;
    this.forkPending = args.forkRef;
    // resume: the sidecar carries the requested thread from the start —
    // the ready gate and latestSessionToken see the honest identity.
    this.threadId = args.resumeThread;
  }

  private baseState(): RunnerState {
    return {
      ready: this.ready && !this.exited,
      launchId: this.args.launchId,
      updatedAt: this.io.now(),
      ...(this.threadId ? { sessionId: this.threadId } : {}),
      streaming: this.busy,
      ...(this.lastPrompt ? { lastPrompt: this.lastPrompt } : {}),
    };
  }

  start(): void {
    fs.mkdirSync(codexHome(this.paths().seatRoot), { recursive: true });
    this.ready = true;
    this.io.writeSidecar(this.baseState());
    this.io.mirror(CODEX_BRIDGE_READY_MARKER + (this.threadId ? ` (thread ${this.threadId})` : ' (fresh)'));
    this.io.appendActivity({ event: 'ready', runtime: 'codex', ...(this.threadId ? { sessionId: this.threadId } : {}) });
  }

  private paths() {
    return seatPaths(this.args.stateRoot, this.args.role);
  }

  // A pane line: flockmsg frame (nonce ack), /abort, or human text.
  handleUserLine(raw: string): void {
    const line = raw.trim();
    if (!line || this.exited) return;
    if (line === '/abort') {
      if (this.child) {
        this.child.kill('SIGTERM');
        this.io.mirror('[flock-codex] abort sent (turn interrupted)');
      } else {
        this.io.mirror('[flock-codex] nothing running to abort');
      }
      return;
    }
    if (line.startsWith('/')) {
      // unknown control line: ignore (codex exec has no dialogs to answer)
      return;
    }
    const framed = unframeMessage(line);
    this.enqueue(framed?.text ?? line, framed?.nonce);
  }

  // Delivery ack happens at ENQUEUE (the message is in the agent's inbox);
  // the turn itself may start after a longer in-flight turn — the same
  // semantics claude uses (transcript grows when the message lands).
  private enqueue(text: string, nonce?: string): void {
    this.queue.push({ text, nonce });
    this.lastPrompt = { text: text.slice(0, 2000), ...(nonce ? { nonce } : {}), at: this.io.now() };
    this.io.writeSidecar(this.baseState());
    this.io.appendActivity({ event: 'prompt', bytes: text.length, via: nonce ? 'flockmsg' : 'pane' });
    this.io.mirror(`you ▸ ${firstLine(text)}`);
    void this.pump();
  }

  private async pump(): Promise<void> {
    if (this.busy || this.exited || this.stopRequested) return;
    const next = this.queue.shift();
    if (!next) return;
    this.busy = true;
    this.io.writeSidecar(this.baseState());
    const startedAt = Date.now();
    await this.runTurn(next.text);
    this.busy = false;
    this.io.writeSidecar(this.baseState());
    const dt = ((Date.now() - startedAt) / 1000).toFixed(1);
    if (!this.exited) {
      // drain the rest of the queue (messages that landed mid-turn)
      this.io.mirror(`[codex] turn done (${dt}s)`);
      await this.pump();
    }
  }

  private runTurn(text: string): Promise<void> {
    const a = this.args;
    // --skip-git-repo-check: the pod dir is a flock-managed workspace, not a
    // "trusted" git repo in codex's sense — the trust boundary is flock's
    // pod isolation (the pi adapter pre-seeds the same thing as sandbox trust)
    const args: string[] = ['exec', '--json', '--skip-git-repo-check'];
    if (this.forkPending) {
      args.push('fork', this.forkPending);
      const forkRef = this.forkPending;
      this.forkPending = undefined; // fork is a one-shot
      args.push('--', text);
      return this.spawnTurn(args, (ev) => {
        this.onEvent(ev);
        // fork rule: the new thread must NOT be the parent's (mirrors pi)
        if (ev.kind === 'thread' && ev.threadId.toLowerCase() === forkRef.toLowerCase()) {
          this.io.mirror(`${RUNNER_ERROR_MARKER} fork reported the parent thread — refused`);
          this.io.appendActivity({ event: 'fork_rule_violation', forkRef });
          this.child?.kill('SIGTERM');
        }
      });
    }
    if (this.threadId) args.push('resume', this.threadId);
    // C10: raw manifest child args ride before the prompt separator
    args.push(...this.args.childArgs);
    args.push('--', text);
    return this.spawnTurn(args, (ev) => this.onEvent(ev));
  }

  private spawnTurn(args: string[], onEvent: (ev: ReturnType<typeof parseCodexEvent>) => void): Promise<void> {
    return new Promise((resolve) => {
      const env = buildCodexChildEnv(process.env, { codexHome: codexHome(this.paths().seatRoot), keyEnv: { ...this.args.keyEnv, ...this.args.childEnv } });
      let child: ChildProcess;
      try {
        child = spawn(this.args.command, args, {
          cwd: this.args.cwd,
          env,
          stdio: ['ignore', 'pipe', 'pipe'],
        });
      } catch (e) {
        this.io.mirror(`${RUNNER_ERROR_MARKER} failed to spawn ${this.args.command}: ${e instanceof Error ? e.message : String(e)}`);
        this.io.appendActivity({ event: 'spawn_error', error: String(e) });
        return resolve();
      }
      this.child = child;
      const out = readline.createInterface({ input: child.stdout! });
      out.on('line', (line) => onEvent(parseCodexEvent(line)));
      const err = readline.createInterface({ input: child.stderr! });
      err.on('line', (line) => {
        if (line.trim()) this.io.mirror(`[codex:err] ${line.trim().slice(0, 300)}`);
      });
      child.on('error', (e) => {
        // spawn-time failure (missing binary): a real crash, not a bad turn
        this.io.mirror(`${RUNNER_ERROR_MARKER} failed to spawn ${this.args.command}: ${e.message}`);
        this.io.appendActivity({ event: 'spawn_error', error: e.message });
        this.exited = true;
        const state = this.baseState();
        state.exited = { code: 1, at: this.io.now() };
        this.io.writeSidecar(state);
        resolve();
      });
      child.on('exit', (code) => {
        out.close();
        err.close();
        if (this.child === child) this.child = null;
        if (code !== 0 && !this.exited) {
          this.io.mirror(`${RUNNER_ERROR_MARKER} codex exec exited (code ${code ?? 'null'})`);
          this.io.appendActivity({ event: 'turn_exit_error', code });
        }
        resolve();
      });
    });
  }

  private onEvent(ev: ReturnType<typeof parseCodexEvent>): void {
    switch (ev.kind) {
      case 'thread':
        if (ev.threadId !== this.threadId) {
          this.threadId = ev.threadId;
          this.io.mirror(`[codex] thread ${ev.threadId}`);
          this.io.writeSidecar(this.baseState());
          this.io.appendActivity({ event: 'thread', sessionId: ev.threadId });
        }
        break;
      case 'turn_start':
        this.io.appendActivity({ event: 'turn_start' });
        break;
      case 'agent_message':
        if (ev.text.trim()) this.io.mirror(ev.text);
        break;
      case 'tool':
        this.io.mirror('  ⚙ command …');
        this.io.appendActivity({ event: 'tool_start', tool: ev.name });
        break;
      case 'error':
        this.io.mirror(`${RUNNER_ERROR_MARKER} ${ev.message.split('\n')[0].slice(0, 300)}`);
        this.io.appendActivity({ event: 'codex_error', message: ev.message.slice(0, 300) });
        break;
      case 'turn_failed':
        this.io.mirror(`${RUNNER_ERROR_MARKER} ${ev.error.split('\n')[0].slice(0, 300)}`);
        this.io.appendActivity({ event: 'turn_failed', error: ev.error.slice(0, 300) });
        break;
      case 'turn_complete': {
        if (ev.usage) {
          this.io.appendActivity({
            event: 'usage',
            input: ev.usage.input,
            output: ev.usage.output,
            cacheRead: ev.usage.cacheRead,
            cacheWrite: ev.usage.cacheWrite,
            totalTokens: ev.usage.input + ev.usage.output,
            model: this.args.model,
          });
        }
        break;
      }
      case 'noise':
        // JSONL lines the parser does not model (item.started, unknown types)
        // are quiet; only human warnings (non-JSON) reach the pane
        if (ev.text.trim() && !ev.text.trimStart().startsWith('{')) {
          this.io.mirror(ev.text.trim().slice(0, 300));
        }
        break;
    }
  }

  stop(sig: string): void {
    if (this.exited) return;
    this.exited = true;
    this.stopRequested = true;
    this.io.mirror(`${RUNNER_EXIT_MARKER} codex bridge stopped (${sig})`);
    const state = this.baseState();
    state.exited = { code: sig === 'SIGINT' ? 130 : 143, signal: sig, at: this.io.now() };
    this.io.writeSidecar(state);
    this.io.appendActivity({ event: 'bridge_exit', signal: sig });
  }
}

export async function main(argv: string[] = process.argv.slice(2)): Promise<void> {
  let args: Args;
  try {
    args = parseArgs(argv);
  } catch (e) {
    console.error(`${RUNNER_ERROR_MARKER} ${e instanceof Error ? e.message : String(e)}`);
    process.exitCode = 2;
    return;
  }
  // resume/fork tokens are validated here (the adapter validates too — the
  // bridge is the second gate: a bad token dies at the pane, not mid-turn).
  for (const [name, tok] of [['resume-thread', args.resumeThread], ['fork-ref', args.forkRef]] as const) {
    if (tok) {
      const v = validateCodexSessionToken(tok);
      if (!v.ok) {
        console.error(`${RUNNER_ERROR_MARKER} ${name}: ${v.error}`);
        process.exitCode = 2;
        return;
      }
      if (name === 'resume-thread') args.resumeThread = v.token;
      else args.forkRef = v.token;
    }
  }
  const paths = seatPaths(args.stateRoot, args.role);
  fs.mkdirSync(path.dirname(paths.runnerStatePath), { recursive: true });
  fs.writeFileSync(paths.runnerStatePath, JSON.stringify(buildPendingState(args.launchId, new Date().toISOString())));
  const now = () => new Date().toISOString();
  const core = new CodexBridgeCore(
    {
      mirror: (line) => process.stdout.write(`${line}\n`),
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
      now,
    },
    args,
  );

  console.log(`[flock-codex] starting codex exec bridge (pod ${args.role}, launch ${args.launchId}, command ${args.command})`);
  core.start();

  const input = readline.createInterface({ input: process.stdin, terminal: process.stdin.isTTY === true, crlfDelay: Infinity });
  input.on('line', (line) => core.handleUserLine(line));
  input.on('close', () => {
    // pane shell went away (window killed) — typed exit so the sidecar is clean
    core.stop('SIGTERM');
    process.exitCode = 143;
  });

  const onSignal = (sig: string) => {
    core.stop(sig);
    input.close();
    setTimeout(() => process.exit(sig === 'SIGINT' ? 130 : 143), 1500).unref();
  };
  process.on('SIGINT', () => onSignal('SIGINT'));
  process.on('SIGTERM', () => onSignal('SIGTERM'));
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
