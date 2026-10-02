import { execFile } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { seatPaths, parseRunnerState, type RunnerState } from './runner-protocol.js';

// tmux transport: one window per pod.
// Text delivery = load-buffer + paste-buffer (NOT send-keys with text):
// long prompts via send-keys are slow and break on special characters.
// send-keys is used only for Enter.

export const TMUX_SESSION = (() => {
  // multi-flock: each profile (FLOCK_HOME) gets its own tmux session so
  // parallel cores don't fight over windows. Main keeps the historic name.
  const h = process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock');
  const b = path.basename(h).replace(/^\./, '');
  return b === 'flock' ? 'flock' : `flock-${b}`;
})();
export const winName = (role: string) => `flock-${role}`;
export const winTarget = (role: string) => `${TMUX_SESSION}:${winName(role)}`;

interface ExecRes {
  code: number;
  out: string;
  err: string;
}

function tmux(args: string[]): Promise<ExecRes> {
  return new Promise((resolve) => {
    execFile('tmux', args, { maxBuffer: 4 * 1024 * 1024 }, (error, stdout, stderr) => {
      const code = error ? (typeof error.code === 'number' ? error.code : 1) : 0;
      resolve({ code, out: String(stdout ?? ''), err: String(stderr ?? '') });
    });
  });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// tmux quirk: without an explicit index, new-window in a clientless session
// can pick a taken index ("index N in use"). Pick max+1 explicitly.
async function newWindow(name: string, dir: string, cmd: string, role: string, env?: Record<string, string>): Promise<void> {
  // env for the window: flock CLI on PATH (auto-registration, task reports),
  // this instance's home/port (multi-flock), pod role for attribution, plus
  // the agent manifest's env (config isolation etc.).
  const home = process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock');
  const envPairs: Record<string, string> = {
    // home bin first (shadows system /usr/bin/flock); <pod>/bin second — it is
    // the only part of $HOME visible to the pi sandbox (bwrap masks /home),
    // so the agent's `flock` resolves inside the sandbox too.
    PATH: `${home}/bin:${dir}/bin:$PATH`,
    FLOCK_HOME: home,
    FLOCK_PORT: process.env.FLOCK_PORT ?? '7460',
    FLOCK_POD_ROLE: role,
    ...(env ?? {}),
  };
  const envPrefix = Object.entries(envPairs).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(' ');
  const list = await tmux(['list-windows', '-t', TMUX_SESSION, '-F', '#{window_index}']);
  const idxs = list.out
    .trim()
    .split('\n')
    .map((s) => parseInt(s, 10))
    .filter((n) => Number.isFinite(n));
  const next = (idxs.length ? Math.max(...idxs) : 0) + 1;
  const r = await tmux([
    'new-window',
    '-t', `${TMUX_SESSION}:${next}`,
    '-n', name,
    '-c', dir,
    `env ${envPrefix} ${cmd}`,
  ]);
  // automatic-rename would rename the window to the running command
  // ("flock-dev" -> "pi"), breaking name addressing: pin it off.
  await tmux(['set-option', '-w', '-t', `${TMUX_SESSION}:${next}`, 'automatic-rename', 'off']);
  await tmux(['rename-window', '-t', `${TMUX_SESSION}:${next}`, name]);
  // tmux also warns "no current client" (exit 1) when the session has no
  // attached client, but the window IS created. Verify by evidence.
  const after = await tmux(['list-windows', '-t', TMUX_SESSION, '-F', '#{window_name}']);
  if (!after.out.trim().split('\n').includes(name)) {
    throw new Error(`tmux new-window failed: ${r.err.trim()}`);
  }
}

export async function sessionExists(): Promise<boolean> {
  const r = await tmux(['has-session', '-t', TMUX_SESSION]);
  return r.code === 0;
}

async function ensureSession(): Promise<void> {
  if (!(await sessionExists())) {
    const r = await tmux(['new-session', '-d', '-s', TMUX_SESSION, '-n', '_init']);
    if (r.code !== 0) throw new Error(`tmux new-session failed: ${r.err.trim()}`);
  }
}

export interface SpawnOpts {
  role: string;
  dir: string;
  cmd?: string;
  env?: Record<string, string>;
}

export async function spawnPod(o: SpawnOpts): Promise<{ target: string; pid: number | null }> {
  await ensureSession();
  const list = await tmux(['list-windows', '-t', TMUX_SESSION, '-F', '#{window_name}']);
  const names = list.out.trim().split('\n').filter(Boolean);
  if (names.includes(winName(o.role))) {
    throw new Error(`pod window already exists: ${winName(o.role)}`);
  }
  await newWindow(winName(o.role), o.dir, o.cmd ?? 'pi', o.role, o.env);
  await sleep(300);
  const pr = await tmux(['display-message', '-p', '-t', winTarget(o.role), '#{pane_pid}']);
  return { target: winTarget(o.role), pid: pr.code === 0 ? Number(pr.out.trim()) : null };
}

export async function killWindow(role: string): Promise<void> {
  await tmux(['kill-window', '-t', winTarget(role)]);
}

export async function paneAlive(role: string): Promise<boolean> {
  const r = await tmux(['list-panes', '-t', winTarget(role), '-F', '#{pane_dead}']);
  return r.code === 0 && r.out.trim() === '0';
}

// Foreground process of the pod pane. A runner-sidecar "ready" (or a READY
// marker in scrollback) only counts while the foreground is NOT a shell —
// a dead runner leaves the pane at the shell (OpenRig's stale-artifact guard).
export const SHELL_COMMANDS = new Set(['bash', 'zsh', 'sh', 'fish', 'dash', 'ksh']);
export async function paneCommand(target: string): Promise<string> {
  const r = await tmux(['display-message', '-p', '-t', target, '#{pane_current_command}']);
  return r.code === 0 ? r.out.trim() : '';
}

// Typed pod state from the runner sidecar (never screen-scraped).
export function readRunnerState(stateRoot: string, role: string): RunnerState | null {
  try {
    return parseRunnerState(fs.readFileSync(seatPaths(stateRoot, role).runnerStatePath, 'utf8'));
  } catch {
    return null;
  }
}

// Poll the sidecar until this launch reports ready (or exits). Used by
// pod_spawn so the arbiter never claims a task to an unready pod.
export async function waitForRunnerReady(
  stateRoot: string,
  role: string,
  launchId: string,
  timeoutMs = 20000,
): Promise<{ ok: true; state: RunnerState } | { ok: false; reason: 'exited' | 'timeout'; code?: number | null; detail?: string }> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const st = readRunnerState(stateRoot, role);
    if (st && st.launchId === launchId) {
      if (st.exited) return { ok: false, reason: 'exited', code: st.exited.code };
      if (st.ready) return { ok: true, state: st };
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  const st = readRunnerState(stateRoot, role);
  return { ok: false, reason: 'timeout', detail: st ? `sidecar: ${JSON.stringify(st).slice(0, 200)}` : 'no sidecar' };
}

const bufName = () => `flock_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;

export async function paste(target: string, text: string): Promise<void> {
  const buf = bufName();
  const file = path.join(os.tmpdir(), `flock-paste-${buf}.txt`);
  fs.writeFileSync(file, text);
  try {
    const load = await tmux(['load-buffer', '-b', buf, file]);
    if (load.code !== 0) throw new Error(`tmux load-buffer failed: ${load.err.trim()}`);
    const p = await tmux(['paste-buffer', '-t', target, '-b', buf, '-r', '-p']);
    if (p.code !== 0) throw new Error(`tmux paste-buffer failed: ${p.err.trim()}`);
  } finally {
    await tmux(['delete-buffer', '-b', buf]);
    fs.rmSync(file, { force: true });
  }
}

export async function sendEnter(target: string): Promise<void> {
  await tmux(['send-keys', '-t', target, 'Enter']);
}

export async function capture(target: string, lines = 200): Promise<string> {
  const r = await tmux(['capture-pane', '-t', target, '-p', '-S', `-${lines}`]);
  if (r.code !== 0) throw new Error(`tmux capture-pane failed: ${r.err.trim()}`);
  return r.out.replace(/\n+$/, '');
}

// Reliable delivery: paste + Enter, then VERIFY the text reached the TUI
// (visible in the transcript or the "Steering" queue). pi TUI queues input
// typed while a turn is in flight, so pasting is safe even when busy — no
// long busy-wait (that would block the caller and time out the CLI).
// ponytail: whitespace-insensitive match — the TUI wraps long lines, which
// breaks naive substring checks (and false-failures cause duplicate resends).
// A repeated identical message can still false-positive from scrollback.
// Upgrade path: per-send nonce in the text.
export async function send(
  target: string,
  text: string,
  opts?: { attempts?: number; waitMs?: number; lines?: number; raw?: boolean },
): Promise<{ delivered: boolean; attempts: number }> {
  // raw: paste + Enter without visual verification — for runner pods, where
  // the typed sidecar ack is the real verification (the visual probe breaks
  // on long/wrapped lines: the TTY line editor clears its input after Enter).
  if (opts?.raw) {
    await paste(target, text);
    await sendEnter(target);
    return { delivered: true, attempts: 1 };
  }
  const attempts = opts?.attempts ?? 3;
  const waitMs = opts?.waitMs ?? 1500;
  const lines = opts?.lines ?? 100;
  const norm = (s: string) => s.replace(/\s+/g, '');
  const probe = norm(text.split('\n').find((l) => l.trim()) ?? '').slice(0, 80);
  for (let i = 1; i <= attempts; i++) {
    await paste(target, text);
    await sendEnter(target);
    await sleep(waitMs);
    try {
      const out = norm(await capture(target, lines));
      if (!probe || out.includes(probe)) return { delivered: true, attempts: i };
    } catch {
      // pane vanished mid-flight; next attempt will surface the error
    }
  }
  return { delivered: false, attempts };
}

// Transport self-test, isolated from agents: temp bash window, echo roundtrip.
export async function checkTransport(home: string): Promise<{ ok: boolean; detail: string }> {
  const tag = `FLOCKCHECK_${Date.now().toString(36)}`;
  const target = winTarget('_check');
  await ensureSession();
  await newWindow(winName('_check'), home, 'bash', '_check');
  try {
    await sleep(400);
    await paste(target, `echo ${tag}`);
    await sendEnter(target);
    await sleep(400);
    const out = await capture(target, 50);
    return out.includes(tag)
      ? { ok: true, detail: 'echo roundtrip ok (paste+capture proven)' }
      : { ok: false, detail: 'tag not found in capture' };
  } finally {
    await killWindow('_check');
  }
}
