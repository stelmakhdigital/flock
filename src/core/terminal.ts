import { execFile } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// tmux transport: one window per pod.
// Text delivery = load-buffer + paste-buffer (NOT send-keys with text):
// long prompts via send-keys are slow and break on special characters.
// send-keys is used only for Enter.

export const TMUX_SESSION = 'flock';
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
async function newWindow(name: string, dir: string, cmd: string, role: string): Promise<void> {
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
    // env wrapper: flock CLI on PATH for the agent (auto-registration,
    // W2), FLOCK_HOME for state, FLOCK_POD_ROLE so the agent's CLI calls
    // are attributed to this pod, not to 'cli'.
    `env PATH="$HOME/.flock/bin:$PATH" FLOCK_HOME="$HOME/.flock" FLOCK_POD_ROLE=${JSON.stringify(role)} ${cmd}`,
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
}

export async function spawnPod(o: SpawnOpts): Promise<{ target: string; pid: number | null }> {
  await ensureSession();
  const list = await tmux(['list-windows', '-t', TMUX_SESSION, '-F', '#{window_name}']);
  const names = list.out.trim().split('\n').filter(Boolean);
  if (names.includes(winName(o.role))) {
    throw new Error(`pod window already exists: ${winName(o.role)}`);
  }
  await newWindow(winName(o.role), o.dir, o.cmd ?? 'pi', o.role);
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
  opts?: { attempts?: number; waitMs?: number; lines?: number },
): Promise<{ delivered: boolean; attempts: number }> {
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
