import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// daemonize: `flock core up` spawns core detached with pidfile + log.

const mainPath = path.join(import.meta.dirname, 'main.js');

const home = () => process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock');
const port = () => Number(process.env.FLOCK_PORT ?? 7460);
const pidFile = () => path.join(home(), 'core.pid');
const tokenFile = () => path.join(home(), 'token');
const logFile = () => path.join(home(), 'core.log');

export function readToken(): string | null {
  try {
    return fs.readFileSync(tokenFile(), 'utf8').trim();
  } catch {
    return null;
  }
}

export function isPidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export function readPid(): number | null {
  try {
    const p = Number(fs.readFileSync(pidFile(), 'utf8').trim());
    return Number.isFinite(p) && p > 0 ? p : null;
  } catch {
    return null;
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function healthz(): Promise<{ status: number; body: unknown }> {
  const token = readToken();
  const res = await fetch(`http://127.0.0.1:${port()}/healthz`, {
    headers: token ? { authorization: `Bearer ${token}` } : {},
  });
  const body = await res.json().catch(() => null);
  return { status: res.status, body };
}

export async function coreUp(): Promise<string> {
  const h = home();
  fs.mkdirSync(h, { recursive: true });
  const pid = readPid();
  if (pid && isPidAlive(pid)) return `already running (pid ${pid})`;
  if (pid) fs.rmSync(pidFile(), { force: true }); // stale pidfile

  const logFd = fs.openSync(logFile(), 'a');
  const child = spawn(process.execPath, [mainPath], {
    detached: true,
    stdio: ['ignore', logFd, logFd],
    env: { ...process.env, FLOCK_HOME: h, FLOCK_PORT: String(port()) },
  });
  child.unref();
  fs.closeSync(logFd);

  for (let i = 0; i < 50; i++) {
    await sleep(100);
    const hz = await healthz().catch(() => null);
    if (hz && hz.status === 200) return `core up: pid ${readPid() ?? '?'} port ${port()}`;
  }
  return `core up: spawned pid ${child.pid}, healthz not ready in 5s — check ${logFile()}`;
}

export async function coreDown(): Promise<string> {
  const pid = readPid();
  if (!pid || !isPidAlive(pid)) {
    fs.rmSync(pidFile(), { force: true });
    return 'not running';
  }
  process.kill(pid, 'SIGTERM');
  for (let i = 0; i < 50; i++) {
    await sleep(100);
    if (!isPidAlive(pid)) return `stopped (pid ${pid})`;
  }
  try {
    process.kill(pid, 'SIGKILL');
  } catch {}
  return `killed (pid ${pid})`;
}

export async function coreStatus(): Promise<string> {
  const pid = readPid();
  if (!pid || !isPidAlive(pid)) return 'not running';
  const hz = await healthz().catch(() => null);
  if (!hz || hz.status !== 200) return `pid ${pid} alive but healthz failing (status ${hz?.status ?? 'n/a'})`;
  const b = hz.body as {
    db: string;
    ticks: { name: string; runs: number; lastError: string | null }[];
    pods: { role: string; state: string }[];
  };
  const ticks = b.ticks
    .map((t) => `${t.name} ${t.runs}x${t.lastError ? ` ERR ${t.lastError}` : ''}`)
    .join(', ');
  const pods = b.pods.map((p) => `${p.role}:${p.state}`).join(', ') || 'none';
  return `running: pid ${pid}, port ${port()}, db ${b.db}, ticks [${ticks}], pods: ${pods}`;
}
