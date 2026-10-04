// Fleet: cross-profile coordination edge (C15).
//
// Profiles are independent cores (own FLOCK_HOME/port/tmux); the
// single-writer invariant per core is preserved. The fleet is an EDGE, not
// a shared memory: fleet.json names the other cores, and coordination
// happens over plain HTTP apply() calls between them.
//
// Addressing: <profile>/<role>; local profile = no prefix.
//
// ponytail: no central orchestrator, no mesh state sync, no sqlite
// replication — an edge with two endpoints is enough for the product loop.
import * as fs from 'node:fs';
import * as path from 'node:path';

export interface FleetProfile {
  name: string;
  url: string; // http://127.0.0.1:<port> (or a host if remote)
  token?: string; // operator token for the remote core (if it requires one)
}

export interface FleetConfig {
  profiles: FleetProfile[];
}

// ---------- config file: ~/.flock/fleet.json (per profile) ----------

export function fleetFile(home: string): string {
  return path.join(home, 'fleet.json');
}

export function loadFleet(home: string): FleetConfig {
  const f = fleetFile(home);
  if (!fs.existsSync(f)) return { profiles: [] };
  try {
    const raw = JSON.parse(fs.readFileSync(f, 'utf8'));
    const profiles = Array.isArray(raw.profiles)
      ? raw.profiles.filter(
          (p: unknown): p is FleetProfile =>
            typeof p === 'object' && p !== null && typeof (p as FleetProfile).name === 'string',
        )
      : [];
    return { profiles };
  } catch {
    return { profiles: [] }; // corrupt config: empty fleet (loud in `fleet ls`)
  }
}

export function saveFleet(home: string, cfg: FleetConfig): void {
  fs.mkdirSync(home, { recursive: true });
  fs.writeFileSync(fleetFile(home), JSON.stringify(cfg, null, 2) + '\n');
}

export function getProfile(home: string, name: string): FleetProfile | undefined {
  return loadFleet(home).profiles.find((p) => p.name === name);
}

// ---------- addressing: <profile>/<role> ----------

const ROLE_RE = /^[a-z0-9][a-z0-9-]{0,30}$/;
const PROFILE_RE = /^[a-z0-9][a-z0-9._-]{0,63}$/;

export interface Address {
  profile?: string; // undefined = local
  role: string;
}

export function parseAddress(to: string): Address {
  const s = to.trim();
  const slash = s.indexOf('/');
  if (slash < 0) {
    if (!ROLE_RE.test(s)) throw new Error(`bad target: ${to}`);
    return { role: s };
  }
  const profile = s.slice(0, slash);
  const role = s.slice(slash + 1);
  if (!PROFILE_RE.test(profile)) throw new Error(`bad profile: ${profile}`);
  if (!ROLE_RE.test(role)) throw new Error(`bad role: ${role}`);
  return { profile, role };
}

// ---------- remote apply (HTTP POST /api/ops) ----------

export interface RemoteResult {
  ok: boolean;
  result?: unknown;
  error?: string;
}

export async function remoteApply(home: string, profile: string, op: Record<string, unknown>, timeoutMs = 10_000): Promise<RemoteResult> {
  const p = getProfile(home, profile);
  if (!p) throw new Error(`no fleet profile: ${profile} (flock fleet add ${profile} <url>)`);
  const url = `${p.url.replace(/\/$/, '')}/api/ops`;
  const headers: Record<string, string> = { 'content-type': 'application/json' };
  if (p.token) headers.authorization = `Bearer ${p.token}`;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(op),
      signal: ctrl.signal,
    });
    const body = (await res.json()) as RemoteResult;
    if (!res.ok && !body.error) return { ok: false, error: `http ${res.status}` };
    return body;
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, error: `remote ${profile} unreachable: ${msg}` };
  } finally {
    clearTimeout(t);
  }
}

// ---------- remote health (GET /healthz) ----------

export interface RemoteHealth {
  ok: boolean;
  detail?: string;
}

export async function remoteHealth(profile: FleetProfile, timeoutMs = 5000): Promise<RemoteHealth> {
  const headers: Record<string, string> = {};
  if (profile.token) headers.authorization = `Bearer ${profile.token}`;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(`${profile.url.replace(/\/$/, '')}/healthz`, { signal: ctrl.signal, headers });
    if (!res.ok) return { ok: false, detail: `http ${res.status}` };
    return { ok: true };
  } catch (e) {
    return { ok: false, detail: e instanceof Error ? e.message : String(e) };
  } finally {
    clearTimeout(t);
  }
}
