// Agent adapters: ONE generic implementation + declarative manifests.
// Adding a runtime (claude, codex, ...) = a JSON manifest in <FLOCK_HOME>/agents/,
// not new code: a RuntimeAdapter per runtime + a manifest describing it.
//
// Manifest = the "interface" a new agent implements:
//   {
//     id: string,
//     command: string,            // base command (e.g. "pi", "claude")
//     modelFlag?: string,         // flag that takes a model id (pi: "--model")
//     args?: string[],            // fixed extra args
//     env?: Record<string,string> // extra window env (config isolation etc.)
//     // stage 3+: readyPattern, sessionFile ("{dir}" template) for resume
//   }
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export interface AgentManifest {
  id: string;
  command: string;
  // runtime selects the RuntimeAdapter (v3.1). Legacy: runner === 'flock-rpc'
  // maps to 'pi'; bare command 'bash' maps to 'bash'.
  runtime?: string;
  modelFlag?: string;
  args?: string[];
  env?: Record<string, string>;
  // pi RPC bridge (legacy selector -> runtime 'pi'): pane-hosted runner
  // (typed ready/busy/exit, delivery ack, session identity).
  runner?: 'flock-rpc';
  trust?: 'approve' | 'no-approve'; // configured resource trust (floor posture)
  trustOption?: string; // fallback: trust dialog option substring
  trustLevel?: 'off' | 'dev' | 'untrusted' | 'vm'; // sandbox level pre-seeded per pod
  // v3.1: launch posture — 'full_bypass' forces full resource trust
  // (YOLO semantics). Default 'floor' respects `trust`.
  launchPosture?: 'floor' | 'full_bypass';
  // v3.1: permission mode slot (claude/codex style). The pi adapter REJECTS
  // a set value: pi resource trust is a separate mechanism (trust/posture).
  permissionMode?: string;
  // v3.1: startup guidance — managed blocks merged into the pod AGENTS.md
  // (idempotent, boot-refresh-safe). Content is additive, never replaces.
  guidance?: { id: string; content: string }[];
  // v3.1: sent to the agent once, after the ready gate, fresh starts only.
  firstPrompt?: string;
}

import { PM_PROTOCOL } from './pm-protocol.js';

export const BUILTIN_AGENTS: Record<string, AgentManifest> = {
  pi: { id: 'pi', command: 'pi', modelFlag: '--model', runner: 'flock-rpc', trust: 'approve', trustLevel: 'dev' },
  bash: { id: 'bash', command: 'bash' },
  // goal-loop lead: wakes on triggers, issues typed intents (whitelist) via the CLI
  pm: {
    id: 'pm',
    command: 'pi',
    modelFlag: '--model',
    runner: 'flock-rpc',
    trust: 'approve',
    trustLevel: 'dev',
    guidance: [{ id: 'pm-protocol', content: PM_PROTOCOL }],
  },
};

// user-defined manifests: <FLOCK_HOME>/agents/*.json (override builtins by id)
export function agentsDir(): string {
  return path.join(process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock'), 'agents');
}

export function loadAgents(): Record<string, AgentManifest> {
  const all = { ...BUILTIN_AGENTS };
  let entries: string[] = [];
  try {
    entries = fs.readdirSync(agentsDir());
  } catch {
    return all; // no agents dir yet
  }
  for (const f of entries) {
    if (!f.endsWith('.json')) continue;
    try {
      const m = JSON.parse(fs.readFileSync(path.join(agentsDir(), f), 'utf8')) as AgentManifest;
      if (m && typeof m.id === 'string' && typeof m.command === 'string') all[m.id] = m;
    } catch {
      // bad manifest file: skip (logged at spawn if requested by id)
    }
  }
  return all;
}

export interface ResolvedAgent {
  id: string; // 'pi' | 'bash' | <manifest> | 'cmd' (raw --cmd)
  cmd: string;
  env: Record<string, string>;
  manifest: AgentManifest;
}

export function resolveAgent(id: string | undefined, model?: string | null): ResolvedAgent | null {
  const agents = loadAgents();
  const m = agents[id ?? 'pi'];
  if (!m) return null;
  const parts = [m.command, ...(m.args ?? [])];
  if (model && m.modelFlag) parts.push(m.modelFlag, model);
  return { id: m.id, cmd: parts.join(' '), env: { ...(m.env ?? {}) }, manifest: m };
}

// Which RuntimeAdapter serves this manifest (v3.1). Legacy manifests without
// an explicit runtime are derived: runner flock-rpc -> pi, bare bash -> bash,
// anything else -> 'cmd' (raw window, no adapter).
export function manifestRuntime(m: AgentManifest): string {
  if (m.runtime) return m.runtime;
  if (m.runner === 'flock-rpc') return 'pi';
  if (m.command === 'bash') return 'bash';
  return 'cmd';
}

// The runtime of a pod by its stored agent id (the id may be 'pm', 'pi', a
// custom manifest, or 'cmd' for raw commands — the runtime comes from the
// manifest, not from the id).
export function podRuntime(agentId: string | null): string {
  if (!agentId || agentId === 'cmd') return 'cmd';
  const r = resolveAgent(agentId, null);
  return r ? manifestRuntime(r.manifest) : 'cmd';
}

// ── Config projection (per-pod PI_CODING_AGENT_DIR, symlinked) ──────────
// Per-pod PI_CODING_AGENT_DIR needs the provider/model config + extension
// cache. Symlinks keep ONE source of truth (no secret copies).

export function userPiAgentDir(): string {
  return process.env.PI_CODING_AGENT_DIR ?? path.join(os.homedir(), '.pi', 'agent');
}

const PROJECTED_FILES = ['models.json', 'models-store.json', 'settings.json', 'auth.json'];

export function projectPodConfig(userAgentDir: string, podAgentDir: string): void {
  fs.mkdirSync(podAgentDir, { recursive: true });
  for (const f of [...PROJECTED_FILES, 'git']) {
    const src = path.join(userAgentDir, f);
    const dst = path.join(podAgentDir, f);
    if (!fs.existsSync(src)) continue;
    try {
      fs.rmSync(dst, { force: true, recursive: true });
      fs.symlinkSync(src, dst);
    } catch {
      // projection is best-effort: a missing optional file is not fatal
    }
  }
}

// The pod dir is the only part of $HOME visible to the pi sandbox (bwrap
// masks /home, binds the workspace rw), so the CLI must live INSIDE the pod
// dir: a snapshot of dist/ + a shim that points at the pod's unix socket and
// carries the token (the sandbox env allowlist has no FLOCK_* vars). A unix
// socket is a file, not a route — it works even at untrusted (no-net) level.
// ponytail: the CLI is a spawn-time snapshot; relaunch re-copies it.
export function installPodCli(podDir: string, token: string): void {
  const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
  const cliDir = path.join(podDir, '.flock-cli');
  fs.rmSync(cliDir, { recursive: true, force: true });
  fs.cpSync(path.join(repoRoot, 'dist'), cliDir, { recursive: true });
  const binJs = path.join(cliDir, 'bin.js');
  const socket = path.join(podDir, 'core.sock');
  const binDir = path.join(podDir, 'bin');
  fs.mkdirSync(binDir, { recursive: true });
  const shim = path.join(binDir, 'flock');
  fs.writeFileSync(
    shim,
    `#!/bin/sh\nexec env FLOCK_SOCKET=${JSON.stringify(socket)} FLOCK_TOKEN=${JSON.stringify(token)} /usr/bin/node ${JSON.stringify(binJs)} "$@"\n`,
    { mode: 0o700 },
  );
}

// Fallback model for pod spawn when --model is not given: the first model
// declared in the user's models.json (single-provider local setups).
export function firstUserModel(): string | null {
  try {
    const mj = JSON.parse(fs.readFileSync(path.join(userPiAgentDir(), 'models.json'), 'utf8')) as {
      providers?: Record<string, { models?: { id?: string }[] }>;
    };
    for (const [pid, p] of Object.entries(mj.providers ?? {})) {
      const m = p.models?.[0];
      if (m?.id) return `${pid}/${m.id}`;
    }
  } catch {
    /* no models.json: pi decides (or fails visibly) */
  }
  return null;
}
