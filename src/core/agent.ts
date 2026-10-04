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

export const THINKING_LEVELS = ['off', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max'] as const;
export type ThinkingLevel = (typeof THINKING_LEVELS)[number];

// MCP server config for an agent's pods (pod-level mcp.json format).
export interface McpServerConfig {
  command?: string;
  args?: string[];
  env?: Record<string, string>;
  url?: string;
  headers?: Record<string, string>;
}

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
  // v3.3: base manifests (builtin id or <FLOCK_HOME>/agents/<id>.json),
  // merged in order — the importer wins on scalars, arrays concatenate,
  // guidance merges by id.
  imports?: string[];
  // v3.3: named override sets; picked with --profile at spawn/relaunch.
  profiles?: Record<string, Partial<AgentManifest>>;
  // pi-specific first-class axes (mapped to pi CLI flags by the pi
  // adapter/runner; ignored by other runtimes — ponytail: claude/codex may
  // map some later, e.g. thinking -> --effort).
  thinking?: ThinkingLevel;
  tools?: string[]; // allowlist of tool names
  excludeTools?: string[]; // denylist of tool names
  skills?: string[]; // paths to skill files/dirs (repeatable --skill)
  noSkills?: boolean; // disable skills discovery
  extensions?: string[]; // paths or builtin:<name> (repeatable --extension)
  noExtensions?: boolean; // disable extension discovery
  // MCP servers for this agent's pods: written to the pod's
  // <PI_CODING_AGENT_DIR>/mcp.json at spawn/relaunch (pod-level, NOT the
  // user's ~/.pi/agent/mcp.json). Replaces any previous mcp.json.
  mcp?: Record<string, McpServerConfig>;
  systemPrompt?: string; // replace default pi system prompt
  appendSystemPrompt?: string[]; // append text/file contents (repeatable flag)
  noContextFiles?: boolean; // skip AGENTS.md/CLAUDE.md discovery
}

import { PM_PROTOCOL } from './pm-protocol.js';

export const BUILTIN_AGENTS: Record<string, AgentManifest> = {
  pi: { id: 'pi', command: 'pi', modelFlag: '--model', runner: 'flock-rpc', trust: 'approve', trustLevel: 'dev' },
  bash: { id: 'bash', command: 'bash' },
  // claude-code TUI: per-pod config home (<pod>/.claude, the adapter sets
  // CLAUDE_CONFIG_DIR), auth via env (local Anthropic-compatible endpoint).
  // Override with a custom manifest for other providers/models.
  claude: {
    id: 'claude',
    command: 'claude',
    runtime: 'claude',
    modelFlag: '--model',
    env: {
      ANTHROPIC_BASE_URL: 'http://192.168.1.114:8000',
      ANTHROPIC_API_KEY: 'flock-local',
    },
    // the local model accepts xhigh/medium/low effort; "high" -> 500
    args: ['--effort', 'medium'],
  },
  // codex-cli: pane-hosted exec bridge (codex-bridge.js); the model provider
  // is projected into the pod CODEX_HOME config.toml by the codex adapter
  // (in-core responses shim -> vLLM, FLOCK_CODEX_UPSTREAM, default below).
  codex: { id: 'codex', command: 'codex', runtime: 'codex' },
  // C8: the pm is a regular pod (no core subsystem) — the coordinator of
  // the team. Woken by interest events (inbox/poke), decides with the
  // pod-scoped ops it already holds.
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
      // T2: a user manifest may omit `command` — it is inherited from the
      // imports graph (flock agents new skeleton: {id, imports, profiles}).
      // command is validated at resolve time.
      if (m && typeof m.id === 'string') all[m.id] = m;
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

// v3.3: one-sided merge — `ext` overrides `base`. Scalars: ext wins when set.
// args concatenate (base first); env is key-merged; guidance merges by id
// (ext entry with the same id replaces base's, others append). Imports and
// profiles of `base` are NOT inherited — only the importer's own graph.
// pi axes (T1): scalars (thinking/noSkills/noExtensions/noContextFiles) —
// ext wins; arrays (tools/excludeTools/skills/extensions/appendSystemPrompt)
// — concat (like args); mcp — merged per server name (an ext entry with the
// same name replaces the server wholesale).
export function mergeManifests(base: AgentManifest, ext: Partial<AgentManifest>): AgentManifest {
  const g = new Map<string, { id: string; content: string }>();
  for (const e of base.guidance ?? []) g.set(e.id, e);
  for (const e of ext.guidance ?? []) g.set(e.id, e);
  const defined = Object.fromEntries(Object.entries(ext).filter(([, v]) => v !== undefined));
  const merged: AgentManifest = {
    ...base,
    ...defined,
    args: [...(base.args ?? []), ...(ext.args ?? [])],
    env: { ...(base.env ?? {}), ...(ext.env ?? {}) },
    guidance: [...g.values()],
    tools: [...(base.tools ?? []), ...(ext.tools ?? [])],
    excludeTools: [...(base.excludeTools ?? []), ...(ext.excludeTools ?? [])],
    skills: [...(base.skills ?? []), ...(ext.skills ?? [])],
    extensions: [...(base.extensions ?? []), ...(ext.extensions ?? [])],
    appendSystemPrompt: [...(base.appendSystemPrompt ?? []), ...(ext.appendSystemPrompt ?? [])],
    mcp: { ...(base.mcp ?? {}), ...(ext.mcp ?? {}) },
    // never bake a profile's own imports into the result as active graph
    imports: ext.imports ?? base.imports,
    profiles: ext.profiles ?? base.profiles,
  };
  // empty arrays are not a signal (a profile that sets no tools keeps none):
  // drop the axis when the merge produced nothing, so `tools: []` never
  // serializes into a runner flag (which would mean "allow nothing")
  for (const k of ['tools', 'excludeTools', 'skills', 'extensions', 'appendSystemPrompt'] as const) {
    if ((merged[k] ?? []).length === 0) delete merged[k];
  }
  if (merged.mcp && Object.keys(merged.mcp).length === 0) delete merged.mcp;
  return merged;
}

// Resolve the imports graph (in order, last wins) with cycle detection.
export function resolveManifest(base: AgentManifest, agents: Record<string, AgentManifest>, seen: Set<string> = new Set()): AgentManifest {
  const id = base.id;
  if (seen.has(id)) throw new Error(`manifest import cycle: ${[...seen, id].join(' -> ')}`);
  seen.add(id);
  let m: AgentManifest = base;
  for (const imp of base.imports ?? []) {
    const dep = agents[imp];
    if (!dep) throw new Error(`manifest ${id}: unknown import: ${imp}`);
    m = mergeManifests(resolveManifest(dep, agents, new Set(seen)), m);
  }
  return m;
}

export function resolveAgent(id: string | undefined, model?: string | null, profile?: string | null): ResolvedAgent | null {
  const agents = loadAgents();
  const raw = agents[id ?? 'pi'];
  if (!raw) return null;
  let m = resolveManifest(raw, agents);
  if (profile) {
    const p = m.profiles?.[profile];
    if (!p) throw new Error(`manifest ${m.id}: unknown profile: ${profile} (has: ${Object.keys(m.profiles ?? {}).join(', ') || 'none'})`);
    m = mergeManifests(m, p);
  }
  // T1 validation: thinking is a small closed set — a typo here would fail
  // deep in pi's flag parser with a worse message. The other axes are
  // pass-through (pi fails visibly on a bad tool name/skill path).
  if (m.thinking != null && !THINKING_LEVELS.includes(m.thinking)) {
    throw new Error(`manifest ${m.id}: unknown thinking level: ${m.thinking} (valid: ${THINKING_LEVELS.join(', ')})`);
  }
  // T2: a skeleton manifest may omit command, but the RESOLVED manifest
  // must have one (inherited from imports, or its own).
  if (typeof m.command !== 'string' || !m.command) {
    throw new Error(`manifest ${m.id}: no command (set "command" or import a manifest that has one, e.g. "pi")`);
  }
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
  try {
    const r = resolveAgent(agentId, null);
    return r ? manifestRuntime(r.manifest) : 'cmd';
  } catch {
    // T2: an invalid manifest (no command, bad thinking) is not a known
    // runtime — the pod keeps its legacy 'cmd' behavior, the error surfaces
    // at spawn/relaunch where resolveAgent is called for real.
    return 'cmd';
  }
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
