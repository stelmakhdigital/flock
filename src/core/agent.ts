// Agent adapters: ONE generic implementation + declarative manifests.
// Adding a runtime (claude, codex, ...) = a JSON manifest in <FLOCK_HOME>/agents/,
// not new code. This mirrors OpenRig's shape (RuntimeAdapter interface +
// agent.yaml spec), reduced to what our v1 actually needs.
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

export interface AgentManifest {
  id: string;
  command: string;
  modelFlag?: string;
  args?: string[];
  env?: Record<string, string>;
}

export const BUILTIN_AGENTS: Record<string, AgentManifest> = {
  pi: { id: 'pi', command: 'pi', modelFlag: '--model' },
  bash: { id: 'bash', command: 'bash' },
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
}

export function resolveAgent(id: string | undefined, model?: string | null): ResolvedAgent | null {
  const agents = loadAgents();
  const m = agents[id ?? 'pi'];
  if (!m) return null;
  const parts = [m.command, ...(m.args ?? [])];
  if (model && m.modelFlag) parts.push(m.modelFlag, model);
  return { id: m.id, cmd: parts.join(' '), env: { ...(m.env ?? {}) } };
}
