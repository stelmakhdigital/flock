// C12: workspace — a named inventory of repos the operator declares once,
// so team files don't hard-code absolute paths per machine.
//
// Filesystem-canonical: <FLOCK_HOME>/workspace.json (per profile, since
// FLOCK_HOME is per profile):
//   { "root": "/home/u/Code", "repos": { "flock": "/home/u/Code/PROJECTS/flock" },
//     "knowledge": "/home/u/notes" }
//
// Surface: `flock workspace show` (via op), team.yaml `dir: ws:<name>`
// resolution (pod_spawn dir), and the `flock status` inventory line.
// There is no `workspace set` op — the file is edited by the operator
// (it's a declaration, not state).

import fs from 'node:fs';
import path from 'node:path';

export interface Workspace {
  root?: string;
  repos: Record<string, string>;
  knowledge?: string;
}

export function workspacePath(home: string): string {
  return path.join(home, 'workspace.json');
}

// Read + validate the workspace file. Missing = an empty workspace
// (a fresh profile has none yet). Bad JSON / bad shape = a loud error:
// a broken declaration should fail, not silently shrink.
export function readWorkspace(home: string): Workspace {
  const p = workspacePath(home);
  let raw: string;
  try {
    raw = fs.readFileSync(p, 'utf8');
  } catch {
    return { repos: {} };
  }
  let obj: unknown;
  try {
    obj = JSON.parse(raw);
  } catch {
    throw new Error(`${p}: not valid JSON`);
  }
  if (typeof obj !== 'object' || obj === null || Array.isArray(obj)) {
    throw new Error(`${p}: top-level must be an object`);
  }
  const o = obj as Record<string, unknown>;
  const repos: Record<string, string> = {};
  if (o.repos !== undefined) {
    if (typeof o.repos !== 'object' || o.repos === null || Array.isArray(o.repos)) {
      throw new Error(`${p}: "repos" must be an object {name: path}`);
    }
    for (const [k, v] of Object.entries(o.repos)) {
      if (typeof v !== 'string' || !v) throw new Error(`${p}: repos.${k} must be a non-empty path`);
      repos[k] = v;
    }
  }
  const ws: Workspace = { repos };
  if (typeof o.root === 'string' && o.root) ws.root = o.root;
  if (typeof o.knowledge === 'string' && o.knowledge) ws.knowledge = o.knowledge;
  return ws;
}

// Resolve a `ws:<name>` reference to an absolute path. The name maps to
// repos.<name>; `ws:root` is the root itself. Unknown = a loud error
// (a typo in a team file must not spawn pods into a guessed directory).
export function resolveWorkspaceRef(home: string, ref: string): string {
  if (!ref.startsWith('ws:')) throw new Error(`not a workspace ref (want ws:<name>): ${ref}`);
  const name = ref.slice('ws:'.length).trim();
  const ws = readWorkspace(home);
  if (name === 'root') {
    if (!ws.root) throw new Error(`workspace has no "root" (check ${workspacePath(home)})`);
    return ws.root;
  }
  const p = ws.repos[name];
  if (!p) {
    const have = Object.keys(ws.repos);
    throw new Error(`workspace repo not found: ${name} (have: ${have.join(', ') || 'none'})`);
  }
  return p;
}
