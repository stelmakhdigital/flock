// C12: plugins — READ-ONLY inspection of the host's pi extensions.
//
// Surface: `flock plugins ls` / `flock plugins show <source>`. The pods
// inherit the host's user packages (their PI_CODING_AGENT_DIR is isolated,
// but user-level installs from settings are visible), so this reports
// what the pods can see: parsed from `pi list` output.
//
// NO install/remove: plugin management stays an explicit operator copy
// into ~/.pi/agent (their model: install is a planned verb, not ours).
// flock only reports.

import fs from 'node:fs';
import { spawn } from 'node:child_process';

export interface PluginEntry {
  source: string;
  path?: string;
  filtered?: boolean;
  kind: 'user' | 'builtin';
}

// Parse `pi list` output. Structure (observed):
//   User packages:
//     git:github.com/user/repo            <- source (indent 2)
//       /path/to/checkout                 <- path (indent 4)
//   Builtin extensions:
//     builtin:web
// A section header ends the previous entry. "(filtered)" = installed but
// filtered out of the active set — surfaced, not hidden.
export function parsePiList(raw: string): PluginEntry[] {
  const out: PluginEntry[] = [];
  let kind: 'user' | 'builtin' = 'user';
  let last: PluginEntry | null = null;
  for (const line of raw.split('\n')) {
    const t = line.trim();
    if (!t) continue;
    const indent = line.length - line.trimStart().length;
    const isHeader = indent === 0 && /:/.test(t);
    const isSource = indent === 2 && /^(git|builtin|file|npm):/.test(t);
    const isPath = indent >= 4 && !t.startsWith(' ');
    if (isHeader) {
      kind = /user/i.test(t) ? 'user' : 'builtin';
      last = null;
      continue;
    }
    if (isSource) {
      last = { source: t, kind };
      if (/\(filtered\)/.test(t)) {
        last.filtered = true;
        last.source = t.replace(/\s*\(filtered\)\s*/, '').trim();
      }
      out.push(last);
      continue;
    }
    if (isPath && last) {
      last.path = t;
      continue;
    }
    // anything else (extra prose) is skipped
  }
  return out;
}

// Run the host's `pi list`.
export async function piList(): Promise<{ ok: boolean; entries: PluginEntry[]; error?: string }> {
  return new Promise((resolve) => {
    let child;
    try {
      child = spawn('pi', ['list'], { stdio: ['ignore', 'pipe', 'pipe'] });
    } catch (e) {
      resolve({ ok: false, entries: [], error: String(e) });
      return;
    }
    let out = '';
    let err = '';
    child.stdout?.on('data', (d) => (out += d));
    child.stderr?.on('data', (d) => (err += d));
    child.on('error', (e) => resolve({ ok: false, entries: [], error: `pi not found: ${e.message}` }));
    child.on('close', (code) => {
      if (code !== 0) resolve({ ok: false, entries: [], error: `pi list exited ${code}: ${err.slice(0, 300)}` });
      else resolve({ ok: true, entries: parsePiList(out) });
    });
  });
}

// `plugins show <source>`: the entry + the SKILL.md/README of the package
// dir when present (a light inspection, no content copying).
export function pluginShowDetail(entry: PluginEntry): string | null {
  if (!entry.path) return null;
  try {
    for (const f of ['SKILL.md', 'README.md', 'package.json']) {
      const p = `${entry.path}/${f}`;
      if (fs.existsSync(p)) {
        const raw = fs.readFileSync(p, 'utf8');
        return raw.length > 4000 ? raw.slice(0, 4000) + '\n… (truncated)' : raw;
      }
    }
  } catch {
    /* not readable: metadata only */
  }
  return null;
}
