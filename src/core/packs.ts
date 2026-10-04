// C12: context packs — filesystem-canonical, in-memory nothing.
//
// A pack is a directory: <FLOCK_HOME>/packs/<name>/
//   pack.json  {id, files: ["a.md", "b/notes.txt"]}
//   + the files themselves (any text)
//
// The bundle (what `pack_show` returns and what goes into the pod's
// AGENTS.md as managed block `pack:<name>`) is assembled on demand by
// concatenating the files in `files` order with a header per file.
// There is NO cache and NO sqlite — the filesystem is canonical (the
// OpenRIG model); assembly is cheap (a handful of small reads) and
// happens only at spawn/relaunch and on explicit pack_show.
// ponytail: a core-side in-memory cache was considered and rejected —
// the invalidation machinery costs more than the reads it saves.

import fs from 'node:fs';
import path from 'node:path';

export interface PackMeta {
  id: string;
  files: string[];
  dir: string;
}

export function packsDir(home: string): string {
  return path.join(home, 'packs');
}

const NAME_RE = /^[a-z][a-z0-9-]{0,63}$/;

export function validatePackName(name: string): string | null {
  if (!NAME_RE.test(name)) return `bad pack name: ${name} (want [a-z][a-z0-9-]*)`;
  return null;
}

function packDir(home: string, name: string): string {
  const err = validatePackName(name);
  if (err) throw new Error(err);
  return path.join(packsDir(home), name);
}

// List all packs: <dir>/pack.json present = a pack. Missing packs dir = [].
export function listPacks(home: string): { name: string; files: number }[] {
  let entries: string[];
  try {
    entries = fs.readdirSync(packsDir(home), { withFileTypes: true }).map((e) => e.name);
  } catch {
    return [];
  }
  const out: { name: string; files: number }[] = [];
  for (const name of entries.sort()) {
    try {
      const meta = readPackMeta(home, name);
      out.push({ name, files: meta.files.length });
    } catch {
      // not a pack (no pack.json) — not a pack, skip
    }
  }
  return out;
}

// Read + validate a pack's pack.json. The `files` list must be non-empty
// and stay inside the pack dir (.. traversal = a broken pack, not an
// operator foot-gun: fail the read, don't read outside).
export function readPackMeta(home: string, name: string): PackMeta {
  const dir = packDir(home, name);
  let raw: string;
  try {
    raw = fs.readFileSync(path.join(dir, 'pack.json'), 'utf8');
  } catch (e) {
    throw new Error(`pack ${name}: cannot read pack.json (${e instanceof Error ? e.message : String(e)})`);
  }
  let meta: { id?: unknown; files?: unknown };
  try {
    meta = JSON.parse(raw);
  } catch {
    throw new Error(`pack ${name}: pack.json is not valid JSON`);
  }
  if (!Array.isArray(meta.files) || meta.files.length === 0) {
    throw new Error(`pack ${name}: pack.json needs a non-empty "files" array`);
  }
  for (const f of meta.files) {
    if (typeof f !== 'string' || !f) throw new Error(`pack ${name}: files entries must be non-empty strings`);
    const resolved = path.resolve(dir, f);
    if (!resolved.startsWith(dir + path.sep)) {
      throw new Error(`pack ${name}: file escapes the pack dir: ${f}`);
    }
  }
  const id = typeof meta.id === 'string' && meta.id ? meta.id : name;
  return { id, files: meta.files as string[], dir };
}

// Assemble the paste-ready bundle: one header per file + the content.
// Missing file = an explicit placeholder in the bundle (a pack that
// references a missing file should be VISIBLE, not silently shrunken).
export function buildPackBundle(home: string, name: string): string {
  const meta = readPackMeta(home, name);
  const parts: string[] = [
    `# pack: ${meta.id}`,
    `source: ${meta.dir}`,
    `files: ${meta.files.length}`,
  ];
  for (const f of meta.files) {
    const p = path.join(meta.dir, f);
    let content: string;
    try {
      content = fs.readFileSync(p, 'utf8').trimEnd();
    } catch {
      content = `(MISSING FILE: ${f})`;
    }
    parts.push('', `## ${f}`, '', content);
  }
  return parts.join('\n') + '\n';
}
