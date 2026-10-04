// C14: named teams + snapshots. Filesystem-canonical (OpenRIG model):
// teams live in ~/.flock/teams/<name>.yaml, snapshots in
// ~/.flock/snapshots/<team>/<mono-id>/. sqlite is not touched — the pod
// rows stay the canonical run state; the snapshot is a checkpoint.
//
// <mono-id> is monotonic (not a timestamp): their "newest" rule is
// lexicographic/numeric over the id, immune to clock skew.
import fs from 'node:fs';
import path from 'node:path';
import { parseTeamYaml, type TeamSpec, type TeamPodSpec } from './team.js';

export function teamsDir(home: string): string {
  return path.join(home, 'teams');
}

export function snapshotsDir(home: string): string {
  return path.join(home, 'snapshots');
}

const TEAM_NAME_RE = /^[a-z0-9][a-z0-9._-]{0,63}$/i;

export function validateTeamName(name: string): boolean {
  return TEAM_NAME_RE.test(name);
}

export function listTeams(home: string): { name: string; pods: string[] }[] {
  const dir = teamsDir(home);
  let files: string[] = [];
  try {
    files = fs.readdirSync(dir).filter((f) => f.endsWith('.yaml') || f.endsWith('.yml'));
  } catch {
    return [];
  }
  const out: { name: string; pods: string[] }[] = [];
  for (const f of files.sort()) {
    const name = f.replace(/\.(yaml|yml)$/, '');
    try {
      const spec = parseTeamYaml(fs.readFileSync(path.join(dir, f), 'utf8'));
      out.push({ name, pods: Object.keys(spec.pods) });
    } catch {
      // a broken team file: skip in ls (loud on up/down)
    }
  }
  return out;
}

export function readTeamSpec(home: string, name: string): { spec: TeamSpec; file: string } {
  if (!validateTeamName(name)) throw new Error(`invalid team name: ${name}`);
  for (const ext of ['.yaml', '.yml']) {
    const file = path.join(teamsDir(home), name + ext);
    if (fs.existsSync(file)) {
      try {
        return { spec: parseTeamYaml(fs.readFileSync(file, 'utf8')), file };
      } catch (e) {
        throw new Error(`team ${name}: cannot parse ${file}: ${e instanceof Error ? e.message : String(e)}`);
      }
    }
  }
  throw new Error(`no team: ${name} (want ~/.flock/teams/${name}.yaml)`);
}

// ── snapshots ─────────────────────────────────────────────────────────────

export interface SnapshotPod {
  role: string;
  agent: string | null;
  manifestId: string | null;
  profile: string | null;
  model: string | null;
  dir: string;
  // the session file (pi pods only); null = no session to restore
  sessionFile: string | null;
  // the copied session (inside the snapshot dir); null = same
  sessionCopy: string | null;
  // true = the pod is restore-able (pi runtime + a session copy)
  restorable: boolean;
}

export interface Snapshot {
  name: string; // team name
  id: string; // mono-id
  savedAt: string; // ISO
  teamFile: string; // absolute path of the team yaml
  pods: SnapshotPod[];
  // pods the snapshot REFUSED to include (foreign runtime, no session) —
  // the honest note: they are not part of this restore
  skipped: { role: string; reason: string }[];
}

function nextMonoId(teamSnapDir: string): string {
  let max = 0;
  let entries: string[] = [];
  try {
    entries = fs.readdirSync(teamSnapDir);
  } catch {
    /* no snapshots yet */
  }
  for (const e of entries) {
    const n = Number(e);
    if (Number.isInteger(n) && n > max) max = n;
  }
  return String(max + 1);
}

export function saveSnapshot(
  home: string,
  teamName: string,
  info: {
    teamFile: string;
    pods: SnapshotPod[];
    skipped: { role: string; reason: string }[];
  },
): Snapshot {
  const teamSnapDir = path.join(snapshotsDir(home), teamName);
  fs.mkdirSync(teamSnapDir, { recursive: true });
  const id = nextMonoId(teamSnapDir);
  const snapDir = path.join(teamSnapDir, id);
  fs.mkdirSync(path.join(snapDir, 'sessions'), { recursive: true });
  // copy the session files into the snapshot (the snapshot must outlive
  // the pods' seats)
  const pods: SnapshotPod[] = info.pods.map((p) => {
    if (!p.sessionFile || !fs.existsSync(p.sessionFile)) return { ...p, sessionCopy: null, restorable: false };
    const copy = path.join(snapDir, 'sessions', `${p.role}-${path.basename(p.sessionFile)}`);
    fs.copyFileSync(p.sessionFile, copy);
    return { ...p, sessionCopy: copy, restorable: true };
  });
  const snap: Snapshot = {
    name: teamName,
    id,
    savedAt: new Date().toISOString(),
    teamFile: info.teamFile,
    pods,
    skipped: info.skipped,
  };
  fs.writeFileSync(path.join(snapDir, 'snapshot.json'), JSON.stringify(snap, null, 2));
  return snap;
}

export interface SnapshotRef {
  snap: Snapshot;
  dir: string; // the snapshot dir
}

export function readSnapshot(home: string, teamName: string, ref: string): SnapshotRef {
  if (!validateTeamName(teamName)) throw new Error(`invalid team name: ${teamName}`);
  const teamSnapDir = path.join(snapshotsDir(home), teamName);
  let id: string;
  if (ref === 'latest') {
    let entries: string[] = [];
    try {
      entries = fs.readdirSync(teamSnapDir).filter((e) => Number.isInteger(Number(e)));
    } catch {
      throw new Error(`no snapshots for team ${teamName}`);
    }
    if (!entries.length) throw new Error(`no snapshots for team ${teamName}`);
    id = entries.sort((a, b) => Number(b) - Number(a))[0];
  } else if (Number.isInteger(Number(ref))) {
    id = ref;
  } else {
    throw new Error(`bad snapshot ref: ${ref} (want a mono-id or 'latest')`);
  }
  const dir = path.join(teamSnapDir, id);
  const p = path.join(dir, 'snapshot.json');
  if (!fs.existsSync(p)) throw new Error(`no snapshot: ${teamName}/${id}`);
  let snap: Snapshot;
  try {
    snap = JSON.parse(fs.readFileSync(p, 'utf8')) as Snapshot;
  } catch (e) {
    throw new Error(`corrupt snapshot ${teamName}/${id}: ${e instanceof Error ? e.message : String(e)}`);
  }
  return { snap, dir };
}

export function listSnapshots(home: string, teamName: string): { id: string; savedAt: string; pods: number }[] {
  if (!validateTeamName(teamName)) return [];
  const teamSnapDir = path.join(snapshotsDir(home), teamName);
  let entries: string[] = [];
  try {
    entries = fs.readdirSync(teamSnapDir).filter((e) => Number.isInteger(Number(e)));
  } catch {
    return [];
  }
  const out: { id: string; savedAt: string; pods: number }[] = [];
  for (const id of entries.sort((a, b) => Number(a) - Number(b))) {
    const p = path.join(teamSnapDir, id, 'snapshot.json');
    if (!fs.existsSync(p)) continue;
    try {
      const s = JSON.parse(fs.readFileSync(p, 'utf8')) as Snapshot;
      out.push({ id, savedAt: s.savedAt, pods: s.pods.length });
    } catch {
      /* corrupt: skip in list (loud on read) */
    }
  }
  return out;
}

// The spec a snapshot restores: the team spec frozen at save time.
export function snapshotTeamSpec(snap: Snapshot): { pods: Record<string, TeamPodSpec> } {
  // re-read the frozen team file if it still exists; otherwise reconstruct
  // a minimal spec from the pod entries (agent/model/profile are recorded)
  if (fs.existsSync(snap.teamFile)) {
    try {
      return parseTeamYaml(fs.readFileSync(snap.teamFile, 'utf8'));
    } catch {
      /* fall through to reconstruction */
    }
  }
  const pods: Record<string, TeamPodSpec> = {};
  for (const p of snap.pods) {
    pods[p.role] = {
      agent: p.manifestId ?? p.agent ?? undefined,
      model: p.model ?? undefined,
      profile: p.profile ?? undefined,
      dir: p.dir,
    };
  }
  return { pods };
}
