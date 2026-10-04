// team — `flock team up <pods.yaml>`: declare a team, spawn it idempotently.
// The file is a flat YAML subset (no dependencies: flock has zero npm deps):
//
//   pods:
//     dev:
//       agent: pi            # agent id (builtin or ~/.flock/agents/<id>.json)
//       model: cat-vllm/...
//       profile: quiet       # manifest profile
//       posture: floor       # launch posture (pi)
//       guidance: |          # extra AGENTS.md block (managed, id `team:<role>`)
//         line one
//         line two
//
// team up is a reconcile: live pods are only refreshed (guidance re-merged,
// idempotent), missing/closed pods are spawned. It never kills live pods.
import type { OpError } from './ops.js';

export interface TeamPodSpec {
  agent?: string;
  model?: string;
  profile?: string;
  posture?: 'floor' | 'full_bypass';
  guidance?: string;
  // C12: workspace ref — `ws:<name>` resolves through the profile's
  // workspace.json (repos.<name> or `ws:root`); an absolute path works too.
  dir?: string;
}

export interface TeamSpec {
  pods: Record<string, TeamPodSpec>;
}

export class TeamParseError extends Error {}

type Err = (msg: string) => never;

// indent-based parser for the documented subset: top-level `pods:` ->
// role (indent 2) -> scalar fields (indent 4) + `guidance: |` block
export function parseTeamYaml(src: string): TeamSpec {
  const err: Err = (msg) => {
    throw new TeamParseError(msg);
  };
  const lines = src.split('\n');
  const pods: Record<string, TeamPodSpec> = {};
  let inPods = false;
  let role: string | null = null;
  let inGuidance = false;
  let guidanceIndent = -1;
  let guidanceLines: string[] = [];

  const flushGuidance = () => {
    if (role && inGuidance) {
      const pod = pods[role] ?? (pods[role] = {});
      pod.guidance = guidanceLines.join('\n').trimEnd();
      inGuidance = false;
    }
  };

  for (const raw of lines) {
    if (!raw.trim() || raw.trim().startsWith('#')) continue;
    if (/\S/.test(raw) && /^\s+/.test(raw) === false) {
      // top-level key
      flushGuidance();
      role = null;
      if (raw === 'pods:') {
        inPods = true;
        continue;
      }
      err(`unknown top-level key: ${raw.trim()} (want 'pods:')`);
    }
    if (!inPods) err(`content before 'pods:'`);
    const indent = raw.length - raw.trimStart().length;
    const content = raw.trim();

    if (indent === 2 && !inGuidance) {
      flushGuidance();
      if (!content.endsWith(':')) err(`role line must end with ':': ${content}`);
      const name = content.slice(0, -1).trim();
      if (!/^[a-z][a-z0-9-]*$/.test(name)) err(`bad pod role: ${name} (want [a-z][a-z0-9-]*)`);
      role = name;
      if (!pods[role]) pods[role] = {};
      continue;
    }
    if (indent >= 4 && !inGuidance) {
      if (!role) err(`field outside a pod: ${content}`);
      const m = content.match(/^([A-Za-z_][A-Za-z0-9_-]*):\s*(.*)$/);
      if (!m) err(`bad field: ${content}`);
      const [, key, value] = m;
      const pod = pods[role] ?? (pods[role] = {});
      if (key === 'guidance') {
        if (value === '|' || value === '') {
          inGuidance = true;
          guidanceIndent = indent;
          guidanceLines = [];
        } else {
          pod.guidance = value;
        }
        continue;
      }
      if (value === '') err(`empty value for ${role}.${key} (use a block '|' for multi-line)`);
      const allowed: (keyof TeamPodSpec)[] = ['agent', 'model', 'profile', 'posture', 'dir'];
      if (!allowed.includes(key as keyof TeamPodSpec)) err(`unknown field ${role}.${key} (allowed: ${allowed.join(', ')}, guidance)`);
      if (key === 'posture' && value !== 'floor' && value !== 'full_bypass') err(`${role}.posture: want floor|full_bypass`);
      (pod as Record<string, unknown>)[key] = value;
      continue;
    }
    if (inGuidance) {
      if (indent > guidanceIndent) {
        guidanceLines.push(raw.slice(guidanceIndent + 2));
        continue;
      }
      flushGuidance();
      // fall through: this line is a new field of the same role
      if (indent < 4) {
        // reprocess as a role/top-level line
        if (indent === 2) {
          if (!content.endsWith(':')) err(`role line must end with ':': ${content}`);
          const name = content.slice(0, -1).trim();
          if (!/^[a-z][a-z0-9-]*$/.test(name)) err(`bad pod role: ${name}`);
          role = name;
          if (!pods[role]) pods[role] = {};
        } else {
          err(`unexpected indent after guidance block: ${content}`);
        }
        continue;
      }
      const m = content.match(/^([A-Za-z_][A-Za-z0-9_-]*):\s*(.*)$/);
      if (!m) err(`bad field: ${content}`);
      const [, key, value] = m;
      const pod = pods[role!] ?? (pods[role!] = {});
      if (key === 'guidance') {
        if (value === '|' || value === '') {
          inGuidance = true;
          guidanceIndent = indent;
          guidanceLines = [];
        } else pod.guidance = value;
      } else {
        const allowed: (keyof TeamPodSpec)[] = ['agent', 'model', 'profile', 'posture', 'dir'];
        if (!allowed.includes(key as keyof TeamPodSpec)) err(`unknown field ${role}.${key}`);
        if (value === '') err(`empty value for ${role}.${key}`);
        if (key === 'posture' && value !== 'floor' && value !== 'full_bypass') err(`${role}.posture: want floor|full_bypass`);
        (pod as Record<string, unknown>)[key] = value;
      }
    }
  }
  flushGuidance();
  if (!inPods) err(`missing top-level 'pods:'`);
  if (Object.keys(pods).length === 0) err(`no pods declared`);
  return { pods };
}
