// claude-protocol — pure claude-code runtime protocol (no side effects):
// session-token rules, arg/env builders, boot-dialog recipes, ready scan.
// Keeps the adapter testable hermetically (same split as runner-protocol).

import fs from 'node:fs';
import path from 'node:path';

// ── Session tokens ──────────────────────────────────────────────────────────
// Claude Code transcripts live in <config>/projects/<cwd-slug>/<uuid>.jsonl.
// The resume token is the FILENAME uuid (NOT the sessionId in the per-pid
// registry <config>/sessions/<pid>.json — that one does not resolve).
// Observed live: --resume <transcript-uuid> works; --resume <registry-id>
// says "No conversation found".

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function validateClaudeSessionToken(raw: string): boolean {
  return UUID_RE.test(raw);
}

// /tmp/foo -> -tmp-foo (observed claude slug rule for the projects dir)
export function claudeProjectsDir(configDir: string, cwd: string): string {
  // slug rule observed live: /home/u/.flock/pods/ctest -> -home-...-flock-pods-ctest
  // (dots also become dashes)
  const slug = cwd.replace(/[/.]/g, '-');
  return path.join(configDir, 'projects', slug);
}

export interface ClaudeSession { token: string; file: string }

// Newest transcript by mtime = the session the pod was last running.
export function latestClaudeSession(projectsDir: string): ClaudeSession | null {
  let entries: string[] = [];
  try {
    entries = fs.readdirSync(projectsDir);
  } catch {
    return null;
  }
  let best: ClaudeSession | null = null;
  let bestM = 0;
  for (const f of entries) {
    if (!f.endsWith('.jsonl')) continue;
    const token = f.slice(0, -'.jsonl'.length);
    if (!UUID_RE.test(token)) continue;
    const p = path.join(projectsDir, f);
    try {
      const m = fs.statSync(p).mtimeMs;
      if (m > bestM) { bestM = m; best = { token, file: p }; }
    } catch {
      /* vanished */
    }
  }
  return best;
}

// ── Launch ──────────────────────────────────────────────────────────────────
export interface ClaudeLaunch {
  model?: string;
  effort?: 'low' | 'medium' | 'high' | 'xhigh';
  resumeToken?: string;
  fork?: boolean;
  permissionMode?: string;
}

export function buildClaudeArgs(o: ClaudeLaunch): string[] {
  const a: string[] = [];
  if (o.model) a.push('--model', o.model);
  if (o.effort) a.push('--effort', o.effort);
  if (o.resumeToken) {
    a.push('--resume', o.resumeToken);
    if (o.fork) a.push('--fork-session');
  }
  if (o.permissionMode) a.push('--permission-mode', o.permissionMode);
  return a;
}

// Extra env every claude pod gets (beyond the manifest env):
// - CLAUDE_CONFIG_DIR: per-pod config+sessions home (isolation; also avoids
//   a root-owned ~/.claude)
// - unknown-model window enforcement off: our models are not in claude's
//   catalog (local vLLM) — observed 500 "reasoning effort high" + window
//   warning without it
export const CLAUDE_FIXED_ENV = {
  CLAUDE_CODE_DISABLE_UNKNOWN_MODEL_WINDOW_ENFORCEMENT: '1',
} as const;

export function claudeConfigDir(podDir: string): string {
  return path.join(podDir, '.claude');
}

// Fresh-config first launch does a connectivity check to api.anthropic.com
// and hard-exits when it is unreachable (observed live: "Unable to connect
// to Anthropic services"), while a config with completed onboarding + an
// approved custom key starts fine (local endpoint). Pre-seed that state so
// first launch is deterministic. A PARTIAL .claude.json left by an aborted
// first launch must be merged, not skipped (it exists, onboarding is not done).
export function seedClaudeConfig(configDir: string, apiKey?: string): void {
  fs.mkdirSync(configDir, { recursive: true });
  const statePath = path.join(configDir, '.claude.json');
  let state: Record<string, unknown> = {};
  try {
    state = JSON.parse(fs.readFileSync(statePath, 'utf8')) as Record<string, unknown>;
  } catch {
    /* missing or bad -> create */
  }
  let changed = false;
  if (state.hasCompletedOnboarding !== true) {
    state.hasCompletedOnboarding = true;
    changed = true;
  }
  const resp = (state.customApiKeyResponses ?? null) as { approved?: string[]; rejected?: string[] } | null;
  if (apiKey && !(resp?.approved ?? []).includes(apiKey)) {
    state.customApiKeyResponses = { approved: [...(resp?.approved ?? []), apiKey], rejected: resp?.rejected ?? [] };
    changed = true;
  }
  if (changed) fs.writeFileSync(statePath, JSON.stringify(state, null, 2), { mode: 0o600 });
  const settingsPath = path.join(configDir, 'settings.json');
  if (!fs.existsSync(settingsPath)) {
    fs.writeFileSync(settingsPath, JSON.stringify({ theme: 'dark' }, null, 2));
  }
}

// ── Boot dialogs (first launch per config dir / per cwd) ────────────────────
// Claude Code asks a chain of one-time questions before the prompt:
//   theme select -> API key confirm -> security notice -> folder trust.
// Each is answered with a fixed key sequence; the marker is a stable
// substring of the prompt. Order matters (they appear sequentially).
export interface ClaudeBootDialog { marker: string; keys: string[] }

export const CLAUDE_BOOT_DIALOGS: ClaudeBootDialog[] = [
  { marker: 'Choose the text style', keys: ['Enter'] },
  { marker: 'Do you want to use this API key', keys: ['Up', 'Enter'] },
  { marker: 'Enter to continue', keys: ['Enter'] },
  { marker: 'Yes, I trust this folder', keys: ['Down', 'Enter'] },
  // MCP server approval (the operator's global servers leak in from
  // ~/.claude.json): reject all - pods don't get them
  { marker: 'Space to select', keys: ['Escape'] },
  // bypassPermissions acceptance: "Yes, I accept" is the second option
  { marker: 'accept all responsibility', keys: ['Down', 'Enter'] },
];

// Ready = an idle input prompt (a line starting with the ❯ cursor) and no
// open dialog/confirmation on screen. Open confirmations always render a
// footer with a key hint ("Enter to confirm", "Space to select", ...); the
// idle footer ("auto mode on · ← for agents") has none. The MCP dialog's
// checkbox lines also start with ❯ - the footer guard is what keeps them
// from being a false ready.
const DIALOG_HINTS = [
  'Enter to confirm',
  'Esc to cancel',
  'Esc to reject',
  'Space to select',
  'to continue',
  'Do you want to use this API key',
];

export function claudePaneReady(paneText: string): boolean {
  if (DIALOG_HINTS.some((h) => paneText.includes(h))) return false;
  return /(^|\n)\s*❯/.test(paneText);
}

// The transcript grows when the agent is working; "esc to interrupt" is the
// in-turn status line.
export function claudePaneBusy(paneText: string): boolean {
  return paneText.includes('esc to interrupt');
}

// ── Delivery ack: the transcript is the typed signal ────────────────────────
// A pasted prompt lands in the transcript (a user entry) within seconds,
// independent of how long the turn takes. Fingerprint = newest file by mtime
// + its size; growth (or first appearance) = delivered.

export type ClaudeTranscriptFp = { file: string; size: number } | null;

export function claudeTranscriptFp(projectsDir: string): ClaudeTranscriptFp {
  const s = latestClaudeSession(projectsDir);
  if (!s) return null;
  try {
    return { file: s.file, size: fs.statSync(s.file).size };
  } catch {
    return null;
  }
}

export async function waitForTranscriptGrowth(
  projectsDir: string,
  before: ClaudeTranscriptFp,
  timeoutMs: number,
  pollMs = 500,
): Promise<{ grown: boolean; after: ClaudeTranscriptFp }> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const after = claudeTranscriptFp(projectsDir);
    const grown =
      after !== null &&
      (before === null || after.file !== before.file || after.size > before.size);
    if (grown) return { grown: true, after };
    await new Promise((r) => setTimeout(r, pollMs));
  }
  return { grown: false, after: claudeTranscriptFp(projectsDir) };
}
