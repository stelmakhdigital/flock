// claude-protocol — hermetic checks (tmp dirs, no network, no tmux).
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  validateClaudeSessionToken,
  claudeProjectsDir,
  latestClaudeSession,
  buildClaudeArgs,
  CLAUDE_BOOT_DIALOGS,
  claudePaneReady,
  claudePaneBusy,
  claudeTranscriptFp,
  waitForTranscriptGrowth,
} from './claude-protocol.js';

// ---- token validation ------------------------------------------------------
assert.ok(validateClaudeSessionToken('c9c97ced-b06a-4220-acfc-a2da47feadb2'));
assert.ok(!validateClaudeSessionToken('/home/x/.pi/sessions/a_dev.jsonl'), 'path is not a claude token');
assert.ok(!validateClaudeSessionToken('17829'), 'pid-registry id is not a token');
assert.ok(!validateClaudeSessionToken(''));

// ---- projects dir slug -----------------------------------------------------
assert.strictEqual(
  claudeProjectsDir('/home/u/.claude', '/home/u/.flock/pods/ctest'),
  path.join('/home/u/.claude', 'projects', '-home-u--flock-pods-ctest'),
  'cwd / and . -> - slug under projects/',
);

// ---- latest session (newest mtime wins, non-uuid files ignored) -------------
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-claude-'));
  const dir = path.join(tmp, 'projects', '-tmp-x');
  fs.mkdirSync(dir, { recursive: true });
  const a = path.join(dir, '11111111-1111-4111-8111-111111111111.jsonl');
  const b = path.join(dir, '22222222-2222-4222-8222-222222222222.jsonl');
  fs.writeFileSync(a, 'old');
  fs.writeFileSync(b, 'newer');
  fs.writeFileSync(path.join(dir, 'notes.txt'), 'not a session');
  fs.writeFileSync(path.join(dir, '17829.jsonl'), 'pid-named file is not a uuid');
  // make b strictly newer
  const t = new Date(Date.now() + 60000);
  fs.utimesSync(b, t, t);
  assert.strictEqual(latestClaudeSession(dir)?.token, '22222222-2222-4222-8222-222222222222');
  fs.rmSync(a);
  assert.strictEqual(latestClaudeSession(dir)?.token, '22222222-2222-4222-8222-222222222222');
  fs.rmSync(dir, { recursive: true });
  assert.strictEqual(latestClaudeSession(dir), null, 'missing dir -> null');
  fs.rmSync(tmp, { recursive: true, force: true });
}

// ---- args ------------------------------------------------------------------
assert.deepStrictEqual(buildClaudeArgs({ model: 'm1' }), ['--model', 'm1']);
assert.deepStrictEqual(
  buildClaudeArgs({ model: 'm1', effort: 'medium', permissionMode: 'acceptEdits' }),
  ['--model', 'm1', '--effort', 'medium', '--permission-mode', 'acceptEdits'],
);
assert.deepStrictEqual(
  buildClaudeArgs({ resumeToken: 'abc-123' }),
  ['--resume', 'abc-123'],
);
assert.deepStrictEqual(
  buildClaudeArgs({ resumeToken: 'abc-123', fork: true }),
  ['--resume', 'abc-123', '--fork-session'],
  'fork = resume + --fork-session',
);
assert.deepStrictEqual(buildClaudeArgs({}), [], 'no flags when nothing set');

// ---- boot dialogs -----------------------------------------------------------
assert.strictEqual(CLAUDE_BOOT_DIALOGS.length, 6, 'six one-time dialogs');
assert.deepStrictEqual(
  CLAUDE_BOOT_DIALOGS.map((d) => d.marker),
  [
    'Choose the text style',
    'Do you want to use this API key',
    'Enter to continue',
    'Yes, I trust this folder',
    'Space to select',
    'accept all responsibility',
  ],
  'observed dialog order',
);
// api key confirm must select "Yes" (the first option) -> Up from the
// recommended-default "No"
assert.deepStrictEqual(CLAUDE_BOOT_DIALOGS[1].keys, ['Up', 'Enter']);
// folder trust must select "Yes, I trust this folder" (the second option)
assert.deepStrictEqual(CLAUDE_BOOT_DIALOGS[3].keys, ['Down', 'Enter']);
// MCP approval: reject all (pods don't get the operator's global servers)
assert.deepStrictEqual(CLAUDE_BOOT_DIALOGS[4].keys, ['Escape']);
// bypass acceptance: "Yes, I accept" is the second option
assert.deepStrictEqual(CLAUDE_BOOT_DIALOGS[5].keys, ['Down', 'Enter']);

// ---- pane scans --------------------------------------------------------------
const idle = 'Welcome to Claude Code v2.1.287\n❯ Try "fix typecheck errors"\n  ⏵⏵ auto mode on';
assert.ok(claudePaneReady(idle), 'idle prompt is ready');
const busy = '✻ Working...\n❯ \n  esc to interrupt · ← for agents';
assert.ok(claudePaneBusy(busy), 'in-turn marker');
const dialog = 'Do you want to use this API key?\n  ❯ No (recommended)\n Enter to confirm · Esc to cancel';
assert.ok(!claudePaneReady(dialog), 'open dialog is not ready');
const mcpDialog = ' MCP servers\n  ❯ [ ] exa\n    [✔] context7\n  Enable selected\n Space to select · Esc to reject all';
assert.ok(!claudePaneReady(mcpDialog), 'MCP dialog checkbox ❯ is not a false ready');
const shell = '╭─ ~/x ─╮\n╰─ ';
assert.ok(!claudePaneReady(shell), 'shell prompt (zsh ❰) is not the claude ❯ cursor');

// ---- transcript fingerprint / growth ----------------------------------------
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-claude-fp-'));
  const dir = path.join(tmp, 'projects', '-tmp-y');
  fs.mkdirSync(dir, { recursive: true });
  const f = path.join(dir, '33333333-3333-4333-8333-333333333333.jsonl');
  assert.strictEqual(claudeTranscriptFp(dir), null, 'no file yet -> null');
  fs.writeFileSync(f, '{"a":1}\n');
  const before = claudeTranscriptFp(dir);
  assert.ok(before && before.file.endsWith('.jsonl'));
  const r1 = await waitForTranscriptGrowth(dir, before, 2000);
  assert.ok(!r1.grown, 'no growth yet');
  fs.appendFileSync(f, '{"b":2}\n');
  const r2 = await waitForTranscriptGrowth(dir, before, 5000);
  assert.ok(r2.grown, 'growth detected');
  // a fresh pod (null fingerprint) acks on first appearance
  const r3 = await waitForTranscriptGrowth(dir, null, 1000);
  assert.ok(r3.grown, 'first appearance counts as growth');
  fs.rmSync(tmp, { recursive: true, force: true });
}

console.log('claude-protocol: all checks passed');
