// Hermetic checks for runtime-adapter pure logic + managed blocks.
// Run: node dist/core/runtime-adapter.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { resolveTrust, validateResumeToken, resolveLaunchMode, seatPaths } from './bridge-protocol.js';
import { mergeManagedBlock, pruneManagedBlocks, PiRuntimeAdapter, ClaudeRuntimeAdapter, getAdapter } from './runtime-adapter.js';

// ── resolveTrust: posture is authoritative ──
assert.strictEqual(resolveTrust(undefined, undefined), 'approve'); // flock default
assert.strictEqual(resolveTrust('no-approve', 'floor'), 'no-approve'); // configured wins on floor
assert.strictEqual(resolveTrust('no-approve', 'full_bypass'), 'approve'); // full_bypass forces
assert.strictEqual(resolveTrust('approve', 'full_bypass'), 'approve');

// ── validateResumeToken: absolute path, no .., charset, .jsonl ──
assert.deepStrictEqual(validateResumeToken('/home/u/.flock/pods/dev/.pi/sessions/abc_dev.jsonl'), {
  ok: true,
  token: '/home/u/.flock/pods/dev/.pi/sessions/abc_dev.jsonl',
});
assert.strictEqual(validateResumeToken('relative/x.jsonl').ok, false);
assert.strictEqual(validateResumeToken('/a/../b.jsonl').ok, false);
assert.strictEqual(validateResumeToken('').ok, false);
assert.strictEqual(validateResumeToken(null).ok, false);
assert.strictEqual(validateResumeToken('/a/b c.jsonl').ok, false); // space
assert.strictEqual(validateResumeToken('/a/b.json').ok, false); // suffix
assert.strictEqual(validateResumeToken('/' + 'a'.repeat(600) + '.jsonl').ok, false); // too long

// ── resolveLaunchMode: fresh / resume / fork / mutual exclusion ──
assert.deepStrictEqual(resolveLaunchMode({}), { mode: 'fresh' });
const res = resolveLaunchMode({ resumeToken: '/s/f.jsonl' });
assert.deepStrictEqual(res, { mode: 'resume', sessionFile: '/s/f.jsonl' });
const bad = resolveLaunchMode({ resumeToken: 'nope' });
assert.strictEqual(bad.mode, 'error');
assert.strictEqual(bad.recovery, 'attention_required'); // C6: no retry_fresh
const fk = resolveLaunchMode({ forkSource: { kind: 'native_id', value: '/s/parent.jsonl' } });
assert.deepStrictEqual(fk, { mode: 'fork', forkRef: '/s/parent.jsonl' });
assert.strictEqual(resolveLaunchMode({ forkSource: { kind: 'name', value: 'x' } }).mode, 'error');
assert.strictEqual(
  resolveLaunchMode({ resumeToken: '/s/f.jsonl', forkSource: { kind: 'native_id', value: '/s/p.jsonl' } }).mode,
  'error',
); // mutual exclusion

// ── mergeManagedBlock: create / replace / idempotent / multi-block / legacy ──
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-mb-'));
try {
  const f = path.join(tmp, 'AGENTS.md');

  // create
  mergeManagedBlock(f, 'b1', 'one');
  assert.ok(fs.readFileSync(f, 'utf8').includes('<!-- BEGIN MANAGED BLOCK: b1 -->\none\n<!-- END MANAGED BLOCK: b1 -->'));

  // idempotent (same content -> file unchanged)
  const before = fs.readFileSync(f, 'utf8');
  mergeManagedBlock(f, 'b1', 'one');
  assert.strictEqual(fs.readFileSync(f, 'utf8'), before);

  // replace (content updated, no duplicate)
  mergeManagedBlock(f, 'b1', 'one-v2');
  let text = fs.readFileSync(f, 'utf8');
  assert.ok(text.includes('one-v2') && !text.includes('\none\n'));
  assert.strictEqual(text.split('BEGIN MANAGED BLOCK: b1').length - 1, 1);

  // second block coexists, user text between blocks survives
  fs.writeFileSync(f, text + '\nuser text here\n');
  mergeManagedBlock(f, 'b2', 'two');
  text = fs.readFileSync(f, 'utf8');
  assert.ok(text.includes('user text here'));
  assert.ok(text.includes('BEGIN MANAGED BLOCK: b2'));
  assert.strictEqual(text.split('BEGIN MANAGED BLOCK: b1').length - 1, 1);
  assert.strictEqual(text.split('BEGIN MANAGED BLOCK: b2').length - 1, 1);

  // replaceBlockIds: legacy block stripped, no residue
  mergeManagedBlock(f, 'b1', 'one-v3', { replaceBlockIds: ['b1-legacy'] });
  text = fs.readFileSync(f, 'utf8');
  assert.ok(!text.includes('b1-legacy'));

  // explicit legacy strip when both present
  fs.writeFileSync(f, '<!-- BEGIN MANAGED BLOCK: old -->\nold\n<!-- END MANAGED BLOCK: old -->\n');
  mergeManagedBlock(f, 'new', 'fresh', { replaceBlockIds: ['old'] });
  text = fs.readFileSync(f, 'utf8');
  assert.ok(text.includes('BEGIN MANAGED BLOCK: new'));
  assert.ok(!text.includes('BEGIN MANAGED BLOCK: old'));
  assert.ok(!text.includes('\nold\n'));
} finally {
  fs.rmSync(tmp, { recursive: true, force: true });
}

// ---- pruneManagedBlocks ----------------------------------------------------
{
  const tmp2 = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-prune-'));
  const p2 = path.join(tmp2, 'AGENTS.md');
  mergeManagedBlock(p2, 'flock-protocol', 'PROTOCOL');
  mergeManagedBlock(p2, 'guid-a', 'A');
  mergeManagedBlock(p2, 'guid-b', 'B');
  fs.appendFileSync(p2, '\nUSER TEXT\n');
  // keep protocol + a; b must go, user text must survive
  pruneManagedBlocks(p2, new Set(['flock-protocol', 'guid-a']));
  const t = fs.readFileSync(p2, 'utf8');
  assert.ok(t.includes('BEGIN MANAGED BLOCK: guid-a'), 'kept block survives');
  assert.ok(!t.includes('guid-b'), 'stale block removed');
  assert.ok(t.includes('USER TEXT'), 'user text untouched');
  // pruning again is a no-op
  const before = fs.readFileSync(p2, 'utf8');
  pruneManagedBlocks(p2, new Set(['flock-protocol', 'guid-a']));
  assert.strictEqual(fs.readFileSync(p2, 'utf8'), before, 'idempotent');
  fs.rmSync(tmp2, { recursive: true, force: true });
}

// ── signal contract: degradation + pi/claude answers (hermetic) ───────────
// bash has NO liveness/sendVerified/healthProbe -> core degrades (pid check
// / visual probe / skip health), it does not error.
{
  const env = { home: '/h', token: 't', runnerPath: '/h/dist/runner.js' };
  const bash = getAdapter({ id: 'bash', command: 'bash' }, env)!;
  assert.strictEqual(bash.runtime, 'bash');
  assert.strictEqual(bash.liveness, undefined, 'bash: no liveness (core falls back to pid check)');
  assert.strictEqual(bash.sendVerified, undefined, 'bash: no sendVerified (core falls back to visual probe)');
  assert.strictEqual(bash.healthProbe, undefined, 'bash: no healthProbe (core skips health)');
  const pi = getAdapter({ id: 'pi', command: 'pi', runner: 'flock-rpc' }, env)!;
  assert.strictEqual(typeof pi.liveness, 'function', 'pi answers liveness');
  assert.strictEqual(typeof pi.sendVerified, 'function', 'pi answers sendVerified');
  assert.strictEqual(typeof pi.healthProbe, 'function', 'pi answers healthProbe');
  const claude = getAdapter({ id: 'c', command: 'claude', runtime: 'claude' }, env)!;
  assert.strictEqual(claude.runtime, 'claude');
  assert.strictEqual(typeof claude.liveness, 'function', 'claude answers liveness');
  assert.strictEqual(typeof claude.sendVerified, 'function', 'claude answers sendVerified');
  assert.strictEqual(typeof claude.healthProbe, 'function', 'claude answers healthProbe');
  const codex = getAdapter({ id: 'codex', command: 'codex', runtime: 'codex' }, env)!;
  assert.strictEqual(codex.runtime, 'codex');
  assert.strictEqual(typeof codex.liveness, 'function', 'codex answers liveness');
  assert.strictEqual(typeof codex.sendVerified, 'function', 'codex answers sendVerified');
  assert.strictEqual(typeof codex.healthProbe, 'function', 'codex answers healthProbe');
  assert.strictEqual(typeof codex.latestSessionToken, 'function', 'codex answers latestSessionToken (thread id)');
}

// pi liveness (pure sidecar logic, pane mocked at the shell):
// typed exit -> clean / crashed; alive sidecar + pane at shell -> crashed.
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-adapt-'));
  const env = { home: tmp, token: 't', runnerPath: '/x/runner.js' };
  const adapter = new PiRuntimeAdapter({ id: 'pi', command: 'pi' }, env);
  const binding = { role: 'lp', cwd: path.join(tmp, 'pods', 'lp') };
  const sp = seatPaths(tmp, 'lp');
  fs.mkdirSync(sp.agentDir, { recursive: true });
  const run = (launchId: string) => ({ id: 'run1', pid: 1, meta: JSON.stringify([{ ts: '2026-01-01T00:00:00Z', kind: 'created', launchId }]), started_at: '2026-01-01T00:00:00Z' });

  // typed exit code 0 -> clean
  fs.writeFileSync(sp.runnerStatePath, JSON.stringify({ ready: true, launchId: 'la_1', updatedAt: '2026-01-01T00:00:00Z', exited: { code: 0, at: '2026-01-01T00:01:00Z' } }));
  let r = await adapter.liveness!(binding, run('la_1'));
  assert.deepStrictEqual(r, { alive: false, reason: 'clean' }, 'typed exit 0 = clean');

  // typed exit with signal -> crashed(signal X)
  fs.writeFileSync(sp.runnerStatePath, JSON.stringify({ ready: true, launchId: 'la_1', updatedAt: 'x', exited: { code: null, signal: 'SIGKILL', at: 'x' } }));
  r = await adapter.liveness!(binding, run('la_1'));
  assert.strictEqual(r.alive, false);
  assert.strictEqual(r.reason, 'crashed(signal SIGKILL)');

  // typed exit for ANOTHER launch -> not this run (alive; launchId scoping)
  fs.writeFileSync(sp.runnerStatePath, JSON.stringify({ ready: true, launchId: 'la_9', updatedAt: 'x', exited: { code: 1, at: 'x' } }));
  r = await adapter.liveness!(binding, run('la_1'));
  assert.strictEqual(r.alive, true, 'stale sidecar from another launch is ignored');

  // alive sidecar + a non-shell foreground (a real temporary window running
  // `sleep` = the agent's foreground) -> alive. (The shell-foreground branch
  // is the same SHELL_COMMANDS rule as the old runkeeper — verified live in
  // E2E, where a dead claude/pi foreground reports crashed.)
  fs.writeFileSync(sp.runnerStatePath, JSON.stringify({ ready: true, launchId: 'la_1', updatedAt: 'x' }));
  const { execFile: ef } = await import('node:child_process');
  const { promisify: pf } = await import('node:util');
  const tmuxTry = (a: string[]) => pf(ef)('tmux', a).then(() => true).catch(() => false);
  // 'no current client' makes tmux exit 1 even though the window IS created
  // (detached session) — so the probe below is the real check.
  if (await tmuxTry(['new-window', '-d', '-t', 'flock', '-n', 'flock-lp', 'sleep', '30']) || true) {
    await new Promise((res) => setTimeout(res, 500));
    try {
      const fg = await (await import('./terminal.js')).paneCommand('flock:flock-lp');
      if (fg === 'sleep') {
        r = await adapter.liveness!(binding, run('la_1'));
        assert.strictEqual(r.alive, true, 'alive sidecar + agent foreground (sleep) = alive');
      }
      // window creation failed (no tmux server) -> skip: the pane-scrape
      // branch is covered by the claude/live E2E, not by this hermetic test
    } finally {
      await tmuxTry(['kill-window', '-t', 'flock:flock-lp']);
    }
  }

  // no launchId in the run meta -> the typed checks are skipped (alive)
  r = await adapter.liveness!(binding, { id: 'run2', pid: 1, meta: JSON.stringify([{ kind: 'created' }]), started_at: 'x' });
  assert.strictEqual(r.alive, true, 'run without a launchId degrades to the generic pid check in core');

  fs.rmSync(tmp, { recursive: true, force: true });
}

// pi healthProbe (pure: sidecar + activity log on disk, no pane involved)
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-adapt2-'));
  const env = { home: tmp, token: 't', runnerPath: '/x/runner.js' };
  const adapter = new PiRuntimeAdapter({ id: 'pi', command: 'pi' }, env);
  const role = 'hp';
  const binding = { role, cwd: path.join(tmp, 'pods', role) };
  const sp = seatPaths(tmp, role);
  fs.mkdirSync(sp.agentDir, { recursive: true });

  // no sidecar at all -> not ready, no gate
  let p = await adapter.healthProbe!(binding);
  assert.deepStrictEqual(p, { ready: false, busy: false, gate: undefined }, 'missing sidecar = not ready');

  // sidecar ready + auto-denied dialog in the activity -> gate (channel
  // attach: C10 — the operator answer channel is gone, the lever is the pane)
  fs.writeFileSync(sp.runnerStatePath, JSON.stringify({ ready: true, launchId: 'la_1', updatedAt: 'x', streaming: true, lastPrompt: { text: 'hi', at: '2026-01-01T00:05:00Z' } }));
  fs.writeFileSync(sp.activityPath, JSON.stringify({ at: '2026-01-01T00:06:00Z', event: 'ext_dialog_auto_denied', id: 'd1', method: 'select', title: 'pick one' }) + '\n');
  p = await adapter.healthProbe!(binding);
  assert.strictEqual(p!.ready, true, 'ready from the sidecar');
  assert.strictEqual(p!.busy, true, 'busy = streaming');
  assert.strictEqual(p!.lastActivityAt, '2026-01-01T00:05:00Z', 'lastActivityAt = lastPrompt.at');
  assert.deepStrictEqual(p!.gate, { id: 'd1', title: 'pick one', channel: 'attach', at: '2026-01-01T00:06:00.000Z' }, 'gate from activity, channel attach (at round-tripped via ISO)');

  // a newer auto-deny is the gate the probe reports (most recent wins)
  fs.appendFileSync(sp.activityPath, JSON.stringify({ at: '2026-01-01T00:07:00Z', event: 'ext_dialog_auto_denied', id: 'd2', method: 'confirm', title: 'another' }) + '\n');
  p = await adapter.healthProbe!(binding);
  assert.strictEqual(p!.gate?.id, 'd2', 'most recent auto-deny is the gate');

  fs.rmSync(tmp, { recursive: true, force: true });
}

// codex liveness (pure sidecar logic — the pi rule, bridge foreground):
// typed exit -> clean / crashed; alive sidecar + pane at shell -> crashed.
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-codex-'));
  const env = { home: tmp, token: 't', runnerPath: '/x/runner.js' };
  const { CodexRuntimeAdapter } = await import('./runtime-adapter.js');
  const adapter = new CodexRuntimeAdapter({ id: 'codex', command: 'codex', runtime: 'codex' }, env);
  const role = 'cx';
  const binding = { role, cwd: path.join(tmp, 'pods', role), seatRoot: path.join(tmp, 'pods', role) };
  const sp = seatPaths(tmp, role);
  fs.mkdirSync(path.dirname(sp.runnerStatePath), { recursive: true });
  const run = (launchId: string) => ({ id: 'run1', pid: 1, meta: JSON.stringify([{ ts: '2026-01-01T00:00:00Z', kind: 'created', launchId }]), started_at: '2026-01-01T00:00:00Z' });

  // typed exit code 0 -> clean
  fs.writeFileSync(sp.runnerStatePath, JSON.stringify({ ready: true, launchId: 'la_1', updatedAt: 'x', exited: { code: 0, at: 'x' } }));
  let r = await adapter.liveness!(binding, run('la_1'));
  assert.deepStrictEqual(r, { alive: false, reason: 'clean' }, 'codex typed exit 0 = clean');

  // typed exit with signal -> crashed
  fs.writeFileSync(sp.runnerStatePath, JSON.stringify({ ready: true, launchId: 'la_1', updatedAt: 'x', exited: { code: null, signal: 'SIGKILL', at: 'x' } }));
  r = await adapter.liveness!(binding, run('la_1'));
  assert.strictEqual(r.reason, 'crashed(signal SIGKILL)');

  // stale sidecar from another launch -> not this run
  fs.writeFileSync(sp.runnerStatePath, JSON.stringify({ ready: true, launchId: 'la_9', updatedAt: 'x', exited: { code: 1, at: 'x' } }));
  r = await adapter.liveness!(binding, run('la_1'));
  assert.strictEqual(r.alive, true, 'codex: launchId scoping');

  // no sidecar at all -> alive (generic pid check in core decides)
  fs.rmSync(sp.runnerStatePath);
  r = await adapter.liveness!(binding, run('la_1'));
  assert.strictEqual(r.alive, true);

  // healthProbe: sidecar ready/streaming/lastPrompt only, never a gate
  fs.writeFileSync(sp.runnerStatePath, JSON.stringify({ ready: true, launchId: 'la_1', updatedAt: 'x', streaming: true, lastPrompt: { text: 'hi', at: '2026-01-01T00:05:00Z' } }));
  const p = await adapter.healthProbe!(binding);
  assert.strictEqual(p!.ready, true);
  assert.strictEqual(p!.busy, true);
  assert.strictEqual(p!.lastActivityAt, '2026-01-01T00:05:00Z');
  assert.strictEqual(p!.gate, undefined, 'codex has no interactive dialogs (approval never)');

  // latestSessionToken: the sidecar's sessionId (the codex thread)
  fs.writeFileSync(sp.runnerStatePath, JSON.stringify({ ready: true, launchId: 'la_1', updatedAt: 'x', sessionId: '01a1059d-5279-77b1-a79f-991941e01774' }));
  assert.strictEqual(await adapter.latestSessionToken!(binding), '01a1059d-5279-77b1-a79f-991941e01774');
  fs.rmSync(sp.runnerStatePath);
  assert.strictEqual(await adapter.latestSessionToken!(binding), null, 'no sidecar -> no token (fresh relaunch)');

  // project(): config.toml lands in the pod CODEX_HOME, shim-pointing
  adapter.project({ ...binding, model: 'qwen3.8-27b-fp8' });
  const cfg = fs.readFileSync(path.join(binding.seatRoot!, '.codex', 'config.toml'), 'utf8');
  assert.ok(cfg.includes('model = "qwen3.8-27b-fp8"'));
  assert.ok(cfg.includes('base_url = "http://127.0.0.1:'), 'provider points at the in-core shim');
  assert.ok(cfg.includes('/v1'), 'responses endpoint');

  fs.rmSync(tmp, { recursive: true, force: true });
}

// claude healthProbe (pure: a fake transcript file for the mtime; the pane
// capture misses -> empty text = not ready, no gate)
{
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-adapt3-'));
  const env = { home: tmp, token: 't', runnerPath: '/x/runner.js' };
  const adapter = new ClaudeRuntimeAdapter({ id: 'c', command: 'claude', runtime: 'claude' }, env);
  const role = 'cp';
  const seatRoot = path.join(tmp, 'pods', role);
  const binding = { role, cwd: seatRoot, seatRoot };
  const { claudeProjectsDir, claudeConfigDir } = await import('./claude-protocol.js');
  const pdir = claudeProjectsDir(claudeConfigDir(seatRoot), seatRoot);
  fs.mkdirSync(pdir, { recursive: true });
  const tok = '11111111-2222-4333-8444-555555555555';
  fs.writeFileSync(path.join(pdir, `${tok}.jsonl`), '{}\n');
  const p = await adapter.healthProbe!(binding);
  assert.ok(p, 'claude probe returns');
  assert.strictEqual(p!.ready, false, 'no pane text -> not ready (capture miss)');
  assert.strictEqual(p!.gate, undefined, 'no pane text -> no gate');
  assert.ok(p!.lastActivityAt, 'lastActivityAt = the transcript mtime: ' + p!.lastActivityAt);
  fs.rmSync(tmp, { recursive: true, force: true });
}

console.log('runtime-adapter: all checks passed');
