// pod discover/adopt — hermetic checks over a REAL tmux scratch session and
// an isolated FLOCK_HOME (so TMUX_SESSION is a fresh core session, never the
// operator's). Verifies: adopt moves the live pane into the core session as
// the pod window WITHOUT restart (same pid), registers pod+run, refuses
// double-adopt / dead pane / core-session pane, discover lists external
// panes only, close releases the pod.
// Run: node dist/core/adopt.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

const home = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-adopt-'));
process.env.FLOCK_HOME = home; // TMUX_SESSION derives from it (fresh session)

const src = `adopt-src-${Date.now().toString(36)}`;
const tmux = (args: string[]) => execFileSync('tmux', args, { encoding: 'utf8' });

function cleanup() {
  try { tmux(['kill-session', '-t', src]); } catch { /* gone */ }
  try { tmux(['kill-session', '-t', tmuxSessionFromEnv()]); } catch { /* gone */ }
  fs.rmSync(home, { recursive: true, force: true });
}
// TMUX_SESSION derives from FLOCK_HOME the same way terminal.ts does it
function tmuxSessionFromEnv(): string {
  const b = path.basename(home);
  return b === 'flock' ? 'flock' : `flock-${b}`;
}

try {
  // scratch session with TWO windows; we adopt window 1's pane
  tmux(['new-session', '-d', '-s', src, '-x', '200', '-y', '50', 'sleep', '100000']);
  tmux(['new-window', '-t', src, '-n', 'target', 'bash']);
  const paneId = tmux(['display-message', '-p', '-t', `${src}:1`, '#{pane_id}']).trim();
  const panePidBefore = Number(tmux(['display-message', '-p', '-t', `${src}:1`, '#{pane_pid}']).trim());
  assert.ok(paneId.startsWith('%'));

  const { openStore, getPodByRole, currentRun } = await import('./store.js');
  const { TMUX_SESSION, windowExists, listAllPanes, paneInfo } = await import('./terminal.js');
  const { apply } = await import('./ops.js');
  const db = openStore(home);
  const ctx = { store: db, ticks: { register() {}, all: () => [] }, startedAt: new Date().toISOString(), emit: () => {} } as never;

  // 1) discover: the scratch pane is listed, core-session panes are not
  {
    const ext = (await listAllPanes()).filter((p) => p.session !== TMUX_SESSION);
    assert.ok(ext.some((p) => p.paneId === paneId), 'scratch pane must be a discover candidate');
    assert.ok(ext.every((p) => p.session !== TMUX_SESSION), 'core session panes must never be candidates');
  }

  // 2) adopt: pane moves into the core session, NO restart (same pid)
  {
    const r = (await apply({ type: 'pod_adopt', role: 'ext', pane: paneId }, ctx)) as { pod: { state: string; terminal_target: string; dir: string; agent: string }; run: { pid: number | null; meta: string }; source: string };
    assert.strictEqual(r.pod.state, 'live');
    assert.strictEqual(r.pod.terminal_target, `${TMUX_SESSION}:flock-ext`);
    assert.strictEqual(r.pod.agent, 'cmd');
    assert.strictEqual(r.run.pid, panePidBefore, 'adopt must NOT restart the pane (pid unchanged)');
    assert.match(r.run.meta, /adopted/);
    assert.ok(r.pod.dir, 'dir defaults to the pane cwd');
    assert.ok(windowExists('ext'), 'pod window exists in the core session');
    // the pane is alive at its new seat, same pid
    const after = await paneInfo(r.pod.terminal_target);
    assert.ok(after, 'adopted pane is alive at the new seat');
    assert.strictEqual(after.pid, panePidBefore);
    assert.strictEqual(after.session, TMUX_SESSION);
  }

  // 3) refusals: double-adopt, dead pane, core-session pane
  {
    let e1 = false;
    try { await apply({ type: 'pod_adopt', role: 'ext', pane: paneId }, ctx); } catch (e) { e1 = /already live/.test(String((e as Error).message)); }
    assert.ok(e1, 'double adopt is refused');
    let e2 = false;
    try { await apply({ type: 'pod_adopt', role: 'x2', pane: '%99999' }, ctx); } catch (e) { e2 = /no live pane/.test(String((e as Error).message)); }
    assert.ok(e2, 'dead/unknown pane is refused');
    let e3 = false;
    try { await apply({ type: 'pod_adopt', role: 'x3', pane: `${TMUX_SESSION}:flock-ext` }, ctx); } catch (e) { e3 = /already in the core session/.test(String((e as Error).message)); }
    assert.ok(e3, 'a core-session pane cannot be adopted again');
  }

  // 4) pod state is visible and close releases the seat
  {
    const pod = getPodByRole(db, 'ext');
    assert.strictEqual(pod?.state, 'live');
    const run = currentRun(db, 'ext');
    assert.ok(run && !run.ended_at);
    const r = (await apply({ type: 'pod_close', role: 'ext' }, ctx)) as { ok: boolean };
    assert.strictEqual(r.ok, true);
    assert.strictEqual(getPodByRole(db, 'ext')?.state, 'closed');
    // kill-window signals the pane process; the window lingers until the
    // process reaps — poll with a short deadline
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline && (await windowExists('ext'))) {
      await new Promise((res) => setTimeout(res, 100));
    }
    assert.ok(!(await windowExists('ext')), 'close kills the adopted window');
    assert.ok(currentRun(db, 'ext')?.ended_at, 'run ended on close');
  }
} finally {
  cleanup();
}

console.log('adopt.test.js: all checks passed');
