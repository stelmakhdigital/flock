// Hermetic checks for runner-protocol (pure module). Run: node dist/core/runner-protocol.test.js
import assert from 'node:assert';
import path from 'node:path';
import {
  frameMessage,
  unframeMessage,
  seatPaths,
  buildPendingState,
  parseRunnerState,
  buildPiChildEnv,
  buildRunnerCommand,
  buildPiChildArgs,
  buildWindowLaunchCmd,
} from './runner-protocol.js';

// frame round-trip (multi-line, unicode)
const msg = 'Task t_1: заголовок\nстрока 2\nПротокол: flock task done t_1';
assert.strictEqual(unframeMessage(frameMessage(msg)), msg);
assert.strictEqual(unframeMessage('plain text'), null);

// seat paths
const p = seatPaths('/home/u/.flock', 'dev');
assert.strictEqual(p.runnerStatePath, path.join('/home/u/.flock', 'pods', 'dev', '.pi', 'runner-state.json'));
assert.ok(p.sessionsDir.endsWith(path.join('.pi', 'sessions')));

// sidecar parse: valid, invalid, launchId-scoped shape
const pending = buildPendingState('la_1', '2026-01-01T00:00:00Z');
assert.deepStrictEqual(pending, { ready: false, launchId: 'la_1', updatedAt: '2026-01-01T00:00:00Z' });
const st = { ...pending, ready: true, sessionFile: '/s/x.jsonl', streaming: true, lastPrompt: { text: 'hi', at: 't' } };
assert.deepStrictEqual(parseRunnerState(JSON.stringify(st)), st);
assert.strictEqual(parseRunnerState('not json'), null);
assert.strictEqual(parseRunnerState(JSON.stringify({ ready: 'x' })), null);

// env allowlist: deny-by-default
const env = buildPiChildEnv(
  { PATH: '/bin', HOME: '/h', SECRET_TOKEN: 'nope', FLOCK_HOME: '/fh', FLOCK_POD_ROLE: 'dev' },
  { agentDir: '/a', sessionsDir: '/s' },
);
assert.strictEqual(env.PATH, '/bin');
assert.strictEqual(env.FLOCK_HOME, '/fh');
assert.strictEqual(env.FLOCK_POD_ROLE, 'dev');
assert.strictEqual(env.PI_CODING_AGENT_DIR, '/a');
assert.strictEqual(env.PI_CODING_AGENT_SESSION_DIR, '/s');
assert.strictEqual(env.SECRET_TOKEN, undefined);

// pi child args: fresh / resume / fork
const fresh = buildPiChildArgs({ sessionsDir: '/s', role: 'dev', trust: 'approve', model: 'p/m', agentsMdPath: '/d/AGENTS.md' });
assert.ok(fresh.includes('--mode') && fresh.includes('rpc'));
assert.ok(fresh.includes('--session-id') && fresh[fresh.indexOf('--session-id') + 1] === 'dev');
assert.ok(fresh.includes('--no-context-files'));
assert.ok(fresh.includes('--append-system-prompt') && fresh[fresh.indexOf('--append-system-prompt') + 1] === '/d/AGENTS.md');
assert.ok(fresh.includes('--approve'));
assert.ok(fresh.includes('--model') && fresh[fresh.indexOf('--model') + 1] === 'p/m');
assert.ok(!fresh.includes('--session') && !fresh.includes('--fork'));

const resume = buildPiChildArgs({ sessionsDir: '/s', role: 'dev', trust: 'no-approve', sessionFile: '/s/f.jsonl' });
assert.strictEqual(resume[resume.indexOf('--session') + 1], '/s/f.jsonl');
assert.ok(resume.includes('--no-approve'));
assert.ok(!resume.includes('--session-id')); // pi 1.0.0: incompatible (verified live)
assert.ok(!resume.includes('--fork'));

const fork = buildPiChildArgs({ sessionsDir: '/s', role: 'rev', trust: 'approve', forkRef: '/s/parent.jsonl' });
assert.strictEqual(fork[fork.indexOf('--fork') + 1], '/s/parent.jsonl');
assert.ok(!fork.includes('--session-id')); // fork yields a NEW session (uuid)
assert.ok(!fork.includes('--session'));

const fresh2 = buildPiChildArgs({ sessionsDir: '/s', role: 'x', trust: 'approve' });
assert.strictEqual(fresh2[fresh2.indexOf('--session-id') + 1], 'x');

// runner command: shell-quoted, launchId present
const cmd = buildRunnerCommand({
  runnerPath: '/d/runner.js',
  stateRoot: '/home/u/.flock',
  role: "o'brien",
  cwd: '/d',
  launchId: 'la_1',
  trust: 'approve',
  model: 'p/m',
});
assert.ok(cmd.startsWith('node '));
assert.ok(cmd.includes("'o'\\''brien'"));
assert.ok(cmd.includes('--launch-id') && cmd.includes('la_1'));
assert.ok(cmd.includes('--approve'));
assert.ok(!cmd.includes('--no-approve'));

// window launch command: env prefix + command, JSON-quoted values
const wlc = buildWindowLaunchCmd('node /d/runner.js --x', {
  role: 'dev',
  dir: '/h/pods/dev',
  home: '/h',
  port: '7461',
  basePath: '/usr/bin',
  extraEnv: { PI_CODING_AGENT_DIR: '/h/pods/dev/.pi/agent' },
});
assert.ok(wlc.startsWith('PATH="/h/bin:/h/pods/dev/bin:/usr/bin" '));
assert.ok(wlc.includes('FLOCK_HOME="/h"'));
assert.ok(wlc.includes('FLOCK_PORT="7461"'));
assert.ok(wlc.includes('FLOCK_POD_ROLE="dev"'));
assert.ok(wlc.includes('PI_CODING_AGENT_DIR="/h/pods/dev/.pi/agent"'));
assert.ok(wlc.endsWith('node /d/runner.js --x'));

console.log('runner-protocol: all checks passed');
