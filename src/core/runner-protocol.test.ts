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
  parseAnswerLine,
  dialogResponse,
  newNonce,
  type PendingDialog,
} from './runner-protocol.js';

// frame round-trip (multi-line, unicode)
const msg = 'Task t_1: заголовок\nстрока 2\nПротокол: flock task done t_1';
assert.strictEqual(unframeMessage(frameMessage(msg))?.text, msg, 'v1 frame round-trip');
assert.strictEqual(unframeMessage('plain text'), null);

// v2 frame: nonce round-trip + v2 line also starts with the v1 prefix,
// so v2 must win the match (order matters)
const nonce = newNonce();
const framed2 = unframeMessage(frameMessage(msg, nonce));
assert.ok(framed2, 'v2 frame parses');
assert.strictEqual(framed2!.text, msg);
assert.strictEqual(framed2!.nonce, nonce);
assert.notStrictEqual(unframeMessage(frameMessage('a', 'n1'))!.nonce, unframeMessage(frameMessage('a', 'n2'))!.nonce);
// distinct nonces are distinct (repeat-message false-positive is closed)
const n1 = newNonce();
const n2 = newNonce();
assert.notStrictEqual(n1, n2);

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

// operator answers for extension dialogs (permission gates)
assert.deepStrictEqual(parseAnswerLine('/answer'), { kind: 'index', n: 1 });
assert.deepStrictEqual(parseAnswerLine('/answer 2'), { kind: 'index', n: 2 });
assert.deepStrictEqual(parseAnswerLine('/answer run'), { kind: 'value', v: 'run' });
assert.strictEqual(parseAnswerLine('hello'), null);
assert.strictEqual(parseAnswerLine('/answer2'), null);

const sel: PendingDialog = { id: 'd1', index: 1, method: 'select', title: 't', options: ['Выполнить', 'Отменить'], at: 't' };
assert.deepStrictEqual(dialogResponse(sel, { kind: 'index', n: 1 }), { value: 'Выполнить' });
assert.deepStrictEqual(dialogResponse(sel, { kind: 'value', v: 'отменить' }), { value: 'Отменить' });
// unambiguous substring (option carries "name — description")
const sel2: PendingDialog = { id: 'd4', index: 4, method: 'select', title: 't', options: ['planner — planning', 'trivial — тестовый субагент', 'worker — implementation'], at: 't' };
assert.deepStrictEqual(dialogResponse(sel2, { kind: 'value', v: 'trivial' }), { value: 'trivial — тестовый субагент' });
assert.strictEqual(dialogResponse(sel2, { kind: 'value', v: 'r' }), null); // ambiguous: matches all three
assert.strictEqual(dialogResponse(sel, { kind: 'index', n: 9 }), null);
assert.strictEqual(dialogResponse(sel, { kind: 'value', v: 'нет такого' }), null);
const conf: PendingDialog = { id: 'd2', index: 2, method: 'confirm', title: 't', at: 't' };
assert.deepStrictEqual(dialogResponse(conf, { kind: 'index', n: 1 }), { confirmed: true });
assert.deepStrictEqual(dialogResponse(conf, { kind: 'index', n: 2 }), { confirmed: false });
assert.deepStrictEqual(dialogResponse(conf, { kind: 'value', v: 'yes' }), { confirmed: true });
assert.deepStrictEqual(dialogResponse(conf, { kind: 'value', v: 'no' }), { confirmed: false });
const inp: PendingDialog = { id: 'd3', index: 3, method: 'input', title: 't', at: 't' };
assert.deepStrictEqual(dialogResponse(inp, { kind: 'value', v: 'текст' }), { value: 'текст' });

console.log('runner-protocol: all checks passed');
