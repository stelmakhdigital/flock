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
  parsePiConfig,
  parseChildArgs,
  buildPiChildArgs,
  buildWindowLaunchCmd,
  newNonce,
  strictestOption,
} from './bridge-protocol.js';

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

// C10: --child-args is the UNIFIED raw passthrough (form a) — appended
// LAST so the manifest wins over the mapped axes; env merges over the
// flock-managed vars (see pi-bridge buildPiChildEnv)
const cmdChild = buildRunnerCommand({
  runnerPath: '/d/pi-bridge.js',
  stateRoot: '/h',
  role: 'raw',
  cwd: '/d',
  launchId: 'la_3',
  trust: 'approve',
  child: { args: ['--model', 'override/model'], env: { GREETING: 'привет' } },
});
assert.ok(cmdChild.includes('--child-args'), 'child-only block serialized');
const mc = /--child-args '(.*)'(?= |$)/.exec(cmdChild);
assert.ok(mc, 'child-args flag extractable (child-only)');
const rtc = parseChildArgs(mc![1].replace(/'\\''/g, "'"));
assert.deepStrictEqual(rtc.args, ['--model', 'override/model'], 'raw args verbatim');
assert.deepStrictEqual(rtc.env, { GREETING: 'привет' }, 'raw env verbatim (unicode safe)');

// C10 REGRESSION: no child block -> byte-identical command to before C10
// (no --child-args flag at all; a manifest without child/axes launches
// exactly as it did)
assert.ok(!cmd.includes('--child-args'), 'no child block: no --child-args flag (regression)');

// C10: the child-args block (raw child + mapped pi axes) rides in ONE JSON
// flag and round-trips
const piBlock = { thinking: 'xhigh', tools: ['read', 'bash'], excludeTools: ['edit'], skills: ['/s/a', '/s/b'], noExtensions: true, extensions: ['builtin:web'], systemPrompt: 'You are terse.', appendSystemPrompt: ['a.md', 'b.md'], noContextFiles: true };
const cmdPi = buildRunnerCommand({
  runnerPath: '/d/pi-bridge.js',
  stateRoot: '/h',
  role: 'dev',
  cwd: '/d',
  launchId: 'la_2',
  trust: 'approve',
  child: { args: ['--foo', 'bar'], env: { MY_VAR: '1' } },
  pi: piBlock,
});
assert.ok(cmdPi.includes('--child-args'), 'child block serialized');
// the flag is shell-quoted JSON: extract it back and parse it
const m = /--child-args '(.*)'(?= |$)/.exec(cmdPi);
assert.ok(m, 'child-args flag extractable');
const rt = parseChildArgs(m![1].replace(/'\\''/g, "'"));
assert.deepStrictEqual(rt.args, ['--foo', 'bar'], 'raw child args round-trip');
assert.deepStrictEqual(rt.env, { MY_VAR: '1' }, 'raw child env round-trips');
assert.strictEqual(rt.pi?.thinking, 'xhigh', 'mapped pi axes round-trip inside child-args');
// corrupt flag degrades to empty (the child still launches with defaults)
assert.deepStrictEqual(parseChildArgs('not-json'), {}, 'corrupt child-args degrades to {}');
assert.deepStrictEqual(parseChildArgs(undefined), {}, 'absent child-args is {}');
// unknown keys are dropped by the parser (the bridge never passes garbage to the child)
const rt2 = parseChildArgs('{"args":["a"],"bogus":123,"env":{"K":"v","BAD":5}}');
assert.deepStrictEqual(rt2.args, ['a'], 'valid args parsed');
assert.deepStrictEqual(rt2.env, { K: 'v' }, 'bad-typed env entry dropped');
assert.ok(!('bogus' in (rt2 as object)), 'unknown key not present');
// parsePiConfig still works standalone (the pi-bridge maps the dictionary)
const piRt = parsePiConfig(JSON.stringify(piBlock));
assert.strictEqual(piRt.thinking, 'xhigh');

// T1: pi child args — axes map 1:1 to pi flags (names verified vs pi --help)
const cargs = buildPiChildArgs({ sessionsDir: '/s', role: 'x', trust: 'approve', pi: piBlock });
assert.strictEqual(cargs[cargs.indexOf('--thinking') + 1], 'xhigh');
assert.strictEqual(cargs[cargs.indexOf('--tools') + 1], 'read,bash');
assert.strictEqual(cargs[cargs.indexOf('--exclude-tools') + 1], 'edit');
assert.ok(cargs.includes('--skill') && cargs[cargs.indexOf('--skill') + 1] === '/s/a');
assert.ok(cargs.includes('--extension') && cargs[cargs.indexOf('--extension') + 1] === 'builtin:web');
assert.ok(cargs.includes('--no-extensions'));
assert.strictEqual(cargs[cargs.indexOf('--system-prompt') + 1], 'You are terse.');
const appends = cargs.filter((a, i) => cargs[i - 1] === '--append-system-prompt');
assert.deepStrictEqual(appends, ['a.md', 'b.md'], 'appendSystemPrompt repeatable');
// noContextFiles: the base args already carry --no-context-files; a second
// explicit one is harmless for pi but we must not DOUBLE it silently
const ncf = cargs.filter((a) => a === '--no-context-files').length;
assert.ok(ncf >= 1, 'no-context-files present');
// empty pi config: byte-identical to the pre-T1 argv (regression)
const preT1 = ['--mode', 'rpc', '--session-dir', '/s', '--approve', '--no-context-files', '--session-id', 'x'];
assert.deepStrictEqual(buildPiChildArgs({ sessionsDir: '/s', role: 'x', trust: 'approve' }), preT1, 'no pi axes: argv unchanged (regression)');

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

// C10: the operator answer channel is GONE — the auto-deny picks the
// strictest option (last = the deny path in pi's ordering)
assert.strictEqual(strictestOption(['Allow all', 'Deny']), 'Deny');
assert.strictEqual(strictestOption([]), null);

console.log('runner-protocol: all checks passed');
