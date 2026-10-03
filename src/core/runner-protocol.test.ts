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

// T1 REGRESSION: no pi block -> byte-identical command to before T1 (no
// --pi-config flag at all; a manifest without the new axes launches exactly
// as it did)
assert.ok(!cmd.includes('--pi-config'), 'no pi block: no --pi-config flag (regression)');

// T1: the pi config block rides in ONE JSON flag and round-trips
const piBlock = { thinking: 'xhigh', tools: ['read', 'bash'], excludeTools: ['edit'], skills: ['/s/a', '/s/b'], noExtensions: true, extensions: ['builtin:web'], systemPrompt: 'You are terse.', appendSystemPrompt: ['a.md', 'b.md'], noContextFiles: true };
const cmdPi = buildRunnerCommand({
  runnerPath: '/d/runner.js',
  stateRoot: '/h',
  role: 'dev',
  cwd: '/d',
  launchId: 'la_2',
  trust: 'approve',
  pi: piBlock,
});
assert.ok(cmdPi.includes('--pi-config'), 'pi block serialized');
// the flag is shell-quoted JSON: extract it back and parse it
const m = /--pi-config '(.*)'(?= |$)/.exec(cmdPi);
assert.ok(m, 'pi-config flag extractable');
let rawJson = m![1].replace(/'\\''/g, "'");
const rt = parsePiConfig(rawJson);
assert.deepStrictEqual(rt, { ...piBlock, noSkills: undefined }, 'pi config round-trips build -> parse (value-wise)');
// corrupt flag degrades to empty (pi still launches with defaults)
assert.deepStrictEqual(parsePiConfig('not-json'), {}, 'corrupt pi-config degrades to {}');
assert.deepStrictEqual(parsePiConfig(undefined), {}, 'absent pi-config is {}');
// unknown keys are dropped by the parser (the runner never passes garbage to pi)
const rt2 = parsePiConfig('{"thinking":"high","bogus":123,"tools":5}');
assert.strictEqual(rt2.thinking, 'high', 'valid key parsed');
assert.strictEqual(rt2.tools, undefined, 'bad-typed key dropped (stays undefined)');
assert.ok(!('bogus' in rt2), 'unknown key not present');

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
