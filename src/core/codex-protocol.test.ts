// Hermetic checks for the codex runtime protocol (pure).
// Run: node dist/core/codex-protocol.test.js
import assert from 'node:assert';
import {
  validateCodexSessionToken,
  codexShimPort,
  codexHome,
  buildCodexConfig,
  parseCodexEvent,
  rewriteResponsesRequest,
  buildCodexBridgeCommand,
  buildCodexChildEnv,
} from './codex-protocol.js';

// ── validateCodexSessionToken: UUID v7 thread ids ──
{
  const ok = '01a1059d-5279-77b1-a79f-991941e01774';
  assert.deepStrictEqual(validateCodexSessionToken(ok), { ok: true, token: ok });
  assert.deepStrictEqual(validateCodexSessionToken(' ' + ok.toUpperCase() + ' '), { ok: true, token: ok }); // trimmed + lowered
  for (const bad of ['', '   ', 'not-a-uuid', '01a1059d-5279-77b1-a79f-991941e0177', '01a1059d527977b1a79f991941e01774', '/tmp/sessions/x.jsonl', 42, null]) {
    assert.strictEqual(validateCodexSessionToken(bad as unknown).ok, false, `bad token accepted: ${JSON.stringify(bad)}`);
  }
}

// ── codexShimPort / codexHome ──
assert.strictEqual(codexShimPort(7460), 7471);
assert.strictEqual(codexShimPort(7461), 7472);
assert.strictEqual(codexHome('/h/pods/dev'), '/h/pods/dev/.codex');

// ── buildCodexConfig: provider always -> in-core shim ──
{
  const toml = buildCodexConfig({ model: 'qwen3.8-27b-fp8', shimPort: 7471 });
  assert.ok(toml.includes('model = "qwen3.8-27b-fp8"'));
  assert.ok(toml.includes('base_url = "http://127.0.0.1:7471/v1"'));
  assert.ok(toml.includes('wire_api = "responses"'));
  assert.ok(toml.includes('approval_policy = "never"'));
  assert.ok(toml.includes('sandbox_mode = "danger-full-access"'));
  assert.ok(toml.includes('env_key = "FLOCK_VLLM_KEY"'));
  // no model: the line is omitted entirely (codex default)
  const bare = buildCodexConfig({ shimPort: 7471 });
  assert.ok(!bare.includes('model ='), 'model line omitted when unset');
}

// ── parseCodexEvent: every observed codex exec JSONL shape ──
{
  assert.deepStrictEqual(parseCodexEvent('{"type":"thread.started","thread_id":"01a1059d-5279-77b1-a79f-991941e01774"}'), {
    kind: 'thread',
    threadId: '01a1059d-5279-77b1-a79f-991941e01774',
  });
  assert.deepStrictEqual(parseCodexEvent('{"type":"turn.started"}'), { kind: 'turn_start' });
  assert.deepStrictEqual(
    parseCodexEvent('{"type":"turn.completed","usage":{"input_tokens":18631,"cached_input_tokens":15680,"cache_write_input_tokens":0,"output_tokens":106,"reasoning_output_tokens":0}}'),
    { kind: 'turn_complete', usage: { input: 18631, output: 106, cacheRead: 15680, cacheWrite: 0 } },
  );
  assert.deepStrictEqual(parseCodexEvent('{"type":"turn.completed"}'), { kind: 'turn_complete', usage: undefined });
  assert.deepStrictEqual(parseCodexEvent('{"type":"turn.failed","error":{"message":"boom"}}'), { kind: 'turn_failed', error: 'boom' });
  assert.deepStrictEqual(parseCodexEvent('{"type":"item.completed","item":{"type":"agent_message","text":"сделано"}}'), {
    kind: 'agent_message',
    text: 'сделано',
  });
  assert.deepStrictEqual(parseCodexEvent('{"type":"item.completed","item":{"type":"error","message":"Model metadata not found"}}'), {
    kind: 'error',
    message: 'Model metadata not found',
  });
  assert.deepStrictEqual(parseCodexEvent('{"type":"item.completed","item":{"type":"command_execution"}}'), { kind: 'tool', name: 'command' });
  assert.deepStrictEqual(parseCodexEvent('{"type":"error","message":"network down"}'), { kind: 'error', message: 'network down' });
  // noise: non-JSON, empty, unknown types, non-object JSON
  assert.strictEqual(parseCodexEvent('WARNING: proceeding, even though we could not create PATH aliases: x').kind, 'noise');
  assert.strictEqual(parseCodexEvent('  ').kind, 'noise');
  assert.strictEqual(parseCodexEvent('{"type":"something.new"}').kind, 'noise');
  assert.strictEqual(parseCodexEvent('[1,2,3]').kind, 'noise');
  assert.strictEqual(parseCodexEvent('42').kind, 'noise');
}

// ── rewriteResponsesRequest: developer role -> instructions ──
{
  // developer messages merge into instructions; user input survives order
  const req = JSON.stringify({
    model: 'm',
    instructions: 'base prompt',
    input: [
      { role: 'developer', content: 'dev context' },
      { role: 'user', content: [{ type: 'input_text', text: 'hi' }] },
      { role: 'developer', content: [{ type: 'input_text', text: 'second dev' }] },
    ],
  });
  const rw = rewriteResponsesRequest(req);
  assert.strictEqual(rw.changed, true);
  const d = JSON.parse(rw.body);
  assert.deepStrictEqual(d.input, [{ role: 'user', content: [{ type: 'input_text', text: 'hi' }] }]);
  assert.ok(d.instructions.startsWith('base prompt'));
  assert.ok(d.instructions.includes('dev context'));
  assert.ok(d.instructions.includes('second dev'));
  assert.ok(d.instructions.indexOf('dev context') < d.instructions.indexOf('second dev'), 'dev messages keep order');

  // no developer role: byte-for-byte pass-through
  const plain = JSON.stringify({ instructions: 'x', input: [{ role: 'user', content: 'hi' }] });
  const plainRw = rewriteResponsesRequest(plain);
  assert.strictEqual(plainRw.changed, false);
  assert.strictEqual(plainRw.body, plain);

  // no instructions: merged text becomes the instructions
  const noInstr = JSON.stringify({ input: [{ role: 'developer', content: 'solo dev' }, { role: 'user', content: 'u' }] });
  const ni = JSON.parse(rewriteResponsesRequest(noInstr).body);
  assert.strictEqual(ni.instructions, 'solo dev');

  // non-JSON body: pass-through
  const notJson = rewriteResponsesRequest('streaming chunk?');
  assert.deepStrictEqual(notJson, { body: 'streaming chunk?', changed: false });

  // empty-string content items: no empty fragments appended
  const emptyDev = JSON.stringify({ input: [{ role: 'developer', content: [{ type: 'input_text', text: '' }] }, { role: 'user', content: 'u' }] });
  const ed = JSON.parse(rewriteResponsesRequest(emptyDev).body);
  assert.strictEqual(ed.instructions, '');
}

// ── buildCodexBridgeCommand: one-line pane command, quoting ──
{
  const cmd = buildCodexBridgeCommand({
    bridgePath: '/d/core/codex-bridge.js',
    stateRoot: '/h',
    role: 'dev',
    cwd: '/h/pods/dev',
    launchId: 'la_1',
    command: 'codex',
    shimPort: 7471,
    model: 'qwen3.8-27b-fp8',
    resumeThread: '01a1059d-5279-77b1-a79f-991941e01774',
    keyEnv: { FLOCK_VLLM_KEY: 'sk-dummy' },
  });
  assert.ok(cmd.startsWith("node '/d/core/codex-bridge.js' "));
  assert.ok(cmd.includes("--launch-id 'la_1'"));
  assert.ok(cmd.includes("--command 'codex'"));
  assert.ok(cmd.includes('--shim-port 7471'));
  assert.ok(cmd.includes("--resume-thread '01a1059d-5279-77b1-a79f-991941e01774'"));
  assert.ok(cmd.includes("--env 'FLOCK_VLLM_KEY' 'sk-dummy'"), cmd);
  // fork: no resume flag
  const fork = buildCodexBridgeCommand({
    bridgePath: '/b', stateRoot: '/h', role: 'r', cwd: '/c', launchId: 'la_2',
    command: 'codex', shimPort: 7471, forkRef: '01a1059d-5279-77b1-a79f-991941e01774',
  });
  assert.ok(fork.includes('--fork-ref'));
  assert.ok(!fork.includes('--resume-thread'));
  // apostrophe in a path gets shell-escaped
  const q = buildCodexBridgeCommand({
    bridgePath: "/it's/bridge.js", stateRoot: '/h', role: 'r', cwd: '/c', launchId: 'la_3',
    command: 'codex', shimPort: 7471,
  });
  assert.ok(q.includes("'/it'\\''s/bridge.js'"));
  // C10: raw child args/env ride in ONE JSON flag (form a, same as pi)
  const c = buildCodexBridgeCommand({
    bridgePath: '/b', stateRoot: '/h', role: 'r', cwd: '/c', launchId: 'la_4',
    command: 'codex', shimPort: 7471,
    childArgs: ['--sandbox', 'workspace-write'], childEnv: { MY_VAR: '1' },
  });
  assert.ok(c.includes('--child-args'), 'child-args serialized');
  assert.ok(c.includes('--child-env'), 'child-env serialized');
  assert.ok(c.includes('\"--sandbox\"'), 'raw args verbatim in the JSON');
}

// ── buildCodexChildEnv: deny-by-default baseline + CODEX_HOME + keys ──
{
  const env = buildCodexChildEnv(
    { PATH: '/bin', HOME: '/h', FLOCK_HOME: '/fh', FLOCK_PORT: '7460', FLOCK_POD_ROLE: 'dev', SECRET: 'nope' },
    { codexHome: '/seat/.codex', keyEnv: { FLOCK_VLLM_KEY: 'sk-dummy' } },
  );
  assert.strictEqual(env.PATH, '/bin');
  assert.strictEqual(env.FLOCK_HOME, '/fh');
  assert.strictEqual(env.FLOCK_POD_ROLE, 'dev');
  assert.strictEqual(env.CODEX_HOME, '/seat/.codex');
  assert.strictEqual(env.FLOCK_VLLM_KEY, 'sk-dummy');
  assert.strictEqual(env.SECRET, undefined, 'non-baseline vars do not cross the boundary');
  assert.strictEqual(env.PI_CODING_AGENT_DIR, undefined);
}

console.log('codex-protocol: all checks passed');
