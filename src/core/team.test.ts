// team parser — hermetic checks. Run: node dist/core/team.test.js
import assert from 'node:assert';
import { parseTeamYaml, TeamParseError } from './team.js';

// 1) full subset
{
  const spec = parseTeamYaml(`
# team file
pods:
  dev:
    agent: pi
    model: cat-vllm/qwen3.8-27b-fp8
    guidance: |
      Будь краток.
      Коммить свою работу.
  rev:
    agent: bash
    profile: loud
    posture: full_bypass
    guidance: one-liner guidance
  bashpod:
    agent: bash
`);
  assert.deepStrictEqual(Object.keys(spec.pods).sort(), ['bashpod', 'dev', 'rev']);
  assert.strictEqual(spec.pods.dev.agent, 'pi');
  assert.strictEqual(spec.pods.dev.model, 'cat-vllm/qwen3.8-27b-fp8');
  assert.strictEqual(spec.pods.dev.guidance, 'Будь краток.\nКоммить свою работу.');
  assert.strictEqual(spec.pods.rev.agent, 'bash');
  assert.strictEqual(spec.pods.rev.profile, 'loud');
  assert.strictEqual(spec.pods.rev.posture, 'full_bypass');
  assert.strictEqual(spec.pods.rev.guidance, 'one-liner guidance');
  assert.deepStrictEqual(spec.pods.bashpod, { agent: 'bash' });
}

// 2) guidance block then more fields of the same role
{
  const spec = parseTeamYaml(`pods:
  dev:
    guidance: |
      line1
      line2
    agent: pi
`);
  assert.strictEqual(spec.pods.dev.guidance, 'line1\nline2');
  assert.strictEqual(spec.pods.dev.agent, 'pi');
}

// 3) errors
const expectFail = (src: string, re: RegExp) => {
  assert.throws(() => parseTeamYaml(src), (e: unknown) => e instanceof TeamParseError && re.test(e.message));
};
expectFail('other: {}\n', /unknown top-level/);
expectFail('pods:\n  Dev:\n    agent: pi\n', /bad pod role/);
expectFail('pods:\n  dev:\n    agent\n', /bad field/);
expectFail('pods:\n  dev:\n    unknown_key: x\n', /unknown field/);
expectFail('pods:\n  dev:\n    posture: high\n', /posture/);
expectFail('pods:\n  dev:\n    model:\n', /empty value/);
expectFail('pods:\n', /no pods declared/);
expectFail('pods:\n  dev\n', /role line must end/);

console.log('team: all checks passed');
