// agent.ts merge/imports/profiles — hermetic (tmp FLOCK_HOME).
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { mergeManifests, resolveManifest, resolveAgent, type AgentManifest } from './agent.js';

// ---- mergeManifests -------------------------------------------------------
const base: AgentManifest = {
  id: 'a',
  command: 'pi',
  args: ['--base'],
  env: { X: '1', Y: '2' },
  guidance: [{ id: 'g1', content: 'base' }, { id: 'g2', content: 'base' }],
  trust: 'no-approve',
};

let m = mergeManifests(base, {});
assert.deepStrictEqual(m.args, ['--base'], 'empty ext keeps base args');
assert.strictEqual(m.command, 'pi', 'empty ext keeps command');
assert.strictEqual(m.trust, 'no-approve');

m = mergeManifests(base, { trust: 'approve', env: { Y: '3', Z: '4' }, args: ['--ext'] });
assert.strictEqual(m.trust, 'approve', 'scalar overridden');
assert.deepStrictEqual(m.args, ['--base', '--ext'], 'args concatenate');
assert.deepStrictEqual(m.env, { X: '1', Y: '3', Z: '4' }, 'env key-merged');

m = mergeManifests(base, { guidance: [{ id: 'g2', content: 'overridden' }, { id: 'g3', content: 'new' }] });
assert.deepStrictEqual(m.guidance, [
  { id: 'g1', content: 'base' },
  { id: 'g2', content: 'overridden' },
  { id: 'g3', content: 'new' },
], 'guidance merges by id, keeps order, appends new');

m = mergeManifests(base, { args: undefined, command: undefined as unknown as string });
assert.strictEqual(m.command, 'pi', 'undefined ext fields do not clobber');
assert.deepStrictEqual(m.args, ['--base']);

// ---- resolveManifest (imports graph) --------------------------------------
const agents: Record<string, AgentManifest> = {
  c: { id: 'c', command: 'pi', args: ['--c'], env: { A: 'c' } },
  b: { id: 'b', command: 'pi', args: ['--b'], env: { A: 'b', B: 'b' }, imports: ['c'] },
  a: { id: 'a', command: 'pi', env: { A: 'a' }, imports: ['b'] },
};
const ra = resolveManifest(agents.a, agents);
assert.deepStrictEqual(ra.args, ['--c', '--b'], 'import args: base first, then importer');
assert.deepStrictEqual(ra.env, { A: 'a', B: 'b' }, 'import env: importer wins per key');
assert.strictEqual(ra.command, 'pi');

// cycle
agents.loop1 = { id: 'loop1', command: 'pi', imports: ['loop2'] };
agents.loop2 = { id: 'loop2', command: 'pi', imports: ['loop1'] };
assert.throws(() => resolveManifest(agents.loop1, agents), /cycle/);

// unknown import
agents.bad = { id: 'bad', command: 'pi', imports: ['nope'] };
assert.throws(() => resolveManifest(agents.bad, agents), /unknown import/);

// ---- profiles (through resolveAgent, tmp FLOCK_HOME) -----------------------
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-agent-test-'));
const prevHome = process.env.FLOCK_HOME;
process.env.FLOCK_HOME = tmp;
try {
  fs.mkdirSync(path.join(tmp, 'agents'));
  fs.writeFileSync(path.join(tmp, 'agents', 'multi.json'), JSON.stringify({
    id: 'multi',
    command: 'pi',
    modelFlag: '--model',
    args: ['--shared'],
    env: { MODE: 'base' },
    guidance: [{ id: 'shared', content: 'SHARED' }],
    profiles: {
      small: { env: { MODE: 'small' }, args: ['--small'] },
      loud: { env: { MODE: 'loud' } },
    },
  }));

    const r1 = resolveAgent('multi', null, 'small');
  assert.ok(r1, 'profile resolves');
  assert.deepStrictEqual(r1!.manifest.args ?? [], ['--shared', '--small'], 'profile args append');
  assert.strictEqual(r1!.manifest.env?.MODE, 'small', 'profile env overrides');
  assert.strictEqual(r1!.manifest.guidance?.[0]?.id, 'shared', 'guidance survives profile merge');

  const r2 = resolveAgent('multi', 'm1', 'loud');
  assert.strictEqual(r2!.manifest.env?.MODE, 'loud');
  assert.strictEqual(r2!.cmd, 'pi --shared --model m1', 'model flag appended after args');

  assert.throws(() => resolveAgent('multi', null, 'nope'), /unknown profile/);

  const r3 = resolveAgent('multi', null);

  // ---- T1: pi first-class axes -----------------------------------------
  // merge semantics: scalars ext-wins, arrays concat, mcp per server name
  const ax1 = mergeManifests(
    { id: 'x', command: 'pi', thinking: 'low', tools: ['read'], skills: ['/s/a'], extensions: ['builtin:e'], mcp: { a: { command: 'aa' }, b: { command: 'bb' } } },
    { thinking: 'high', tools: ['bash'], skills: ['/s/b'], extensions: ['/ext/f.ts'], mcp: { a: { command: 'aa2' }, c: { url: 'http://c' } } },
  );
  assert.strictEqual(ax1.thinking, 'high', 'thinking: scalar ext-wins');
  assert.deepStrictEqual(ax1.tools, ['read', 'bash'], 'tools: concat (like args)');
  assert.deepStrictEqual(ax1.skills, ['/s/a', '/s/b'], 'skills: concat');
  assert.deepStrictEqual(ax1.extensions, ['builtin:e', '/ext/f.ts'], 'extensions: concat');
  assert.deepStrictEqual(ax1.mcp, { a: { command: 'aa2' }, b: { command: 'bb' }, c: { url: 'http://c' } }, 'mcp: per-server merge, same name replaces wholesale');

  // boolean axes ext-wins; empty arrays drop out (no "allow nothing" flag)
  const ax2 = mergeManifests({ id: 'y', command: 'pi', noSkills: true }, { noSkills: false });
  assert.strictEqual(ax2.noSkills, false, 'noSkills: scalar ext-wins');
  const ax3 = mergeManifests({ id: 'z', command: 'pi' }, {});
  assert.strictEqual(ax3.tools, undefined, 'unset array axes stay unset');
  const ax4 = mergeManifests({ id: 'z2', command: 'pi', tools: [] }, {});
  assert.strictEqual(ax4.tools, undefined, 'empty tools drops out of the merge');

  // full chain: imports + profile through resolveAgent
  fs.writeFileSync(path.join(tmp, 'agents', 'axes.json'), JSON.stringify({
    id: 'axes-base',
    command: 'pi',
    modelFlag: '--model',
    thinking: 'low',
    tools: ['read'],
    mcp: { stub: { command: 'echo', args: ['stub'] } },
    profiles: { careful: { thinking: 'xhigh', tools: ['bash'], excludeTools: ['edit'], appendSystemPrompt: ['be careful'] } },
  }));
  fs.writeFileSync(path.join(tmp, 'agents', 'axes-kid.json'), JSON.stringify({
    id: 'axes-kid',
    command: 'pi',
    imports: ['axes-base'],
    skills: ['/base/skill'],
    profiles: { cheap: { thinking: 'minimal', noSkills: true } },
  }));
  // careful profile on the base: profile axes override/append the base axes
  const axc = resolveAgent('axes-base', null, 'careful')!;
  assert.strictEqual(axc.manifest.thinking, 'xhigh', 'profile thinking overrides base scalar');
  assert.deepStrictEqual(axc.manifest.tools, ['read', 'bash'], 'base + profile tools concat');
  assert.deepStrictEqual(axc.manifest.excludeTools, ['edit'], 'profile-only axis appears');
  assert.deepStrictEqual(axc.manifest.appendSystemPrompt, ['be careful'], 'profile appendSystemPrompt');
  assert.deepStrictEqual(axc.manifest.mcp, { stub: { command: 'echo', args: ['stub'] } }, 'mcp intact, profile did not touch it');
  // no profile: the import chain resolves (kid inherits base axes + its own skills)
  const axplain = resolveAgent('axes-kid')!;
  assert.strictEqual(axplain.manifest.thinking, 'low', 'imported thinking without profile');
  assert.deepStrictEqual(axplain.manifest.tools, ['read'], 'imported tools without profile');
  assert.deepStrictEqual(axplain.manifest.skills, ['/base/skill'], 'importer skills kept');
  // own profile of the kid: profile wins over imported scalar, imported axes survive
  const axcheap = resolveAgent('axes-kid', null, 'cheap')!;
  assert.strictEqual(axcheap.manifest.thinking, 'minimal', 'own profile wins over imported scalar');
  assert.strictEqual(axcheap.manifest.noSkills, true, 'own profile boolean axis');
  assert.deepStrictEqual(axcheap.manifest.mcp, { stub: { command: 'echo', args: ['stub'] } }, 'mcp imported through the profile merge');

  // thinking validation: closed set, error lists the valid values
  fs.writeFileSync(path.join(tmp, 'agents', 'badthinking.json'), JSON.stringify({ id: 'badthinking', command: 'pi', thinking: 'ultra' }));
  assert.throws(() => resolveAgent('badthinking'), /unknown thinking level: ultra \(valid: off, minimal, low, medium, high, xhigh, max\)/);

  assert.strictEqual(r3!.manifest.env?.MODE, 'base', 'no profile = base manifest');

  // imports through the on-disk loader
  fs.writeFileSync(path.join(tmp, 'agents', 'base-agent.json'), JSON.stringify({
    id: 'base-agent',
    command: 'pi',
    trust: 'no-approve',
    guidance: [{ id: 'base-rule', content: 'B' }],
  }));
  fs.writeFileSync(path.join(tmp, 'agents', 'child-agent.json'), JSON.stringify({
    id: 'child-agent',
    command: 'pi',
    imports: ['base-agent'],
    trust: 'approve',
    guidance: [{ id: 'child-rule', content: 'C' }],
  }));
  const rc = resolveAgent('child-agent', null);
  assert.ok(rc, 'child resolves');
  assert.strictEqual(rc!.manifest.trust, 'approve', 'child scalar wins over imported');
  assert.deepStrictEqual(
    rc!.manifest.guidance?.map((g) => g.id).sort(),
    ['base-rule', 'child-rule'],
    'guidance accumulates across imports',
  );
} finally {
  if (prevHome === undefined) delete process.env.FLOCK_HOME;
  else process.env.FLOCK_HOME = prevHome;
  fs.rmSync(tmp, { recursive: true, force: true });
}

console.log('agent: all checks passed');
