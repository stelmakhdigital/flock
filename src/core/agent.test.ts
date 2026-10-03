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
