// Topology catalog — hermetic checks. Run: node dist/core/topology.test.js
//
// 1) every preset round-trips through the REAL pods.yaml parser (renderer is
//    only trusted insofar as parseTeamYaml accepts it and returns the pods);
// 2) unknown names fail loudly;
// 3) topology_up op over a raw /api/ops endpoint (same path the core serves):
//    live pods refreshed, honest per-pod results, unknown name = op error.
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { openStore } from './store.js';
import { TOPOLOGIES, listTopologies, getTopology, renderTopologyYaml, topologySpec } from './topologies.js';
import { parseTeamYaml } from './team.js';
import { createHttp } from './http.js';
import type { CoreCtx } from './ops.js';

// --- 1) round-trip through the real parser, for every preset -----------------
assert.deepStrictEqual(listTopologies().map((t) => t.name), [
  'conveyor',
  'adversarial-review',
  'research-team',
  'secrets-manager',
]);
for (const t of TOPOLOGIES) {
  const yaml = renderTopologyYaml(t.name);
  const spec = topologySpec(t.name);
  // same roles, in order (the chain order is part of the contract)
  assert.deepStrictEqual(Object.keys(spec.pods), Object.keys(t.pods), `topology ${t.name}: roles drifted`);
  for (const [role, want] of Object.entries(t.pods)) {
    const got = spec.pods[role];
    assert.strictEqual(got.agent, want.agent, `${t.name}/${role}.agent`);
    assert.strictEqual(got.guidance, want.guidance, `${t.name}/${role}.guidance drifted in the round-trip`);
  }
  // and the standalone parser accepts the rendered document too
  assert.deepStrictEqual(parseTeamYaml(yaml).pods, spec.pods);
}
// a preset that names the next stage in its handoff chain: the target role
// must exist in the same topology (no handoff into a void)
for (const t of TOPOLOGIES) {
  const roles = new Set(Object.keys(t.pods));
  for (const [role, p] of Object.entries(t.pods)) {
    for (const m of (p.guidance ?? '').matchAll(/handoff <id> ([a-z-]+)/g)) {
      assert.ok(roles.has(m[1]), `${t.name}/${role}: handoff target '${m[1]}' is not in the topology`);
    }
  }
}

// --- 2) unknown names fail loudly --------------------------------------------
assert.throws(() => getTopology('nope'), /no topology: nope/);
assert.throws(() => renderTopologyYaml('nope'), /no topology: nope/);

// --- 3) topology_up over a raw /api/ops endpoint ------------------------------
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-topology-'));
const home = path.join(tmp, 'core');
{
  // live bash pods for every conveyor role (seeded like fleet.test.ts: no
  // tmux, no spawn — the live branch of reconcile only re-merges guidance)
  const roles = Object.keys(getTopology('conveyor').pods);
  const store = openStore(home);
  const raw = new DatabaseSync(path.join(home, 'flock.db'));
  for (const role of roles) {
    const dir = path.join(home, 'pods', role);
    fs.mkdirSync(dir, { recursive: true });
    raw.prepare("INSERT OR IGNORE INTO pods(id, role, dir, state, agent, created_at) VALUES (?, ?, ?, 'live', 'bash', ?)")
      .run('pod_' + role, role, dir, new Date().toISOString());
  }
  raw.close();
  void store;

  const ctx: CoreCtx = { store: openStore(home) as never, ticks: { register() {}, all: () => [] } as never, startedAt: new Date().toISOString(), emit: () => {} };
  const app = createHttp(ctx).app;
  const token = fs.readFileSync(path.join(home, 'token'), 'utf8').trim();

  async function ops(op: Record<string, unknown>): Promise<{ ok: boolean; result?: unknown; error?: string }> {
    const res = await app.fetch(new Request('http://core/api/ops', {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
      body: JSON.stringify(op),
    }));
    return (await res.json()) as never;
  }

  // unknown name: honest op error, not a crash
  const bad = await ops({ type: 'topology_up', name: 'nope' });
  assert.strictEqual(bad.ok, false);
  assert.match(bad.error ?? '', /no topology: nope/);

  // known preset: all live pods reconciled, yaml materialized in the result
  const good = await ops({ type: 'topology_up', name: 'conveyor' });
  assert.strictEqual(good.ok, true, `topology_up failed: ${good.error}`);
  const r = good.result as { topology: string; yaml: string; results: Record<string, { action: string }> };
  assert.strictEqual(r.topology, 'conveyor');
  assert.deepStrictEqual(Object.keys(r.results).sort(), Object.keys(getTopology('conveyor').pods).sort());
  for (const [role, res] of Object.entries(r.results)) assert.strictEqual(res.action, 'live', `${role}: ${JSON.stringify(res)}`);
  assert.strictEqual(parseTeamYaml(r.yaml).pods.intake.agent, 'pi');
}

fs.rmSync(tmp, { recursive: true, force: true });
console.log('topology.test.js: all checks passed');
