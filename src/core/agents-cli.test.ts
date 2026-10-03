// T2 hermetic: loadAgents accepts command-less skeletons; resolveAgent
// resolves them via imports; missing command after resolve -> clear error
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { loadAgents, resolveAgent, podRuntime } from './agent.js';
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-t2-'));
const prev = process.env.FLOCK_HOME;
process.env.FLOCK_HOME = tmp;
try {
  fs.mkdirSync(path.join(tmp, 'agents'), { recursive: true });
  fs.writeFileSync(path.join(tmp, 'agents', 'skel.json'), JSON.stringify({ id: 'skel', imports: ['pi'], profiles: {} }));
  fs.writeFileSync(path.join(tmp, 'agents', 'noimport.json'), JSON.stringify({ id: 'noimport', profiles: {} }));
  const agents = loadAgents();
  assert.ok(agents.skel, 'command-less skeleton is loaded');
  const r = resolveAgent('skel');
  assert.ok(r, 'skeleton resolves');
  assert.strictEqual(r.manifest.command, 'pi', 'command inherited from the imported pi manifest');
  assert.strictEqual(r.cmd.startsWith('pi'), true, 'cmd built from inherited command');
  assert.throws(() => resolveAgent('noimport'), /no command/, 'no command anywhere -> clear error');
  assert.strictEqual(podRuntime('noimport'), 'cmd', 'invalid manifest -> cmd runtime (no throw in podRuntime)');
  console.log('agents-cli: all checks passed');
} finally {
  if (prev === undefined) delete process.env.FLOCK_HOME; else process.env.FLOCK_HOME = prev;
  fs.rmSync(tmp, { recursive: true, force: true });
}
