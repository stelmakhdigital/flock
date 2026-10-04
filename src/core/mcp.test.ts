// C9: MCP stdio server — hermetic (no core needed for tools/list; the
// tools/call surface is covered by the live check in CI-free smoke).
// Run: node dist/core/mcp.test.js
import assert from 'node:assert';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const OP_COUNT = 35; // OP_REGISTRY entries (keep in sync; the per-tool checks below are the real guarantee)

const here = path.dirname(fileURLToPath(import.meta.url));

const child = spawn(process.execPath, [path.join(here, '..', 'bin.js'), 'mcp', 'serve'], { stdio: ['pipe', 'pipe', 'pipe'] });
let childErr = '';
child.stderr.on('data', (d) => (childErr += d.toString()));
let buf = '';
const pending = new Map<string | number, (line: string) => void>();
child.stdout.on('data', (d) => {
  buf += d.toString();
  let i: number;
  while ((i = buf.indexOf('\n')) !== -1) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    const msg = JSON.parse(line) as { id?: string | number };
    const key = msg.id as string | number;
    const cb = pending.get(key);
    if (cb) {
      pending.delete(key);
      cb(line);
    }
  }
});

function rpc(id: string | number, method: string, params?: Record<string, unknown>): Promise<any> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`timeout: ${method}\n${childErr.slice(-300)}`)), 3000);
    pending.set(id, (line) => {
      clearTimeout(t);
      resolve(JSON.parse(line));
    });
    child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
  });
}

// 1) initialize
const init = await rpc(1, 'initialize', { protocolVersion: '2025-03-26' });
assert.strictEqual(init.result.serverInfo.name, 'flock');
assert.ok(init.result.capabilities.tools, 'tools capability advertised');

// 2) tools/list: every OP_REGISTRY op is a tool
const list = await rpc(2, 'tools/list');
const tools = list.result.tools as { name: string; description: string; inputSchema: { type: string } }[];
assert.ok(tools.length >= OP_COUNT, `all ops exported (got ${tools.length}, want >= ${OP_COUNT})`);
for (const name of ['pod_spawn', 'task_done', 'task_handoff', 'message_send', 'workflow_start', 'pod_relaunch']) {
  assert.ok(tools.some((t) => t.name === name), `tool present: ${name}`);
}
for (const t of tools) {
  assert.strictEqual(t.inputSchema.type, 'object', `${t.name}: permissive object schema`);
  assert.ok(t.description.length > 0, `${t.name}: has a description`);
}

// 3) unknown method -> -32601
const bad = await rpc(3, 'bogus/method');
assert.strictEqual(bad.error?.code, -32601);

// 4) unknown tool -> -32602
const badTool = await rpc(4, 'tools/call', { name: 'nope', arguments: {} });
assert.strictEqual(badTool.error?.code, -32602);

child.kill();
console.log('mcp: all checks passed');
