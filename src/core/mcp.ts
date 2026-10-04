// C9: `flock mcp serve` — an MCP (Model Context Protocol) server over
// stdio JSON-RPC. Zero new dependencies: the JSON-RPC framing is a few
// lines of readline, the tools are OP_REGISTRY itself.
//
// v1 surface: initialize, tools/list, tools/call (apply() as the operator
// token; the core's token comes from the FLOCK_HOME token file or
// FLOCK_TOKEN). Everything else is an error response — deliberately
// minimal, extended only when a concrete client needs it.
//
// The tools are the coordination plane itself: an LLM client (pi, claude,
// anything MCP-speaking) gets the SAME ops the CLI and the pods use,
// through the SAME single mutation path.

import os from 'node:os';
import path from 'node:path';
import readline from 'node:readline';
import { OP_REGISTRY } from './ops.js';

const JSONRPC = '2.0';

interface RpcRequest {
  jsonrpc?: string;
  id?: string | number | null;
  method?: string;
  params?: Record<string, unknown>;
}

function respond(id: string | number | null, result: unknown): void {
  process.stdout.write(JSON.stringify({ jsonrpc: JSONRPC, id, result }) + '\n');
}

function respondError(id: string | number | null, code: number, message: string): void {
  process.stdout.write(JSON.stringify({ jsonrpc: JSONRPC, id, error: { code, message } }) + '\n');
}

// Q3 (from the prompt's open questions, resolved as implemented): every op
// is exported. Ops with a formal validate get their summary; the
// inputSchema is a permissive object (the real validation lives in the op
// handlers, where they can see ctx — a strict generated schema would lie
// about the surface).
function toolList() {
  return Object.entries(OP_REGISTRY).map(([name, d]) => ({
    name,
    description: `[${d.group}] ${d.summary}`,
    inputSchema: { type: 'object' },
  }));
}

// tools/call -> apply(op) through the core HTTP API (the SAME path the CLI
// uses; the pod tokens stay pod-scoped, this is the operator surface).
// ponytail: a direct in-process apply() would need a full CoreCtx (store,
// ticks, emit) and would bypass the auth boundary — calling the running
// core over HTTP keeps ONE writer and the token check.
async function toolCall(name: string, args: Record<string, unknown>): Promise<{ content: { type: string; text: string }[] }> {
  const token = process.env.FLOCK_TOKEN ?? (await readFlockToken());
  const port = process.env.FLOCK_PORT ? Number(process.env.FLOCK_PORT) : 7460;
  // profile-aware base: `flock -p <name> mcp serve` set FLOCK_HOME/FLOCK_PORT
  const res = await fetch(`http://127.0.0.1:${port}/api/ops`, {
    method: 'POST',
    headers: { authorization: `Bearer ${token ?? ''}`, 'content-type': 'application/json' },
    body: JSON.stringify({ type: name, ...args }),
  });
  const data = (await res.json().catch(() => null)) as { ok?: boolean; result?: unknown; error?: string } | null;
  const text = JSON.stringify(data ?? { error: `core HTTP ${res.status}` }, null, 2);
  return { content: [{ type: 'text', text }] };
}

async function readFlockToken(): Promise<string | null> {
  try {
    const home = process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock');
    const { readFileSync } = await import('node:fs');
    return readFileSync(path.join(home, 'token'), 'utf8').trim();
  } catch {
    return null;
  }
}

export async function mcpServe(): Promise<void> {
  const rl = readline.createInterface({ input: process.stdin, terminal: false });
  for await (const line of rl) {
    const s = line.trim();
    if (!s) continue;
    let req: RpcRequest;
    try {
      req = JSON.parse(s) as RpcRequest;
    } catch {
      respondError(null, -32700, 'parse error');
      continue;
    }
    const id = req.id ?? null;
    const method = String(req.method ?? '');
    try {
      switch (method) {
        case 'initialize':
          respond(id, {
            protocolVersion: String(req.params?.protocolVersion ?? '2025-03-26'),
            capabilities: { tools: {} },
            serverInfo: { name: 'flock', version: '1.0' },
          });
          break;
        case 'notifications/initialized':
        case 'initialized':
          break; // notification: no response
        case 'tools/list':
          respond(id, { tools: toolList() });
          break;
        case 'tools/call': {
          const name = String(req.params?.name ?? '');
          const args = (req.params?.arguments ?? {}) as Record<string, unknown>;
          if (!OP_REGISTRY[name]) {
            respondError(id, -32602, `unknown tool: ${name || '(empty)'}`);
            break;
          }
          const out = await toolCall(name, args);
          respond(id, out);
          break;
        }
        default:
          // notifications (no id) are acknowledged by silence; unknown
          // REQUESTS get method-not-found
          if (req.id !== undefined) respondError(id, -32601, `method not found: ${method}`);
      }
    } catch (e) {
      respondError(id, -32603, e instanceof Error ? e.message : String(e));
    }
  }
}
