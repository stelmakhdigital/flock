import fs from 'node:fs';
import path from 'node:path';
import * as store from './store.js';
import * as terminal from './terminal.js';
import type { Ticks } from './ticks.js';

// apply(op) — the SINGLE mutation path.
// Ticks, CLI, and (stage 4) LLM intents all mutate the world through ops.
// Delivery ops (pod_send) are transport actions, not state mutations:
// they are audited in runs.meta, while state transitions are logged in
// *_transitions (from stage 1, with tasks).

export interface CoreCtx {
  store: store.Store;
  ticks: Ticks;
  startedAt: string;
  emit?: (e: Record<string, unknown>) => void;
}

export class OpError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

const ROLE_RE = /^[a-z0-9][a-z0-9-]{0,30}$/;

function requireRole(op: Record<string, unknown>): string {
  const role = String(op.role ?? '');
  if (!ROLE_RE.test(role)) throw new OpError(400, `bad role (want [a-z0-9-]): ${role || '(empty)'}`);
  return role;
}

function requireLivePod(ctx: CoreCtx, role: string): store.Pod {
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod || pod.state !== 'live' || !pod.terminal_target) {
    throw new OpError(404, `no live pod: ${role}`);
  }
  return pod;
}

export async function apply(op: Record<string, unknown> | null, ctx: CoreCtx): Promise<unknown> {
  const o = op ?? {};
  const t = String(o.type ?? '');
  switch (t) {
    case 'pod_spawn':
      return podSpawn(o, ctx);
    case 'pod_send':
      return podSend(o, ctx);
    case 'pod_capture':
      return podCapture(o, ctx);
    case 'pod_close':
      return podClose(o, ctx);
    case 'terminal_check':
      return terminalCheck(ctx);
    default:
      throw new OpError(400, `unknown op: ${t || '(empty)'}`);
  }
}

async function podSpawn(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const existing = store.getPodByRole(ctx.store, role);
  if (existing && existing.state !== 'closed') {
    throw new OpError(409, `pod ${role} already ${existing.state}`);
  }
  const dir = String(op.dir ?? path.join(ctx.store.home, 'pods', role));
  fs.mkdirSync(dir, { recursive: true });
  const cmd = op.cmd ? String(op.cmd) : 'pi';
  const { target, pid } = await terminal.spawnPod({ role, dir, cmd });
  store.openPod(ctx.store, {
    id: store.newId('pod'),
    role,
    dir,
    terminalTarget: target,
    model: op.model ? String(op.model) : null,
  });
  const run = store.insertRun(ctx.store, { id: store.newId('run'), podRole: role, pid });
  ctx.emit?.({ type: 'pod_spawned', role, target, run: run.id });
  return { pod: store.getPodByRole(ctx.store, role), run };
}

async function podSend(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const text = String(op.text ?? '');
  if (!text.trim()) throw new OpError(400, 'text required');
  const pod = requireLivePod(ctx, role);
  await terminal.paste(pod.terminal_target!, text);
  await terminal.sendEnter(pod.terminal_target!);
  const run = store.currentRun(ctx.store, role);
  if (run && !run.ended_at) {
    store.appendRunMeta(ctx.store, run.id, { kind: 'sent', bytes: Buffer.byteLength(text) });
  }
  ctx.emit?.({ type: 'pod_sent', role, bytes: Buffer.byteLength(text) });
  return { ok: true };
}

async function podCapture(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const lines = Math.max(10, Math.min(2000, Number(op.lines ?? 200)));
  const pod = requireLivePod(ctx, role);
  const text = await terminal.capture(pod.terminal_target!, lines);
  return { role, text };
}

async function podClose(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const pod = store.getPodByRole(ctx.store, role);
  if (!pod) throw new OpError(404, `no pod: ${role}`);
  try {
    await terminal.killWindow(role);
  } catch {
    // window may already be gone
  }
  const run = store.currentRun(ctx.store, role);
  if (run && !run.ended_at) store.endRun(ctx.store, run.id, 'done');
  store.setPodState(ctx.store, role, 'closed');
  ctx.emit?.({ type: 'pod_closed', role });
  return { ok: true };
}

async function terminalCheck(ctx: CoreCtx): Promise<unknown> {
  return terminal.checkTransport(ctx.store.home);
}
