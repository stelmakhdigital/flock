import fs from 'node:fs';
import path from 'node:path';
import * as store from './store.js';
import * as terminal from './terminal.js';
import type { Ticks } from './ticks.js';

// apply(op) — the SINGLE mutation path.
// Ticks, CLI, and (stage 4) LLM intents all mutate the world through ops.
// Delivery ops (post_send) are transport actions, not state mutations:
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

function requireLivePost(ctx: CoreCtx, role: string): store.Post {
  const post = store.getPostByRole(ctx.store, role);
  if (!post || post.state !== 'live' || !post.terminal_target) {
    throw new OpError(404, `no live post: ${role}`);
  }
  return post;
}

export async function apply(op: Record<string, unknown> | null, ctx: CoreCtx): Promise<unknown> {
  const o = op ?? {};
  const t = String(o.type ?? '');
  switch (t) {
    case 'post_spawn':
      return postSpawn(o, ctx);
    case 'post_send':
      return postSend(o, ctx);
    case 'post_capture':
      return postCapture(o, ctx);
    case 'post_close':
      return postClose(o, ctx);
    case 'terminal_check':
      return terminalCheck(ctx);
    default:
      throw new OpError(400, `unknown op: ${t || '(empty)'}`);
  }
}

async function postSpawn(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const existing = store.getPostByRole(ctx.store, role);
  if (existing && existing.state !== 'closed') {
    throw new OpError(409, `post ${role} already ${existing.state}`);
  }
  const pod = String(op.pod ?? 'default');
  const dir = String(op.dir ?? path.join(ctx.store.home, 'posts', role));
  fs.mkdirSync(dir, { recursive: true });
  const cmd = op.cmd ? String(op.cmd) : 'pi';
  const { target, pid } = await terminal.spawnPost({ role, dir, cmd });
  store.openPost(ctx.store, {
    id: store.newId('post'),
    pod,
    role,
    dir,
    terminalTarget: target,
    model: op.model ? String(op.model) : null,
  });
  const run = store.insertRun(ctx.store, { id: store.newId('run'), postRole: role, pid });
  ctx.emit?.({ type: 'post_spawned', role, target, run: run.id });
  return { post: store.getPostByRole(ctx.store, role), run };
}

async function postSend(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const text = String(op.text ?? '');
  if (!text.trim()) throw new OpError(400, 'text required');
  const post = requireLivePost(ctx, role);
  await terminal.paste(post.terminal_target!, text);
  await terminal.sendEnter(post.terminal_target!);
  const run = store.currentRun(ctx.store, role);
  if (run && !run.ended_at) {
    store.appendRunMeta(ctx.store, run.id, { kind: 'sent', bytes: Buffer.byteLength(text) });
  }
  ctx.emit?.({ type: 'post_sent', role, bytes: Buffer.byteLength(text) });
  return { ok: true };
}

async function postCapture(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const lines = Math.max(10, Math.min(2000, Number(op.lines ?? 200)));
  const post = requireLivePost(ctx, role);
  const text = await terminal.capture(post.terminal_target!, lines);
  return { role, text };
}

async function postClose(op: Record<string, unknown>, ctx: CoreCtx): Promise<unknown> {
  const role = requireRole(op);
  const post = store.getPostByRole(ctx.store, role);
  if (!post) throw new OpError(404, `no post: ${role}`);
  try {
    await terminal.killWindow(role);
  } catch {
    // window may already be gone
  }
  const run = store.currentRun(ctx.store, role);
  if (run && !run.ended_at) store.endRun(ctx.store, run.id, 'done');
  store.setPostState(ctx.store, role, 'closed');
  ctx.emit?.({ type: 'post_closed', role });
  return { ok: true };
}

async function terminalCheck(ctx: CoreCtx): Promise<unknown> {
  return terminal.checkTransport(ctx.store.home);
}
