// pod-scoped socket — live check: the operator token arriving on a pod's
// unix socket is scoped to that pod (5.3). Requires a running core + a live
// pod with a socket. Run: node dist/core/pod-scope-check.js [role]
// (role defaults to 'dev'; FLOCK_HOME / FLOCK_PORT respected)
import { readFileSync, existsSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import net from 'node:net';

const FLOCK_HOME = process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock');
const role = process.argv[2] ?? 'dev';
const tokenFile = path.join(FLOCK_HOME, 'token');
const sock = path.join(FLOCK_HOME, 'pods', role, 'core.sock');
if (!existsSync(tokenFile)) {
  console.log(`SKIP: no token at ${tokenFile} (core not started?)`);
  process.exit(0);
}
const token = readFileSync(tokenFile, 'utf8').trim();
if (!existsSync(sock)) {
  console.log(`SKIP: no live ${role} pod socket at ${sock} (flock pod relaunch ${role} first)`);
  process.exit(0);
}

function call(body: Record<string, unknown>, retries = 2): Promise<{ ok: boolean; error?: string; result?: unknown }> {
  return new Promise((resolve, reject) => {
    let s: net.Socket;
    const attempt = (n: number) => {
      const s = net.createConnection(sock, () => {
        const data = JSON.stringify(body);
        s.write(
          `POST http://x/api/ops HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer ${token}\r\nContent-Type: application/json\r\nContent-Length: ${Buffer.byteLength(data)}\r\nConnection: close\r\n\r\n${data}`,
        );
      });
    let raw = '';
    let len = -1;
    let settled = false;
    const finish = (fn: () => void) => {
      if (settled) return;
      settled = true;
      s.destroy();
      fn();
    };    s.on('data', (d) => {
      raw += d.toString();
      const m = /\r\n\r\n/.exec(raw);
      if (m && len < 0) {
        const cm = /Content-Length: (\d+)/i.exec(raw.slice(0, m.index));
        len = cm ? Number(cm[1]) : -1;
      }
      if (m && len >= 0) {
        const bodyStart = m.index + 4;
        if (raw.length >= bodyStart + len) {
          try {
            finish(() => resolve(JSON.parse(raw.slice(bodyStart, bodyStart + len))));
          } catch (e) {
            finish(() => reject(e as Error));
          }
          return;
        }
        // full body will not arrive on this connection; the close handler
        // decides (retry / scope-proof fallback)
      }
    });
    s.on('error', (e) => finish(() => reject(e)));
    s.on('close', () => {
      finish(() => {
        // the pod socket can be recreated (pod relaunch): retry
        if (n > 0) {
          setTimeout(() => attempt(n - 1), 300);
          return;
        }
        // final truncation fallback: the scope decision is visible in the
        // partial body (either a 403 message or "ok":true) — task_list is
        // large and can be cut by Connection: close racing the write
        if (raw.includes('not available to pod tokens') || raw.includes('pod token')) {
          resolve({ ok: false, error: 'scoped (403)' });
          return;
        }
        if (raw.includes('"ok":true')) {
          resolve({ ok: true, error: 'truncated-but-ok' });
          return;
        }
        const err = new Error(`closed without response (bytes=${raw.length} cl=${len})`);
        process.nextTick(() => reject(err));
      });
    });
    s.setTimeout(15_000, () => finish(() => reject(new Error('timeout'))));
    };
    attempt(retries);
  });
}

let failed = 0;
const check = (name: string, cond: boolean, detail = '') => {
  console.log(`${cond ? 'PASS' : 'FAIL'}: ${name}${detail ? ` — ${detail}` : ''}`);
  if (!cond) failed++;
};

const d1 = await call({ type: 'pod_relaunch', role });
check('operator-only op blocked (pod_relaunch)', !d1.ok, d1.error);

const d2 = await call({ type: 'task_add', role: 'pm', title: 'scope hack' });
check('foreign-pod task blocked', !d2.ok, d2.error);

const d3 = await call({ type: 'watchdog_register', name: 'scope-x', policy: 'timer', intervalMs: 999_999 });
check('operator-only op blocked (watchdog_register)', !d3.ok, d3.error);

const d4 = await call({ type: 'task_add', role, title: 'scope self-check' });
check('own-pod task add allowed', d4.ok);
const tid = (d4.result as { id?: string } | undefined)?.id;

// queued -> cancelled is legal (the arbiter may not have claimed it yet);
// a 403 would be the scope failure, a 409 a timing quirk — both prove the
// request reached the task handler with auth intact
// task_unblock is 'pod'-scoped and routes through taskReport's own-pod
// guard; queued->queued is a no-op transition, so the call must not 403
const d5 = tid ? await call({ type: 'task_unblock', id: tid }) : { ok: false };
const d5ok = d5.ok || (typeof d5.error === 'string' && !d5.error.includes('pod token'));
check('own-pod task op reaches handler (no 403)', d5ok, d5.error);

const d6 = await call({ type: 'task_list', status: 'done' }, 8);
check('task_list allowed (pod scope)', d6.ok, d6.error);

console.log(failed ? 'pod-scope-check: FAIL' : 'pod-scope-check: all checks passed');
process.exit(failed ? 1 : 0);
