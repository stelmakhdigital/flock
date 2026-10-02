import { coreUp, coreDown, coreStatus, healthz, readToken } from './core/up.js';

const USAGE = `flock — core CLI (stage 0)

  flock core up | down | status
  flock healthz
  flock pod spawn <role> [--dir d] [--cmd c]
  flock pod status [role]
  flock pod send <role> <text...>
  flock pod capture <role> [--lines N]
  flock pod close <role>
  flock watchdog add --policy <marker|timer|stall> --target <role> [opts]
      marker: --text T [--lines N] [--repeat]
      timer:  --after N
      stall:  --idle N [--lines N]
      common: [--interval N] [--wake-interval N] [--timeout N]
  flock watchdog ls
  flock watchdog history <id>
  flock watchdog cancel <id>
  flock terminal check`;

const [, , cmd, sub, ...rest] = process.argv;

const port = () => Number(process.env.FLOCK_PORT ?? 7460);

async function api(method: 'GET' | 'POST', path: string, body?: unknown): Promise<any> {
  const token = readToken();
  const res = await fetch(`http://127.0.0.1:${port()}${path}`, {
    method,
    headers: {
      authorization: `Bearer ${token ?? ''}`,
      'content-type': 'application/json',
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = await res.json().catch(() => null);
  if (!res.ok) {
    if (res.status === 401) {
      console.error('unauthorized — is core up? (flock core up)');
    } else {
      console.error(JSON.stringify(data ?? { error: res.statusText }, null, 2));
    }
    process.exit(1);
  }
  return data;
}

function flag(flags: string[], name: string): string | undefined {
  const i = flags.indexOf(name);
  return i >= 0 ? flags[i + 1] : undefined;
}

function numFlag(flags: string[], name: string, dflt: number): number {
  const v = flag(flags, name);
  return v === undefined ? dflt : Number(v);
}

function print(v: unknown): void {
  console.log(JSON.stringify(v, null, 2));
}

async function main(): Promise<void> {
  switch (cmd) {
    case 'core':
      if (sub === 'up') console.log(await coreUp());
      else if (sub === 'down') console.log(await coreDown());
      else if (sub === 'status') console.log(await coreStatus());
      else console.log(USAGE);
      return;

    case 'healthz': {
      const h = await healthz();
      print(h.body);
      return;
    }

    case 'pod': {
      const action = sub;
      const role = rest[0];
      if (action === 'spawn') {
        const flags = rest.slice(1);
        print(await api('POST', '/api/ops', {
          type: 'pod_spawn',
          role,
          dir: flag(flags, '--dir'),
          cmd: flag(flags, '--cmd'),
        }));
      } else if (action === 'status') {
        const data = await api('GET', '/api/pods');
        const pods = rest[1] ? data.pods.filter((p: { role: string }) => p.role === rest[1]) : data.pods;
        print({ pods, runs: data.runs });
      } else if (action === 'send') {
        print(await api('POST', '/api/ops', { type: 'pod_send', role, text: rest.slice(1).join(' ') }));
      } else if (action === 'capture') {
        const flags = rest.slice(1);
        print(await api('POST', '/api/ops', { type: 'pod_capture', role, lines: numFlag(flags, '--lines', 200) }));
      } else if (action === 'close') {
        print(await api('POST', '/api/ops', { type: 'pod_close', role }));
      } else {
        console.log(USAGE);
      }
      return;
    }

    case 'watchdog': {
      const action = sub;
      const args = rest;
      if (action === 'add') {
        const flags = args;
        const policy = flag(flags, '--policy');
        const target = flag(flags, '--target');
        if (!policy || !target) {
          console.error('usage: flock watchdog add --policy <marker|timer|stall> --target <role> [--text T] [--after N] [--idle N] [--lines N] [--interval N] [--wake-interval N] [--timeout N] [--repeat]');
          process.exit(1);
        }
        const specObj: Record<string, unknown> = {};
        if (policy === 'marker') {
          const text = flag(flags, '--text');
          if (!text) { console.error('marker: --text required'); process.exit(1); }
          specObj.text = text;
          if (flags.includes('--repeat')) specObj.once = false;
        } else if (policy === 'timer') {
          const after = flag(flags, '--after');
          if (!after) { console.error('timer: --after required'); process.exit(1); }
          specObj.afterSeconds = Number(after);
        } else if (policy === 'stall') {
          const idle = flag(flags, '--idle');
          if (!idle) { console.error('stall: --idle required'); process.exit(1); }
          specObj.idleSeconds = Number(idle);
        } else {
          console.error(`unknown policy: ${policy} (want marker | timer | stall)`);
          process.exit(1);
        }
        const lines = flag(flags, '--lines');
        if (lines) specObj.lines = Number(lines);
        const timeout = flag(flags, '--timeout');
        if (timeout) specObj.timeoutSeconds = Number(timeout);
        print(await api('POST', '/api/ops', {
          type: 'watchdog_register',
          policy,
          target,
          spec: specObj,
          intervalSeconds: numFlag(flags, '--interval', 5),
          activeWakeIntervalSeconds: flag(flags, '--wake-interval') ? Number(flag(flags, '--wake-interval')) : null,
        }));
      } else if (action === 'ls') {
        print(await api('GET', '/api/watchdog'));
      } else if (action === 'history') {
        print(await api('GET', `/api/watchdog/${encodeURIComponent(args[0] ?? '')}/history`));
      } else if (action === 'cancel') {
        print(await api('POST', '/api/ops', { type: 'watchdog_cancel', id: args[0] }));
      } else {
        console.log(USAGE);
      }
      return;
    }

    case 'terminal':
      if (sub === 'check') print(await api('POST', '/api/ops', { type: 'terminal_check' }));
      else console.log(USAGE);
      return;

    default:
      console.log(USAGE);
  }
}

main().catch((e) => {
  console.error(e?.message ?? String(e));
  process.exit(1);
});
