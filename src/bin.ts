import os from 'node:os';
import path from 'node:path';
import { coreUp, coreDown, coreStatus, healthz, readToken } from './core/up.js';

// multi-flock: `flock -p <name> <cmd>...` (or FLOCK_PROFILE env) runs against
// an isolated core instance: own home (~/.flock/<name>), port, tmux session.
// ponytail: port = 7461 + stable hash(name) % 499; override with FLOCK_PORT.
{
  const argv = process.argv.slice(2);
  let profile = process.env.FLOCK_PROFILE ?? '';
  if (argv[0] === '-p' || argv[0] === '--profile') {
    profile = argv[1] ?? '';
    process.argv.splice(2, 2);
  }
  if (profile) {
    process.env.FLOCK_HOME = path.join(os.homedir(), '.flock', profile);
    if (!process.env.FLOCK_PORT) {
      process.env.FLOCK_PORT = String(7461 + ([...profile].reduce((a, c) => a + c.charCodeAt(0), 0) % 499));
    }
  }
}

const USAGE = `flock — core CLI
  [global: -p <profile> — отдельный core-инстанс (multi-flock)]

  flock core up | down | status
  flock healthz
  flock pod spawn <role> [--dir d] [--agent <id>] [--model M] [--cmd c]
      agent id: встроенные (pi, bash) или <FLOCK_HOME>/agents/<id>.json (manifest)
  flock pod status [role]
  flock pod send <role> <text...>
  flock pod capture <role> [--lines N]
  flock pod close <role>
  flock watchdog add --policy <marker|timer|stall|file> --target <role> [opts]
      marker: --text T [--lines N] [--repeat]
      timer:  --after N
      stall:  --idle N [--lines N]
      file:   --path P [--wait-for exists|absent] [--repeat]
      common: [--interval N] [--wake-interval N] [--timeout N]
  flock watchdog ls
  flock watchdog history <id>
  flock watchdog cancel <id>
  flock task add <role> <title...> [--body TEXT]
  flock task ls [status]
  flock task history <id>
  flock task done <id> [result...]
  flock task blocked <id> [reason...]
  flock task needs <id> [reason...]
  flock task cancel <id>
  flock workflow define <name> --steps "id1:role1,id2:role2"
  flock workflow start <name> [payload...]
  flock workflow ls
  flock workflow status <instance_id>
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
          agent: flag(flags, '--agent'),
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
        } else if (policy === 'file') {
          const p = flag(flags, '--path');
          if (!p) { console.error('file: --path required'); process.exit(1); }
          specObj.path = p;
          const wf = flag(flags, '--wait-for');
          if (wf) specObj.waitFor = wf;
        } else {
          console.error(`unknown policy: ${policy} (want marker | timer | stall | file)`);
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
          // pod-registered job? attribute it to the pod, not to 'cli'
          ...(process.env.FLOCK_POD_ROLE ? { registeredBy: process.env.FLOCK_POD_ROLE } : {}),
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

    case 'task': {
      const action = sub;
      const args = rest;
      if (action === 'add') {
        const role = args[0];
        const rest2 = args.slice(1);
        const bi = rest2.indexOf('--body');
        let body: string | undefined;
        let titleArgs = rest2;
        if (bi >= 0) {
          body = rest2[bi + 1];
          titleArgs = [...rest2.slice(0, bi), ...rest2.slice(bi + 2)];
        }
        const title = titleArgs.join(' ').trim();
        if (!role || !title) {
          console.error('usage: flock task add <role> <title...> [--body TEXT]');
          process.exit(1);
        }
        print(await api('POST', '/api/ops', { type: 'task_add', role, title, body }));
      } else if (action === 'ls') {
        print(await api('GET', `/api/tasks${args[0] ? `?status=${encodeURIComponent(args[0])}` : ''}`));
      } else if (action === 'history') {
        print(await api('POST', '/api/ops', { type: 'task_history', id: args[0] }));
      } else if (action === 'done' || action === 'blocked' || action === 'needs' || action === 'cancel') {
        const op =
          action === 'done' ? { type: 'task_done', id: args[0], result: args.slice(1).join(' ') } :
          action === 'blocked' ? { type: 'task_blocked', id: args[0], reason: args.slice(1).join(' ') } :
          action === 'needs' ? { type: 'task_needs', id: args[0], reason: args.slice(1).join(' ') } :
          { type: 'task_cancel', id: args[0] };
        // from inside a pod window attribute the report to the pod
        print(await api('POST', '/api/ops', { ...op, ...(process.env.FLOCK_POD_ROLE ? { registeredBy: process.env.FLOCK_POD_ROLE } : {}) }));
      } else {
        console.log(USAGE);
      }
      return;
    }

    case 'workflow': {
      const action = sub;
      const args = rest;
      if (action === 'define') {
        const name = args[0];
        const stepsRaw = flag(args, '--steps');
        if (!name || !stepsRaw) {
          console.error('usage: flock workflow define <name> --steps "id1:role1,id2:role2"');
          process.exit(1);
        }
        const steps = stepsRaw.split(',').map((s) => {
          const [id, role, ...t] = s.trim().split(':');
          return t.length ? { id, role, title: t.join(':') } : { id, role };
        });
        print(await api('POST', '/api/ops', { type: 'workflow_define', name, steps }));
      } else if (action === 'start') {
        print(await api('POST', '/api/ops', { type: 'workflow_start', name: args[0], payload: args.slice(1).join(' ') || undefined }));
      } else if (action === 'ls') {
        print(await api('POST', '/api/ops', { type: 'workflow_ls' }));
      } else if (action === 'status') {
        print(await api('POST', '/api/ops', { type: 'workflow_status', id: args[0] }));
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
