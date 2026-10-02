import { coreUp, coreDown, coreStatus, healthz, readToken } from './core/up.js';

const USAGE = `flock — core CLI (stage 0)

  flock core up | down | status
  flock healthz
  flock post spawn <role> [--pod p] [--dir d] [--cmd c]
  flock post status [role]
  flock post send <role> <text...>
  flock post capture <role> [--lines N]
  flock post close <role>
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

    case 'post': {
      const action = sub;
      const role = rest[0];
      if (action === 'spawn') {
        const flags = rest.slice(1);
        print(await api('POST', '/api/ops', {
          type: 'post_spawn',
          role,
          pod: flag(flags, '--pod'),
          dir: flag(flags, '--dir'),
          cmd: flag(flags, '--cmd'),
        }));
      } else if (action === 'status') {
        const data = await api('GET', '/api/posts');
        const posts = rest[1] ? data.posts.filter((p: { role: string }) => p.role === rest[1]) : data.posts;
        print({ posts, runs: data.runs });
      } else if (action === 'send') {
        print(await api('POST', '/api/ops', { type: 'post_send', role, text: rest.slice(1).join(' ') }));
      } else if (action === 'capture') {
        const flags = rest.slice(1);
        print(await api('POST', '/api/ops', { type: 'post_capture', role, lines: numFlag(flags, '--lines', 200) }));
      } else if (action === 'close') {
        print(await api('POST', '/api/ops', { type: 'post_close', role }));
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
