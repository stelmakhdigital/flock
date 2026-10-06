import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
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

  flock core up | down | restart | status
  flock healthz
  flock board [--token] [--open]              # URL web-board + hint на токен; --open — браузер (honest fallback без GUI)
  flock pod spawn <role> [--dir d] [--agent <id>] [--model M] [--fork <role|file>] [--posture floor|full_bypass] [--cmd c]
      plain-dir: под получает рабочую директорию; git за агентами (core git не видит)
      agent id: встроенные (pi, bash) или <FLOCK_HOME>/agents/<id>.json (manifest)
      pi-под: runner-мост (RPC), своя изоляция конфига, сессия = role (память при relaunch)
  flock pod relaunch <role> [--model M] [--fork [role]] [--profile P] [--fresh]   # --fork (без аргумента) = форк своей сессии; --fresh = явный чистый старт (C6)
  flock pod spawn <role> [--agent X] [--profile P]   # профиль манифеста (override-set)
  flock pod discover                                 # live tmux-панели вне core (кандидаты на adopt)
  flock pod adopt <role> <pane> [--dir d]            # прицепить живую сессию к core без перезапуска
  flock pod resume-token <role> <file|reset>   # зафиксировать сессию для resume (иначе — последняя)
  flock pod status [role]
  flock pod send <role> <text...>
  flock pod capture <role> [--lines N]
  flock pod close <role>
  flock team up [pods.yaml]              # team-реконсиляция: spawn недостающих, refresh живых
  flock topology ls | up <name> [--dir d] # именованные пресеты (conveyor, adversarial-review, ...)
  flock pack ls | pack show <name>       # context packs (filesystem: ~/.flock/packs/)
  flock workspace show                   # декларация workspace (workspace.json, per-profile)
  flock plugins ls | plugins show <src>  # pi-расширения хоста (read-only)
  flock pm up                          # поднять pm-под (goal loop)
  flock pm down                        # остановить goal loop (close pm-пода)
  flock pm state                       # снимок pipeline для pm
  flock pm intent '<json>'             # typed intent: {"op":"task_done","id":"t_x"} или {"intents":[...]}
  flock health ls                      # built-in health-алерты (gate/idle)
  flock ops ls                         # реестр ops (introspection: group, scopes)
  flock agents ls                      # манифесты: id, runtime, source, profiles, thinking
  flock agents show <id> [--profile P] # resolved manifest (imports+profile, как при spawn)
  flock agents new <id> [--from <base>]  # каркас ~/.flock/agents/<id>.json (не перезаписывает)
  flock watchdog add --policy <marker|timer|stall|file> --target <role> [opts]
      marker: --text T [--lines N] [--repeat]
      timer:  --after N
      stall:  --idle N [--lines N]
      file:   --path P [--wait-for exists|absent] [--repeat]
      common: [--interval N] [--wake-interval N] [--timeout N]
  flock watchdog ls
  flock watchdog history <id>
  flock watchdog cancel <id>
  flock task add <role> <title...> [--body TEXT] [--campaign ID]
  flock task ls [status]
  flock task history <id>
  flock task done <id> <reason: finished|blocked|denied|canceled|escalated>
  flock task blocked <id> [reason...]
  flock task needs <id> [reason...]
  flock task handoff <id> <to-role>
  flock task gate <id> <checker-role>       # owner→checker review gate (task не закрывается до вердикта)
  flock task verdict <id> <pass|reject> [reason...]   # review-gate-вердикт: pass: таск → done; reject: → queued на переделку
  flock task cancel <id>
  flock events tail [--since N]        # event-лог (SSE, live)
  flock message send <role> <text...>   # durable-сообщение в inbox (+ poke живому)
  flock message ls [role] [--unclaimed] [--all]
  flock message claim <id>
  flock message broadcast [--to r1,r2] <text...>   # без --to — всем подам (общий «chatroom»-канал; durable + poke живым)
  flock mcp serve                         # stdio JSON-RPC (MCP): tools = OP_REGISTRY (C9)
  flock task unblock <id>               # blocked → queued (arbiter возьмёт заново)
  flock workflow define <name> --steps "id1:role1,id2:role2"
  flock workflow rm <name>
  flock workflow define <name> --steps-json '[{"id":"dev","role":"dev"},{"id":"rev","role":"rev","deps":["dev"]}]'
  flock workflow start <name> [payload...]
  flock workflow ls
  flock workflow status <instance_id>
  flock task add <role> "title" [--body ...] [--campaign ID]
  flock terminal check`;

const [, , cmd, sub, ...rest] = process.argv;

const port = () => Number(process.env.FLOCK_PORT ?? 7460);

// Pod CLI shim (inside the workspace): the daemon also listens on a per-pod
// unix socket (<pod dir>/core.sock) — visible inside the pi sandbox, needs no
// network (untrusted level blocks TCP, a unix socket is a file, not a route).
const POD_SOCKET = process.env.FLOCK_SOCKET;

function unixRequest(socketPath: string, method: string, path: string, token: string, payload?: string): Promise<Response> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        socketPath,
        path,
        method,
        headers: {
          authorization: `Bearer ${token ?? ''}`,
          'content-type': 'application/json',
          ...(payload ? { 'content-length': Buffer.byteLength(payload) } : {}),
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () =>
          resolve(
            new Response(Buffer.concat(chunks), {
              status: res.statusCode ?? 0,
              headers: { 'content-type': 'application/json' },
            }),
          ),
        );
      },
    );
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

const agentsDirOf = () => path.join(process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock'), 'agents');

async function api(method: 'GET' | 'POST', path: string, body?: unknown): Promise<any> {
  const token = process.env.FLOCK_TOKEN ?? readToken();
  const res = POD_SOCKET
    ? await unixRequest(POD_SOCKET, method, path, token ?? '', body === undefined ? undefined : JSON.stringify(body))
    : await fetch(`http://127.0.0.1:${port()}${path}`, {
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
      else if (sub === 'restart') {
        await coreDown();
        console.log(await coreUp());
      }
      else if (sub === 'status') console.log(await coreStatus());
      else console.log(USAGE);
      return;

    case 'mcp': {
      // C9: stdio JSON-RPC MCP server — tools = OP_REGISTRY, zero-dep
      if (sub === 'serve') {
        const { mcpServe } = await import('./core/mcp.js');
        await mcpServe();
      } else {
        console.log('usage: flock mcp serve   # stdio JSON-RPC (MCP), tools = OP_REGISTRY');
      }
      return;
    }
    case 'healthz': {
      const h = await healthz();
      print(h.body);
      return;
    }

    case 'board': {
      const url = `http://127.0.0.1:${process.env.FLOCK_PORT ?? 7460}/board`;
      console.log(url);
      // [U7] hint на токен: login-форма на странице либо flock board --token
      const boardHome = process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock');
      console.log(`login: вставь токен в форму на странице или: flock board --token (файл токена: ${boardHome}/token)`);
      // флаги могут быть в sub (flock board --token) и/или в rest
      const flags = [sub, ...rest];
      if (flags.includes('--token')) {
        const token = readToken();
        console.log(`token: ${token ?? '(нет токена — core не запущен?)'}`);
      }
      // [U7] --open: браузер, если opener доступен; без GUI — honest fallback
      // (заметка, НЕ падение; URL уже напечатан)
      if (flags.includes('--open')) {
        const headless = process.platform === 'linux' && !process.env.DISPLAY && !process.env.WAYLAND_DISPLAY;
        const opener = process.platform === 'darwin' ? 'open' : 'xdg-open';
        const probe = headless ? null : spawnSync(opener, ['--help'], { stdio: 'ignore' });
        if (headless || probe?.error) {
          console.log(`GUI не найден (headless или нет ${opener}) — открой URL вручную`);
        } else {
          const child = spawn(opener, [url], { stdio: 'ignore', detached: true });
          child.on('error', () => console.log(`${opener} не запустился — открой URL вручную`));
          child.unref();
        }
      }
      return;
    }

    case 'pod': {
      const action = sub;
      const role = rest[0];
      if (action === 'discover') {
        // local read: list live panes outside the core session (adopt candidates)
        const { listAllPanes, TMUX_SESSION } = await import('./core/terminal.js');
        const panes = (await listAllPanes()).filter((p) => p.session !== TMUX_SESSION);
        if (!panes.length) {
          console.log(`(no panes outside the core session ${TMUX_SESSION})`);
          return;
        }
        for (const p of panes) {
          console.log(`${p.paneId}  ${p.target}  ${p.cmd}  pid=${p.pid ?? '-'}  ${p.cwd}`);
        }
        console.log(`adopt: flock pod adopt <role> <pane-id> [--dir d]`);
        return;
      }
      if (action === 'adopt') {
        const flags = rest.slice(2);
        if (!role || !rest[1]) {
          console.error('usage: flock pod adopt <role> <pane> [--dir d]   (pane from: flock pod discover)');
          return;
        }
        print(await api('POST', '/api/ops', { type: 'pod_adopt', role, pane: rest[1], dir: flag(flags, '--dir') }));
        return;
      }
      if (action === 'spawn') {
        const flags = rest.slice(1);
        print(await api('POST', '/api/ops', {
          type: 'pod_spawn',
          role,
          dir: flag(flags, '--dir'),
          agent: flag(flags, '--agent'),
          model: flag(flags, '--model'),
          cmd: flag(flags, '--cmd'),
          fork: flag(flags, '--fork'),
          posture: flag(flags, '--posture'),
          profile: flag(flags, '--profile'),
          image: flag(flags, '--image'),
        }));
      } else if (action === 'relaunch') {
        const flags = rest.slice(1);
        const fresh = flags.includes('--fresh');
        print(await api('POST', '/api/ops', {
          type: 'pod_relaunch',
          role,
          model: flag(flags, '--model'),
          profile: flag(flags, '--profile'),
          fork: fresh ? undefined : (flag(flags, '--fork') ?? role),
          fresh: fresh || undefined,
          image: flag(flags, '--image'),
        }));
      } else if (action === 'resume-token') {
        print(await api('POST', '/api/ops', { type: 'pod_set_resume_token', role, token: rest[1] }));
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
        const flags = rest.slice(1);
        print(await api('POST', '/api/ops', { type: 'pod_close', role }));
      } else {
        console.log(USAGE);
      }
      return;
    }

    case 'team': {
      if (sub === 'up') {
        // named team (no /) or a file path (legacy) — the op resolves both.
        // --restore <snap|latest>: restore mode (resume from a snapshot,
        // per-node honest outcomes); without it — plain reconcile.
        const name = rest[0];
        const ri = rest.indexOf('--restore');
        const ref = ri >= 0 ? rest[ri + 1] : undefined;
        if (!name) {
          console.error('usage: flock team up <name|file> [--restore <snap|latest>]');
          return;
        }
        if (ref) {
          print(await api('POST', '/api/ops', { type: 'team_restore', name, restore: ref }));
        } else {
          print(await api('POST', '/api/ops', { type: 'team_up', file: name }));
        }
      } else if (sub === 'down') {
        const name = rest[0];
        if (!name) {
          console.error('usage: flock team down <name>');
          return;
        }
        print(await api('POST', '/api/ops', { type: 'team_down', name }));
      } else if (sub === 'ls') {
        // local read: teams + their latest snapshots (no core round-trip)
        const { listTeams, listSnapshots } = await import('./core/team-snap.js');
        const home = process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock');
        const teams = listTeams(home);
        if (!teams.length) {
          console.log('(no teams — create ~/.flock/teams/<name>.yaml)');
          return;
        }
        for (const t of teams) {
          const snaps = listSnapshots(home, t.name);
          const last = snaps.length ? snaps[snaps.length - 1] : null;
          console.log(`${t.name}  pods: ${t.pods.join(', ') || '(empty)'}  last snapshot: ${last ? `${last.id} (${last.savedAt})` : '—'}`);
        }
      } else {
        console.log('usage: flock team up <name|file> [--restore <snap|latest>] | down <name> | ls');
      }
      return;
    }

    // Topology catalog: named declarative presets over the team path
    case 'topology': {
      if (sub === 'ls') {
        const { listTopologies } = await import('./core/topologies.js');
        for (const t of listTopologies()) {
          console.log(`${t.name}  pods: ${t.pods.join(', ')}  — ${t.summary}`);
        }
      } else if (sub === 'up') {
        const name = rest[0];
        const di = rest.indexOf('--dir');
        const dir = di >= 0 ? rest[di + 1] : undefined;
        if (!name) {
          console.error('usage: flock topology up <name> [--dir d]');
          console.error('available: ' + (await import('./core/topologies.js')).listTopologies().map((t) => t.name).join(', '));
          return;
        }
        print(await api('POST', '/api/ops', { type: 'topology_up', name, dir }));
      } else {
        console.log('usage: flock topology ls | up <name> [--dir d]');
      }
      return;
    }

    // C15: fleet — cross-profile coordination (profiles = other cores)
    case 'fleet': {
      if (sub === 'add') {
        const name = rest[0];
        const url = rest[1];
        const ti = rest.indexOf('--token');
        const token = ti >= 0 ? rest[ti + 1] : undefined;
        if (!name || !url) {
          console.error('usage: flock fleet add <name> <url> [--token <operator-token>]');
          return;
        }
        print(await api('POST', '/api/ops', { type: 'fleet_add', name, url, token }));
      } else if (sub === 'ls') {
        print(await api('POST', '/api/ops', { type: 'fleet_ls' }));
      } else if (sub === 'rm') {
        const name = rest[0];
        if (!name) {
          console.error('usage: flock fleet rm <name>');
          return;
        }
        print(await api('POST', '/api/ops', { type: 'fleet_rm', name }));
      } else {
        console.log('usage: flock fleet add <name> <url> [--token] | ls | rm <name>');
      }
      return;
    }

    // 5.6: campaigns — named persistent goals
    case 'campaign': {
      if (sub === 'new') {
        // goal = everything except the flags (join the words back)
        const pi = rest.indexOf('--pod');
        const goalParts = rest.filter((r, i) => !r.startsWith('--') && (pi < 0 || i !== pi + 1));
        const goal = goalParts.join(' ').trim();
        const pod = pi >= 0 ? rest[pi + 1] : undefined;
        if (!goal) {
          console.error('usage: flock campaign new <goal> [--pod <role>]');
          return;
        }
        print(await api('POST', '/api/ops', { type: 'campaign_new', goal, pod }));
      } else if (sub === 'ls') {
        print(await api('POST', '/api/ops', { type: 'campaign_ls' }));
      } else if (sub === 'status') {
        const id = rest.find((r) => !r.startsWith('-'));
        if (!id) {
          console.error('usage: flock campaign status <id>');
          return;
        }
        print(await api('POST', '/api/ops', { type: 'campaign_status', id }));
      } else if (sub === 'pause' || sub === 'resume' || sub === 'cancel') {
        const id = rest.find((r) => !r.startsWith('-'));
        if (!id) {
          console.error(`usage: flock campaign ${sub} <id>`);
          return;
        }
        print(await api('POST', '/api/ops', { type: `campaign_${sub}`, id }));
      } else {
        console.log('usage: flock campaign new <goal> [--pod] | ls | status <id> | pause|resume|cancel <id>');
      }
      return;
    }

    // C12: content layer — packs / workspace / plugins (read-only surface)
    case 'pack': {
      if (sub === 'ls') {
        print(await api('POST', '/api/ops', { type: 'pack_ls' }));
      } else if (sub === 'show') {
        const name = rest.find((r) => !r.startsWith('-'));
        if (!name) {
          console.error('usage: flock pack show <name>');
          return;
        }
        print(await api('POST', '/api/ops', { type: 'pack_show', name }));
      } else {
        console.log('usage: flock pack ls | flock pack show <name>');
      }
      return;
    }

    case 'workspace': {
      if (sub === 'show') {
        print(await api('POST', '/api/ops', { type: 'workspace_show' }));
      } else {
        console.log('usage: flock workspace show');
      }
      return;
    }

    case 'plugins': {
      if (sub === 'ls') {
        print(await api('POST', '/api/ops', { type: 'plugins_ls' }));
      } else if (sub === 'show') {
        const source = rest.find((r) => !r.startsWith('-'));
        if (!source) {
          console.error('usage: flock plugins show <source>');
          return;
        }
        print(await api('POST', '/api/ops', { type: 'plugins_show', source }));
      } else {
        console.log('usage: flock plugins ls | flock plugins show <source>');
      }
      return;
    }

    case 'esc': {
      // 5.4c durable escalation ladder: audit of "why did it hang"
      if (sub === 'ls') {
        const activeOnly = rest.includes('--all') ? false : true;
        print(await api('POST', '/api/ops', { type: 'esc_ls', active: activeOnly }));
      } else if (sub === 'ack') {
        const id = rest.find((r) => !r.startsWith('-'));
        if (!id) {
          console.log('usage: flock esc ack <id>');
          return;
        }
        print(await api('POST', '/api/ops', { type: 'esc_ack', id }));
      } else {
        console.log('usage: flock esc ls [--all] | flock esc ack <id>');
      }
      return;
    }

    case 'events': {
      // C5: the append-only event log — the memory of the system
      if (sub === 'tail') {
        const sinceArg = rest.find((r) => !r.startsWith('-'));
        const since = sinceArg ? Number(sinceArg) || 0 : 0;
        const token = process.env.FLOCK_TOKEN ?? readToken();
        const res = await fetch(`http://127.0.0.1:${port()}/events?since=${since}`, {
          headers: { authorization: `Bearer ${token ?? ''}` },
        }).catch((e) => {
          console.error(e instanceof Error ? e.message : String(e));
          process.exit(1);
        });
        if (!res.ok || !res.body) {
          console.error(`events tail: HTTP ${res.status}`);
          process.exit(1);
        }
        const reader = res.body.getReader();
        const dec = new TextDecoder();
        let buf = '';
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          let i: number;
          while ((i = buf.indexOf('\n\n')) !== -1) {
            const frame = buf.slice(0, i);
            buf = buf.slice(i + 2);
            const id = frame.match(/^id:\s*(.+)$/m)?.[1];
            const ev = frame.match(/^event:\s*(.+)$/m)?.[1] ?? 'event';
            const data = frame.match(/^data:\s*(.+)$/m)?.[1];
            if (data) console.log(`${id ?? ''}\t${ev}\t${data}`);
          }
        }
      } else {
        console.log('usage: flock events tail [--since N]');
      }
      return;
    }

    case 'message': {
      // C4: inboxes + outboxes — durable pod↔pod/operator messages
      if (sub === 'send') {
        const role = rest[0];
        const text = rest.slice(1).join(' ');
        if (!role || !text) {
          console.error('usage: flock message send <role> <text...>');
          process.exit(1);
        }
        print(await api('POST', '/api/ops', { type: 'message_send', to: role, text }));
      } else if (sub === 'ls') {
        const role = rest.find((r) => !r.startsWith('-'));
        print(await api('POST', '/api/ops', {
          type: 'message_list',
          to: role,
          unclaimed: rest.includes('--unclaimed'),
          limit: rest.includes('--all') ? 500 : 50,
        }));
      } else if (sub === 'claim') {
        const id = rest[0];
        if (!id) {
          console.error('usage: flock message claim <id>');
          process.exit(1);
        }
        print(await api('POST', '/api/ops', { type: 'message_claim', id: Number(id) }));
      } else if (sub === 'broadcast') {
        // flock message broadcast [--to r1,r2] <text...> — без --to: все поды
        const ti = rest.indexOf('--to');
        const excl = ti >= 0 ? new Set([ti, ti + 1]) : new Set<number>();
        const roles = ti >= 0 ? (rest[ti + 1] ?? '').split(',').map((s) => s.trim()).filter(Boolean) : undefined;
        const text = rest.filter((_, i) => !excl.has(i)).join(' ').trim();
        if (!text) {
          console.error('usage: flock message broadcast [--to r1,r2] <text...>   (без --to — всем подам; общий «chatroom»-канал)');
          process.exit(1);
        }
        print(await api('POST', '/api/ops', { type: 'message_broadcast', text, roles }));
      } else {
        console.log('usage: flock message send <role> <text...> | ls [role] [--unclaimed] [--all] | claim <id> | broadcast [--to r1,r2] <text...>');
      }
      return;
    }


    case 'pm': {
      if (sub === 'up') {
        print(await api('POST', '/api/ops', { type: 'pm_up' }));
      } else if (sub === 'down') {
        print(await api('POST', '/api/ops', { type: 'pod_close', role: 'pm' }));
      } else if (sub === 'state') {
        print(await api('POST', '/api/ops', { type: 'pm_state' }));
      } else if (sub === 'intent') {
        const json = rest.join(' ');
        let intents: unknown;
        try {
          intents = JSON.parse(json);
        } catch {
          console.error('usage: flock pm intent \'<json>\'  (опт или {"intents":[...]})');
          process.exit(1);
        }
        const body = Array.isArray(intents) ? { type: 'pm_intents', intents } : { type: 'pm_intents', intents: [intents] };
        print(await api('POST', '/api/ops', body));
      } else {
        console.log('usage: flock pm up | down | state | intent \'<json>\'');
      }
      return;
    }

    case 'health': {
      if (sub === 'ls') {
        print(await api('POST', '/api/ops', { type: 'health_list' }));
      } else {
        console.log('usage: flock health ls');
      }
      return;
    }

    case 'ops': {
      if (sub === 'ls') {
        print(await api('GET', '/api/ops'));
      } else {
        console.log('usage: flock ops ls');
      }
      return;
    }

    case 'agents': {
      // T2: manifests are local config — no core round-trip needed
      // (loadAgents/resolveAgent run in-process; core may be down)
      const { loadAgents, manifestRuntime, resolveAgent, THINKING_LEVELS } = await import('./core/agent.js');
      const { writeFileSync, existsSync, mkdirSync } = await import('node:fs');
      const agents = loadAgents();
      if (!sub || sub === 'ls') {
        const builtin = new Set(['pi', 'bash', 'pm']);
        const rows = Object.values(agents).map((m) => ({
          id: m.id,
          runtime: manifestRuntime(m),
          source: builtin.has(m.id) && !existsSync(path.join(agentsDirOf(), `${m.id}.json`)) ? 'builtin' : 'user',
          profiles: Object.keys(m.profiles ?? {}).join(',') || '-',
          thinking: m.thinking ?? '-',
        }));
        const w = (k: keyof (typeof rows)[0]) => Math.max(...rows.map((r) => String(r[k]).length), k.length);
        console.log(`${'id'.padEnd(w('id'))}  ${'runtime'.padEnd(w('runtime'))}  ${'source'.padEnd(w('source'))}  ${'profiles'.padEnd(w('profiles'))}  ${'thinking'.padEnd(w('thinking'))}`);
        for (const r of rows) {
          console.log(`${String(r.id).padEnd(w('id'))}  ${String(r.runtime).padEnd(w('runtime'))}  ${String(r.source).padEnd(w('source'))}  ${String(r.profiles).padEnd(w('profiles'))}  ${String(r.thinking)}`);
        }
        return;
      }
      if (sub === 'show') {
        const id = rest.find((r) => !r.startsWith('-'));
        if (!id) {
          console.log('usage: flock agents show <id> [--profile P]');
          return;
        }
        const profileIdx = rest.indexOf('--profile');
        const profile = profileIdx >= 0 ? rest[profileIdx + 1] : undefined;
        try {
          const r = resolveAgent(id, null, profile);
          if (!r) throw new Error(`unknown agent: ${id} (have: ${Object.keys(agents).join(', ')})`);
          console.log(JSON.stringify(r.manifest, null, 2));
        } catch (e) {
          // resolve errors are the config check: print them as-is
          console.error(e instanceof Error ? e.message : String(e));
          process.exitCode = 1;
        }
        return;
      }
      if (sub === 'new') {
        const id = rest.find((r) => !r.startsWith('-'));
        if (!id || !/^[a-z0-9][a-z0-9-]{0,30}$/.test(id)) {
          console.log('usage: flock agents new <id> [--from <base>]');
          return;
        }
        const fromIdx = rest.indexOf('--from');
        const from = fromIdx >= 0 ? rest[fromIdx + 1] : 'pi';
        if (from && !agents[from]) {
          console.error(`unknown base manifest: ${from} (have: ${Object.keys(agents).join(', ')})`);
          process.exitCode = 1;
          return;
        }
        const dir = agentsDirOf();
        const file = path.join(dir, `${id}.json`);
        if (existsSync(file)) {
          console.error(`already exists: ${file} (not overwriting)`);
          process.exitCode = 1;
          return;
        }
        mkdirSync(dir, { recursive: true });
        writeFileSync(file, JSON.stringify({ id, imports: [from], profiles: {} }, null, 2) + '\n');
        console.log(`created ${file}`);
        console.log(`override axes: thinking (${THINKING_LEVELS.join('/')}), tools, excludeTools, skills, noSkills, extensions, noExtensions, mcp, systemPrompt, appendSystemPrompt, noContextFiles; plus model (spawn --model), guidance, merge, testCmd`);
        console.log('named override sets live under "profiles": {}; pick one with `flock pod spawn <role> --agent ' + id + ' --profile P`');
        return;
      }
      if (sub === 'image') {
        // C13: agent images — a snapshot of a productive agent's resumable
        // state (manifest + session copy). The ops round-trip keeps the
        // single-mutation-path rule (the image store is under FLOCK_HOME).
        const { apply } = await import('./core/ops.js');
        const { openStore } = await import('./core/store.js');
        const st = openStore(process.env.FLOCK_HOME ?? path.join(os.homedir(), '.flock'));
        const ctx = { store: st, ticks: null as never, startedAt: new Date().toISOString() } as never;
        const c = rest;
        const act = c[0];
        const err = (m: string) => { console.error(m); process.exitCode = 1; };
        try {
          if (act === 'save') {
            // flock agent image save <role> --as <name>
            const role = c.find((r) => !r.startsWith('-') && r !== 'save');
            const asIdx = c.indexOf('--as');
            const as = asIdx >= 0 ? c[asIdx + 1] : undefined;
            if (!role || !as) { err('usage: flock agent image save <role> --as <name>'); return; }
            const r = await apply({ type: 'agent_image_save', role, as }, ctx);
            console.log(JSON.stringify(r, null, 2));
          } else if (act === 'ls') {
            const r = await apply({ type: 'agent_image_ls' }, ctx);
            const rows = r as { name: string; agent: string; source: string; savedAt: string }[];
            if (!rows.length) { console.log('(no images)'); return; }
            const w = (k: keyof (typeof rows)[0]) => Math.max(...rows.map((x) => String(x[k]).length), k.length);
            for (const row of rows) console.log(`${row.name.padEnd(w('name'))}  ${row.agent.padEnd(w('agent'))}  ${row.source.padEnd(w('source'))}  ${row.savedAt}`);
          } else if (act === 'rm') {
            const name = c.find((r) => !r.startsWith('-') && r !== 'rm');
            const force = c.includes('--force');
            if (!name) { err('usage: flock agent image rm <name> [--force]'); return; }
            const r = await apply({ type: 'agent_image_rm', name, force }, ctx);
            console.log(JSON.stringify(r, null, 2));
          } else {
            err('usage: flock agent image save <role> --as <name> | ls | rm <name> [--force]');
          }
        } catch (e) {
          err(e instanceof Error ? e.message : String(e));
        }
        return;
      }
      console.log('usage: flock agents ls | show <id> [--profile P] | new <id> [--from <base>] | image save|ls|rm');
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
        const ci = rest2.indexOf('--campaign');
        let body: string | undefined;
        let campaignId: string | undefined;
        let titleArgs = rest2;
        if (bi >= 0) {
          body = rest2[bi + 1];
          titleArgs = [...rest2.slice(0, bi), ...rest2.slice(bi + 2)];
        }
        if (ci >= 0) {
          campaignId = rest2[ci + 1];
          titleArgs = [...titleArgs.slice(0, ci), ...titleArgs.slice(ci + 2)];
        }
        const title = titleArgs.join(' ').trim();
        if (!role || !title) {
          console.error('usage: flock task add <role> <title...> [--body TEXT] [--campaign ID]');
          process.exit(1);
        }
        print(await api('POST', '/api/ops', { type: 'task_add', role, title, body, ...(campaignId ? { campaign_id: campaignId } : {}) }));
      } else if (action === 'ls') {
        print(await api('GET', `/api/tasks${args[0] ? `?status=${encodeURIComponent(args[0])}` : ''}`));
      } else if (action === 'history') {
        print(await api('POST', '/api/ops', { type: 'task_history', id: args[0] }));
      } else if (action === 'unblock') {
        print(await api('POST', '/api/ops', { type: 'task_unblock', id: args[0] }));
      } else if (action === 'done' || action === 'blocked' || action === 'needs' || action === 'cancel' || action === 'handoff') {
        const op =
          action === 'done' ? { type: 'task_done', id: args[0], reason: args.slice(1).join(' ') } :
          action === 'blocked' ? { type: 'task_blocked', id: args[0], reason: args.slice(1).join(' ') } :
          action === 'needs' ? { type: 'task_needs', id: args[0], reason: args.slice(1).join(' ') } :
          action === 'handoff' ? { type: 'task_handoff', id: args[0], to: args[1] } :
          { type: 'task_cancel', id: args[0] };
        // from inside a pod window attribute the report to the pod
        print(await api('POST', '/api/ops', { ...op, ...(process.env.FLOCK_POD_ROLE ? { registeredBy: process.env.FLOCK_POD_ROLE } : {}) }));
      } else if (action === 'gate') {
        if (!args[0] || !args[1]) {
          console.error('usage: flock task gate <id> <checker-role>');
          return;
        }
        print(await api('POST', '/api/ops', { type: 'task_gate', id: args[0], checker: args[1] }));
      } else if (action === 'verdict') {
        if (!args[0] || !args[1]) {
          console.error('usage: flock task verdict <id> <pass|reject> [reason...]');
          return;
        }
        print(await api('POST', '/api/ops', { type: 'task_verdict', id: args[0], verdict: args[1], reason: args.slice(2).join(' ') || undefined }));
      } else {
        console.log(USAGE);
      }
      return;
    }

    case 'workflow': {
      const action = sub;
      const args = rest;
      if (action === 'rm') {
        print(await api('POST', '/api/ops', { type: 'workflow_rm', name: args[0] }));
        return;
      }
      if (action === 'define') {
        const name = args[0];
        const stepsRaw = flag(args, '--steps');
        const stepsJson = flag(args, '--steps-json');
        if (!name || (!stepsRaw && !stepsJson)) {
          console.error('usage: flock workflow define <name> --steps "id1:role1,id2:role2" | --steps-json \'[{id, role, ...}]\'');
          process.exit(1);
        }
        const steps = stepsJson
          ? JSON.parse(stepsJson)
          : stepsRaw!.split(',').map((s) => {
              const [id, role, ...t] = s.trim().split(':');
              return t.length ? { id, role, title: t.join(':') } : { id, role };
            });
        print(await api('POST', '/api/ops', {
          type: 'workflow_define',
          name,
          steps,
        }));
      } else if (action === 'start') {
        const payloadArgs = args.slice(1);
        print(
          await api('POST', '/api/ops', {
            type: 'workflow_start',
            name: args[0],
            payload: payloadArgs.join(' ') || undefined,
          }),
        );
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
