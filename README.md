# flock

Множественная агентная автоматизация разработки: **core** (демон —
deterministic control plane, без LLM) + **pod**'ы (агенты в терминалах) +
очередь задач + durable-координация.

Философия (OpenRIG-style): core — координационная плоскость. Он хранит
состояние, доставляет, наблюдает и эскалирует; решения принимает LLM в подах.
**Git за агентами** (core git не видит, поды — plain-dir), **экономики в core
нет** (аудит usage остаётся в `activity.jsonl` — для оператора, не для
решений). Ноль npm-зависимостей сверх `hono` + node builtins.

Архитектура и глоссарий — в `docs/`.

## Быстрый старт

```sh
npm install
npm run build
./bin/flock core up
./bin/flock healthz
./bin/flock pod spawn dev           # tmux-окно с pi (bridge-рантайм)
./bin/flock pod send dev "напиши слово hello"
./bin/flock pod capture dev
./bin/flock task add dev "создай hello.txt"
./bin/flock events tail             # live-лента событий (SSE)
./bin/flock core status
./bin/flock core down
```

Состояние: `~/.flock/` (flock.db, core.pid, core.log, token). Порт =
7460 + hash(профиля), `FLOCK_PORT` переопределяет.

## Единый путь мутаций

Все изменения — через `apply(op)` в core: один writer (node:sqlite), один
`OP_REGISTRY` (34 ops: `flock ops ls`), один аудит (event-лог +
`task_transitions` + `runs.meta`). CLI, тики (arbiter/watchdog/escalation),
pm и MCP `tools/call` — все идут через тот же путь. Под-токены ограничены
`pod`-scoped ops (свои задачи, свой inbox, свой под).

```
оператор (CLI/HTTP/MCP) ─┐
тики core (arbiter и др.) ─┼─► apply(op) ─► sqlite (single writer)
поды (pod-токен, unix-сокет) ─┘        └─► event log + SSE
```

## Координация: hot-potato closure + transactional handoff

Работа «вечно горяч»: задача никогда не висит без ответственного — она
закончена, передана или эскалирована.

- **Closure vocabulary** (терминально): `finished | handed-off | blocked |
  denied | canceled | escalated` (+ `target` для `handed-off`/`escalated`).
  `tasks.status` (`queued|active|done|blocked|needs|cancelled`) — жизненный
  цикл, не меняется; `tasks.closed` (JSON) пишется только терминально.
  `flock task done <id> <reason>` — причина обязательна.
- **Transactional handoff**: `flock task handoff <id> <to-role>` — одна
  транзакция: старая закрывается `handed-off (target)`, successor создаётся у
  получателя. Handoff не теряется. (Кросс-профильный handoff — two-phase,
  фаза 5 — до релиза.)

```sh
./bin/flock task done <id> finished            # или: blocked|denied|canceled|escalated
./bin/flock task handoff <id> rev              # transactional: close + successor
./bin/flock task blocked <id> "что сломалось"   # НЕ терминально: под остаётся ответственным
./bin/flock task needs <id> "что нужно"
./bin/flock task unblock <id>                   # явный акт оператора: blocked → queued
```

`blocked`/`needs` — НЕ терминальные: под всё ещё «держит горячую картошку»,
а stuck-детекция — на watchdog/лестнице эскалаций (автоматических повторов
нет — scribe model ниже).

## Inboxes / outboxes: durable-сообщения

Pod'ы общаются durable-записями, а не «надеждой на экран»: `inboxes` —
mailbox получателя, `outboxes` — sender-side record (local send —
`pending=0`; cross-profile `pending=1` зарезервировано под fleet-фазу).

```sh
./bin/flock message send <role> "текст..."   # durable-строка + best-effort poke живому
./bin/flock message ls [role] [--unclaimed] [--all]
./bin/flock message claim <id>
```

- Доставка = **durable inbox-строка** + poke живому под'у (`pod send`).
  Мёртвый/закрытый под — сообщение остаётся в inbox и дождётся relaunch.
- Pod-токен видит только свой inbox и не пишет сам себе; оператор — любой
  или все unclaimed.
-	pm читает свои inbox-сообщения через `flock pm state`
  (`unclaimedMessages`).

## Event log + SSE

Каждая успешная мутация — одна строка в append-only `events`
(actor: `core` / `pod:<role>`, subject, payload). Сбойные ops событий не
пишут. Это «память системы»: аудит, debug, будущий board.

```sh
./bin/flock events tail [--since N]        # CLI: SSE → stdout
curl -N -H "Authorization: Bearer $(cat ~/.flock/token)" "http://127.0.0.1:7460/events?since=0"
```

WS-фид убран (C5): `GET /events?since=<id>` — SSE (Hono streaming), backlog
из `events` + live-push.

## Workflow: scribe model + DAG

Определение = именованный набор шагов; instance двигает шаги через обычную
очередь. **Scribe (C7)**: runtime закрывает и записывает, НЕ гейтит —
ручек `priority`/`retry`/`timeoutMin` в шагах нет. Failed шаг (blocked/
cancelled) → сcribe записывает и ставит инстанс в blocked/cancelled;
повторов нет — оператор решает (`task unblock` / cancel). Stuck-детекция —
на watchdog.

```json
[{"id":"dev","role":"dev"},{"id":"rev","role":"rev","deps":["dev"]}]
```

- **deps** (DAG): шаги стартуют, когда все deps done. Без deps — стартовый
  фронт. Циклы/self/unknown dep отклоняются на define. Ready-фронт
  закидывается параллельно; тело таска получает результаты зависимых шагов.
- **Frozen step task**: остановленный инстанс не принимает поздние смены
  статуса шага.

```sh
./bin/flock workflow define pipe --steps-json '[...]'
./bin/flock workflow start pipe "фича X"
./bin/flock workflow status <instance_id>   # state + stepState (pending/running/done/blocked)
```

## Strict honest resume (C6)

Relaunch = честный resume ТОЧНОЙ сессии; silent fresh запрещён.

- `flock pod relaunch <role>` — resume (pi: `--session <file>`);
- сессии нет/проба упала → **failed resume fails loudly**: `attention_required`
  в run meta + hint `flock pod relaunch <role> --fresh`;
- `--fresh` — явный чистый старт (операторский выбор, не fallback);
- `--fork [role]` — форк сессии (новый identity, parent не трогается);
- pre-claim пробы: pi (session-файл + runner-sidecar).

## Runtimes: bridge и terminal-native

Запуск — через **RuntimeAdapter** (launch/ready + `liveness` /
`sendVerified` / `healthProbe`). Core не знает, какой рантайм под окном;
новый рантайм = адаптер + manifest, без правок core.

**Bridge-рантаймы** (`bridge-protocol` общий; один JSON-флаг
`--child-args` = raw passthrough + mapped-оси):

- **pi** (`pi-bridge.js`): `pi --mode rpc`, typed sidecar
  (ready/streaming/lastPrompt/exited), flockmsg v2 + nonce-ack, session
  identity = session-файл. Dialog'и расширений без оператора: авто-ответ
  strictest option (deny) + LOUD mirror + `ext_dialog_auto_denied` в
  activity (gate-канал = `attach`: рычаг оператора — pane);

**Terminal-native** (delta маленькая: нет моста, сигналы — с панели):

- **bash**: plain window, без ready-gate и resume.

C12b: claude- и codex-адаптеры убраны (commit C12b, возврат =
cherry-pick) — манифест с удалённым рантаймом получает чистую ошибку
`runtime not supported`. Контракт (RuntimeAdapter + bridge-protocol) —
точка расширения: новый рантайм = один адаптер + manifest, ноль строк в
core.

**Unified `--child-args` (C10)**: manifest-поле `child: {args?, env?}` —
raw passthrough (args — после mapped-осей, env — поверх flock-managed):

```json
{ "id": "cheap-dev", "imports": ["pi"], "thinking": "low",
  "child": { "env": { "MY_VAR": "1" } } }
```

Мерж манифестов: скаляры ext-wins (profile > imports > base), массивы
concat, `mcp` по имени сервера.

```sh
./bin/flock agents ls                      # id, runtime, source, profiles, thinking
./bin/flock agents show <id> [--profile P] # resolved manifest
./bin/flock pod spawn dev --agent pi --profile careful
```

Изоляция (bridge-рантаймы): per-pod конфиг (`PI_CODING_AGENT_DIR`),
`--no-context-files` + `--append-system-prompt <pod>/AGENTS.md`
(home-AGENTS.md в под не попадает), per-pod sandbox-trust из manifest
`trustLevel`.

## pm — обычный под (C8)

pm — не подсистема core, а обычный pod (manifest `pm`), который будится
inbox-сообщениями/poke и решает **обычными pod-ops**. У core нет pm-тика
и pm-sweep; special-case `pmNotify` убран — delivery = `message_send
{to:'pm'}` + poke.

```sh
./bin/flock pm up        # spawn/relaunch pm-пода
./bin/flock pm state     # снимок pipeline: tasks, openTasks(+closed), waitingOnClosed,
                         #   pods(+activity busy/idle/not-ready), liveRuns, unclaimedMessages
./bin/flock pm down
```

`flock pm intent '<json>'` — legacy-алиас (whitelist, тот же `apply`);
pm-протокол: inbox-событие → `flock pm state` → решения твоими ops.
Hang-worthy события (pod_crashed, task_blocked, task_needs) открывают
эскалацию ДО доставки.

## Durable-лестница эскалаций (5.4c)

Hang-worthy триггер первым делом пишется в `escalations` (BDD-строка,
переживает рестарт), а потом доставляется. Tick 30s водит лестницу:

```
open → pm_notified   (pm жив: доставлено + pm-silence-таймер)
open → escalated     (pm не жив: сразу оператору)
pm_notified → escalated (pm молчит > FLOCK_ESC_PM_TIMEOUT_S, default 300s)
escalated → health-алерт (kind=ladder) + re-reminder каждые
             FLOCK_ESC_REMINDER_S (default 3600s) до ack/resolve
```

Авто-закрытие: tick перечитывает исходное условие → `resolved`. Дедуп: один
активный ряд на key.

```sh
./bin/flock esc ls [--all]     # аудит «почему провисло»
./bin/flock esc ack <id>
```

## Watchdog

Декларативные проверки: кто угодно (CLI/агент) регистрирует job, core
оценивает по расписанию (tick 1с) и будит нужный pod.

```sh
./bin/flock watchdog add --policy timer  --after 30 --target dev
./bin/flock watchdog add --policy marker --text "CI:OK" --target dev --repeat
./bin/flock watchdog add --policy stall  --idle 120 --target dev --wake-interval 300
./bin/flock watchdog add --policy file --path ~/build/out.txt --target dev
```

Политики: `marker` (текст в capture), `timer`, `stall` (экран не меняется),
`file` (появился/исчез). Доставка — verified-send (paste → верификация по
capture → ретраи).

## Health (gate/idle)

Built-in health-чеки поверх typed-сигналов адаптера: **gate** (dialog
ждёт человека: у pi — `ext_dialog_auto_denied`, канал `attach`) и **idle**
(агент на паузе с active-задачей: nudge →
`task needs`). Лестница: alert → nudge/realert → эскалация.

```sh
./bin/flock health ls
curl -H "Authorization: Bearer $(cat ~/.flock/token)" http://127.0.0.1:7460/api/health
# → alerts, activeEscalations, opts
```

## MCP: `flock mcp serve` (C9)

Zero-dep stdio JSON-RPC server (MCP): `tools/list` = весь `OP_REGISTRY`
(`inputSchema: {type:"object"}`, description из summary), `tools/call` —
HTTP-запрос в `/api/ops` running core (один writer, token-аутентификация,
реальная валидация — в op-хендлерах).

```sh
./bin/flock mcp serve    # stdio; подключить в MCP-клиент как stdio-сервер
```

## Team up

`flock team up [pods.yaml]` — декларативный состав: reconcile (недостающих
spawn, живым — refresh managed guidance в AGENTS.md), никого не убивает,
идемпотентен.

```yaml
pods:
  dev:
    agent: pi
    model: cat-vllm/qwen3.8-27b-fp8
    guidance: |
      Пиши на русском.
  rev:
    agent: pi
```

Pod'ы — plain-dir: рабочая директория пода — просто каталог; git,
ветки, коммиты — за агентами (свой workflow, свои коммиты). Merge-гейты,
review-гейты, merge-queue, conflict-резолвер и вся экономика — **убраны из
core** (C1/C2).

## Content layer (C12): packs, workspace, plugins

Файл-канонический (OpenRIG-модель): sqlite не трогаем, кэша нет —
ассамбл бандлов — горстка маленьких чтений по требованию.

**Context packs** — `~/.flock/packs/<name>/`: `pack.json` {id, files: […]}
+ файлы. Ассамбл в один paste-ready бандл. Манифест-ось `packs: [names]`
→ managed-блок `pack:<name>` в `<pod>/AGENTS.md` при spawn/relaunch:

```sh
flock pack ls
flock pack show <name>                    # бандл (paste-ready)

# manifest: { "id": "dev", "imports": ["pi"], "packs": ["style"] }
```

**Workspace** — `~/.flock/workspace.json` (per-profile):
`{root, repos: {name: path}, knowledge?}`. Team-файл: `dir: ws:<name>`
резолвится через workspace (или абсолютный путь). Inventory — в
`flock core status`.

```sh
flock workspace show
# pods.yaml: pods: dev: { agent: pi, dir: ws:flock }
```

**Plugins** — read-only инспекция pi-расширений хоста, которые наследуют
поды (`pi list`). Install — нет (операторская копия в ~/.pi/agent):

```sh
flock plugins ls
flock plugins show <source>               # entry + SKILL.md/README (≤4KB)
```

## Multi-flock (profiles)

Несколько изолированных core-инстансов:

```sh
./bin/flock -p team2 core up      # свой home ~/.flock/team2, порт, tmux flock-team2
./bin/flock -p team2 pod spawn dev
```

БД/token/log/sessions — свои, инстансы не пересекаются. (Кросс-профильная
координация — fleet-фаза.)

## Retention

Тик 24h (env-настраивается): runs старше N дней (default 14) →
`runs_archive` (архив, не delete), `activity.jsonl` head-trim, `core.log`
rotate. Usage-аудит: bridge'и пишут `usage`-события в `activity.jsonl`
(аудит оператора, не решений core).

## Pod-scoped авторизация

Один токен core, scope зависит от канала: unix-сокет пода
(`<pod>/core.sock`) видит только `pod`-scoped ops (свои задачи, свой
inbox, свой под); operator-only ops через сокет пода → 403. Узкие места
(own-pod/own-task) проверяются в op.

## Принципы

- **Core без LLM**: тики = watchdog/scheduler; решения — в подах.
- **Single writer**: все мутации через `apply` в один процесс; MCP и
  поды — клиенты.
- **Honest state**: failed resume = failed (не silent fresh); handoff —
  transactional; сообщения — durable; события — append-only.
- **Ноль новых npm-зависимостей**: SSE — Hono streaming, MCP — JSON-RPC
  руками, всё остальное — node builtins.
- Тюнинг — `FLOCK_*` env (`FLOCK_PORT`, `FLOCK_ESC_*`, `FLOCK_HEALTH_*`,
  `FLOCK_RETENTION_*`, `FLOCK_CODEX_UPSTREAM`, …).
