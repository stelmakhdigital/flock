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
  детали в разделе Fleet ниже.)

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
  activity (gate-канал = `attach`: рычаг оператора — pane).
  **Stale-bridge guard**: sidecar несёт marker `bridge: "v2"`; раннер
  без marker (пред-v2 сборка) не декодирует v2-фреймы — core
  отказывается ему кидать (честная ошибка «relaunch the pod»), а не
  бинарный мусор в сессию; liveness такой раннер считает мёртвым.
  **Headless bash-guard**: `FLOCK_PI_BASH_GUARD_FLOOR=1` у core'а —
  все pi-поды стартуют с `--bash-guard-disabled` (интерактивное
  подтверждение в под'е никому не ответить; катастрофический «пол»
  — rm -rf/sudo/git commit — действует). Без флага — как у оператора.

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
inbox-сообщениями/poke и решает **pod-ops** (+ координатор-права: см.
«Pod-scoped авторизация»). У core нет pm-тика и pm-sweep;
special-case `pmNotify` убран — delivery = `message_send
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

## Team lifecycle (C14): named teams + snapshots + honest restore

**Именованные команды** — `~/.flock/teams/<name>.yaml`. Формат:

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

`flock team up <name>` резолвит имя (файл-путь тоже работает):

```sh
flock team up alpha            # ~/.flock/teams/alpha.yaml
flock team up ./pods.yaml      # legacy: файл напрямую
flock team ls                  # команды + последние снапшоты
```

**Team down — auto-snapshot** (`flock team down <name>`): закрывает
live-поды команды и сохраняет снапшот в
`~/.flock/snapshots/<name>/<mono-id>/` (snapshot.json + sessions/).
Копия сессии — **до** close (seat может почиститься). Под без сессии
честно помечается `restorable: false`; чужой рантайм — в `skipped`.

**Restore** (`flock team up <name> --restore <snap|latest>`):

- live-поды + restore не запрошен → reconcile (обычный `team up`);
- `--restore`: **refuse over live** (outcomes `awaiting-decision`, не
  перезаписываем); сессия копируется в seat НОВОГО пода (seat-изоляция);
  missing required session = hard failure **для того нода** (per-node
  isolation: один битый нод не топит команду);
- per-node outcome: `resumed | rebuilt | fresh | fresh-primed |
  awaiting-decision | failed | attention_required | operator_recovered`
  — печатаются в отчёт и в event log.

```sh
flock team down alpha          # close + snapshot (mono-id)
flock team up alpha --restore latest
flock team up alpha --restore 3
```

Снапшот-иды — **монотонные** (не timestamp): правило «newest» —
лексикографическое, не зависит от clock skew. `ponytail:` второй контур
восстановления поверх restart-safe core — осознанно по решению
оператора; страховки: mono-id, refuse-over-live, per-node isolation.

Pod'ы — plain-dir: рабочая директория пода — просто каталог; git,
ветки, коммиты — за агентами (свой workflow, свои коммиты). Merge-гейты,
review-гейты, merge-queue, conflict-резолвер и вся экономика — **убраны из
core** (C1/C2).

## Topologies: именованные пресеты команд (OpenRIG-топологии)

**Topology** = именованный декларативный пресет поверх team-механизма
(один файл `src/core/topologies.ts`, reconcile — тот же путь, что у
`team up`). Гейты «owner→checker» живут НЕ в core, а в guidance ролей
+ существующих ops (`task handoff`/`task done`) — второй контур не
изобретён.

```sh
flock topology ls                  # каталог
flock topology up conveyor [--dir d]   # запустить одной командой
```

Пресеты: `conveyor` (intake→plan→build→review), `adversarial-review`
(owner→checker→skeptic), `research-team` (scout→analyst→scribe),
`secrets-manager` (keeper→auditor). Цепочка — handoff'ом: роль не
закрывает свой этап, пока не передала таск следующему (review/skeptic
закрывают, либо возвращают брак назад). `--dir` — общая рабочая
dиректория для всех под'ов. Под'ы — обычные team-под'ы: `team down`/
snapshots/restore на них работают.

## Agent images (C13): чекапойнты сессий агента

**Image** = момент сессии: копия сессионного файла + верbatim-манифест
(`restore`-поле в image.json — чтобы user-defined агент, отсутствующий в
другом профиле, воссоздавался). Хранилище — файлово-каноническое:
`~/.flock/images/<name>/` (image.json + session-<name>.jsonl), sqlite не
трогаем.

```sh
flock agents image save dev --as dev-flow   # чекапойнт текущей сессии
flock agents image ls
flock pod spawn flowtest --image dev-flow   # resumed (не fresh) из копии
flock pod relaunch dev --image dev-flow     # image бьёт pin/latest/fork
flock agents image rm dev-flow              # отказ без --force (evidence-защита)
```

Seat-изоляция: сессия копируется в sessions-dir **нового** пода, image
store не мутирует. `--image` + `--fresh` = конфликт (image = resume из
чекапойнта). Pi only (единственный рантайм с сессией).

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

БД/token/log/sessions — свои, инстансы не пересекаются.

## Fleet (C15): кросс-профильная координация

Profiles = независимые core (свой FLOCK_HOME/порт/tmux). **Invariant не
ломается: каждый core — single-writer своего sqlite.** Fleet — **ребро**
(сообщения/handoff между core), не общая память и не центральный
оркестратор.

```sh
flock fleet add b http://127.0.0.1:7561 [--token <operator-token>]
flock fleet ls                    # health (/healthz удалённого) + pending-очереди
flock fleet rm b
```

Конфиг — `~/.flock/fleet.json` (per-profile). Адресация: `<profile>/<role>`;
локальный профиль = без префикса.

**Кросс-профильный message_send** — `message_send {to: "b/dev"}` → HTTP
apply() на удалённый core (там — обычный local-путь: inbox + poke).
На локальном core остаётся **только outbox-строка** (delivery-ledger):
`pending=1` пока удалённый не принял; успех → `forwarded`; сбой → pending
+ event, флит-тик (60s) ретраит.

**Кросс-профильный task_handoff — two-phase** (честно, не атомарно):

1. локально (commit): таск закрывается `handed-off (outbound, pending)`
   + строка `outbound_handoffs` (один транзакт);
2. удалённо: `fleet_handoff_accept` — создание successor с
   **предопределённым id** (идемпотентно: повтор с тем же id = no-op,
   дубликатов нет);
3. успех → локальный `committed`; сбой → `pending`, тик 60s ретраит,
   N попыток (`FLOCK_FLEET_HANDOFF_MAX_ATTEMPTS`, default 10) →
   `attention_required` + эскалация (лестница 5.4c).

Event log на **обеих** сторонах фиксирует фазы (`task_handoff` /
`fleet_handoff_accept` / `fleet_message_*`). `ponytail:` атомарный
кросс-DB handoff невозможен при single-writer (два core = два писателя);
потолок — eventual handoff с явным pending; истинная атомарность = общий
store (другая архитектура, не сейчас). НЕ делаем: центральный
оркестратор-процесс, mesh-синхронизацию состояний, репликацию sqlite.

## Кампании (5.6): именованные персистентные цели

**Кампания = именованная персистентная цель с жизненным циклом и
прогрессом; переживает много pm-пробуждений и движется без
оператора.** Tasks — юниты выполнения; pm — мозг декомпозиции; кампания
— контейнер + цикл. `ponytail:` без бюджета — экономика (usage-tracking)
убрана из core в C2: кампания — цель + цикл, не расход.

```sh
flock campaign new "Сделать X" [--pod <role>]   # planning, pm получил триггер
flock campaign ls                                # id, status, done/total
flock campaign status <id>                       # цель, задачи, note
flock campaign pause|resume|cancel <id>
flock task add <pod> "title" --campaign <id>     # задача в кампанию (pm/оператор)
```

Жизненный цикл (детерминированный тик 30s, **без LLM**):

```
planning -> running:    появилась первая задача (campaign_id)
planning -> blocked:    FLOCK_CAMPAIGN_PLAN_TTL_MIN (15) без задач
                        -> эскалация 5.4c (pm не декомпозировал)
running -> done:        все задачи терминальны и есть хотя бы одна done
running -> blocked:     открытой работы нет, что-то застряло (blocked/
                        needs) или всё отменено -> эскалация 5.4c
blocked -> running:     pm разблокировал / добавил задачу (лестница
                        закрывается сама, когда условие исцелилось)
paused/cancelled/done — терминальны для тика (resume — только оператор)
```

Декомпозиция — pm'ом: `campaign_new` шлёт ему durable-триггер (inbox +
poke), он раскладывает цель в `flock task add <pod> "<title>" --campaign <id>`
(координатор-права pm). Cancel — статус, не cascade: задачи не трогаем.
Task в кампанию — поле `campaign_id` в `task_add` / флаг `--campaign`
(валидация: существует, не cancelled/done); handoff наследует campaign_id.
Поверхность: SSE `campaign_status`, `flock pm state` (строка кампаний),
README-примеры в CLI.

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

**Координатор (pm)**: роль `pm` — единственный повышенный scope среди
подов (PM_PROTOCOL обещает ему управление командой): `task_add` для
любого пода, `task_cancel`/`task_unblock`/`task_handoff` на чужие
задачи, `pod_spawn`/`pod_relaunch`/`pod_close`, `workflow_start`,
`pm_intents`. Все остальные поды — только own-pod/own-task.

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
