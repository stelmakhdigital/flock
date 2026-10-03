# flock

Множественная агентная автоматизация разработки: **core** (демон, единственная
власть) + **pod**'ы (агенты в терминалах) + очередь задач.

Архитектура и глоссарий — в `docs/` (не коммитится в гит по соглашению проекта).

## Быстрый старт

```sh
npm install
npm run build
./bin/flock core up
./bin/flock healthz
./bin/flock pod spawn dev           # tmux-окно flock:flock-dev с pi
./bin/flock pod send dev "напиши слово hello"
./bin/flock pod capture dev
./bin/flock core status
./bin/flock core down
```

Состояние: `~/.flock/` (flock.db, core.pid, core.log, token).

## Очередь задач (этап 1)

Одна активная задача на pod. Arbiter (тик 10с) самовольно claim'ит
очередную задачу на свободного pod, доставляет её verified-send'ом и следит,
что активная задача не осталась без pod.

```sh
./bin/flock task add dev "создай hello.txt" --body "слово flock внутри"
./bin/flock task ls [queued|active|done|blocked|cancelled]
./bin/flock task history <id>     # все переходы: created/claimed/done/...
./bin/flock task done <id>
./bin/flock task blocked <id> "почему"
./bin/flock task needs <id> "что нужно"
./bin/flock task cancel <id>
```

Статусы: `queued → active → done|blocked|cancelled|needs` (+ `active → queued`
при неудачной доставке; `needs → active|done|blocked|cancelled`). Каждый
переход — строка в `task_transitions`.

Агент отчитывается сам: текст задачи несёт протокол, агент выполняет
`flock task done|blocked|needs <id>` в своём bash (attribution через
`FLOCK_POD_ROLE`). Per-pod `AGENTS.md` (пишет core) описывает протокол.
`needs` = нужен человек/решение — оператор видит в `task ls`, отвечает
подсказкой, агент (или оператор) закрывает задачу.

## Watchdog (W1+W2)

Декларативные проверки: кто угодно (CLI/агент в pod) регистрирует job, core
оценивает по расписанию (tick 1с) и будит нужный pod.

```sh
./bin/flock watchdog add --policy timer  --after 30 --target dev
./bin/flock watchdog add --policy marker --text "CI:OK" --target dev --repeat
./bin/flock watchdog add --policy stall  --idle 120 --target dev --wake-interval 300
./bin/flock watchdog add --policy file --path ~/build/out.txt --target dev
./bin/flock watchdog ls
./bin/flock watchdog history <job_id>
./bin/flock watchdog cancel <job_id>
```

Политики: `marker` (текст в capture pod), `timer` (разбудить через N сек),
`stall` (экран не меняется N сек), `file` (файл появился/исчез).
Quiet-period (`--wake-interval`, дефолт 30с/60с для stall), timeout
(`--timeout`), история доставок.

Доставка надёжная: paste + Enter → **верификация по capture** (белспейс-
независимая) → ретраи при неуверке. Ввод во время занятого pod не теряется
(pi TUI его очередь), поэтому отправка не блокирует и не таймаутит.

Auto-registration: core кладёт `flock` в `~/.flock/bin` (на PATH в окне pod),
поэтому агент сам ставит слежку — job атрибутируется его pod'у
(`FLOCK_POD_ROLE`).

Архитектура — docs/04-watchdog.md.

## Workflow / runkeeper / адаптеры / multi-flock (этап 2.5)

**Workflow** — именованный последовательный пайплайн шагов; запуск
(instance) двигает шаги через обычную очередь задач, результат шага
передаётся в тело следующего.

```sh
./bin/flock workflow define demo --steps "intake:pm,build:dev,review:rev"
./bin/flock workflow start demo "payload для всех шагов"
./bin/flock workflow ls
./bin/flock workflow status <instance_id>
```

Шаг done → следующий сам ставится в очередь; blocked/под-лоуст →
instance останавливается. DAG/зависимости/retry — при первом реальном случае.

**Runkeeper** (тик 5с) — pid агента умер → run `crashed` за один тик
(«окно живо, агент мёртв»: постоянный pane переживает runner, поэтому
одного pid-проверки недостаточно).

**Агент-адаптеры** — manifest-driven: один generic-реализатор + декларация
`{id, command, runtime?, modelFlag?, args?, env?, guidance?, firstPrompt?,
imports?, profiles?}`. Встроенные: `pi`, `bash`, `claude`, `pm`;
свой рантайм = `<FLOCK_HOME>/agents/<id>.json` (не код). `imports` —
наследование фрагментов манифестов (merge: скаляры — потомок, args —
конкатенация, env/guidance — по ключу/id); `profiles` — per-spawn override
(`--profile <name>`), при смене профиля stale managed-blocks чистятся:

```sh
./bin/flock pod spawn dev --agent pi --model <provider/id>
./bin/flock pod spawn stub --agent bash
./bin/flock pod spawn mate --agent teammate --profile quiet
```

**Multi-flock (profiles)** — несколько изолированных инстансов core:

```sh
./bin/flock -p team2 core up        # свой home ~/.flock/team2, порт, сессия flock-team2
./bin/flock -p team2 pod spawn dev
./bin/flock -p team2 task add dev "..."
```

Порт = 7461 + hash(name) (переопределить `FLOCK_PORT`), tmux-сессия
`flock-<name>`, БД/token/log — свои. Инстансы не пересекаются.

## flock-rpc runner + изоляция пода (этап 3)

Под с агентом `pi` живёт не голым TUI, а через **runner**: процесс в окне
запускает `pi --mode rpc`, держит с ним typed JSONL-канал. Всё, что раньше
гадлось по экрану, теперь — события:

- **готовность и exit**: spawn ждёт typed ready (sidecar), смерть агента —
  `crashed(signal SIGKILL)` / `crashed(code N)` в runkeeper'е;
- **доставка с подтверждением**: `pod send` идёт `flockmsg <base64>`
  (одна строка для любого текста), ack — из sidecar, не с экрана;
- **relaunch с памятью (честный resume)**: `flock pod relaunch <role>`
  перезапускает ТОЧНЫЙ persisted session-файл (pi: `--session <file>`,
  claude: `--resume <uuid>`); файла нет → retry_fresh с записью в meta,
  никогда silent fresh; `--fork [role]` — форк сессии (pi: `--fork <ref>`,
  claude: `--resume <uuid> --fork-session`); `flock pod resume-token <role>
  <file|uuid|reset>` — зафиксировать сессию для resume (иначе — последняя);
- **постоянный pane**: окно пода — постоянный shell,
  relaunch = typed stop старого runner'а (C-c → sidecar `exited`) + новая
  команда в то же окно; скроллбек живёт через агентов, pane_pid неизменен;
  runkeeper детектит нетипизированную смерть: `runner gone (pane at shell)`;
- **здоровье подов** (этап 4.1): built-in health-чеки поверх typed-сигналов —
  **gate** (dialog ждёт человека: `flock pod answer <role> <n|текст>`) и
  **idle** (агент на паузе с active-задачей: nudge → `task needs`);
  `flock health ls` / `/api/health`, алерты в `health_alerts`;
- **goal loop / pm** (этап 4.2): `flock pm up` — LLM-lead под, который на
  триггерах (task add/done/needs/blocked/cancel, pod crash, 5min sweep)
  читает pipeline и решает через typed intents (whitelist: task_*, pod_*,
  workflow_start) — единый `apply`, audit в session pm; sweep будит pm
  только при изменениях (тихий pipeline = 0 LLM-стоимости);
- **изоляция**: per-pod конфиг pi (`PI_CODING_AGENT_DIR`/`SESSION_DIR`,
  симлинки моделей/auth), `--no-context-files` + `--append-system-prompt
  <pod>/AGENTS.md` — home-AGENTS.md (и родительские context-файлы) в под
  не попадают; per-pod sandbox-trust (уровень из manifest `trustLevel`);
- **CLI в песочнице**: per-pod unix-сокет `<pod>/core.sock` + снапшот CLI
  в `<pod>/.flock-cli/` — агент в bwrap-песочнице завершает задачи
  `flock task done` без сети и без видимого home.

```sh
./bin/flock pod spawn dev                  # runner + pi (ready-gate)
./bin/flock pod send dev "..."              # flockmsg + sidecar-ack
./bin/flock pod relaunch dev                # честный resume: точный session-файл
./bin/flock pod spawn dev2 --fork dev       # форк сессии dev в новую
```

Анатомия: чистый модуль `runner-protocol` (фрейминг, env-allowlist, билдеры,
trust/resume/fork-решения; hermetic-тест `npm test`) +
`runner.js` (child pi, зеркало в панель, sidecar, activity.jsonl).

## RuntimeAdapter + honest resume (этап 3.1)

Запуск рантайма — через 5-методный **RuntimeAdapter**:
`listInstalled / project / deliverStartup / launchHarness / checkReady`.
Адаптеры: **pi** (RPC-мост, typed session identity), **claude** (Claude Code
TUI, transcript-сессии), **bash** (plain window). Новый рантайм
= адаптер + manifest (`runtime` в JSON).

- **launch posture**: `pod spawn --posture full_bypass` форсирует полный
  bypass (pi: trust+approve; claude: `--permission-mode bypassPermissions`,
  подтверждение диалога — автоматом); `floor` уважает конфиг.
  `permissionMode` в manifest — нативная ось claude (`--permission-mode`);
  pi отклоняет (у pi своя ось — trust-уровни);
- **startup-контекст**: manifest `guidance[]` — managed blocks в
  `<pod>/AGENTS.md` (идемпотентный merge, до запуска; claude — `CLAUDE.md`);
  `firstPrompt` — первый промпт после ready (только fresh);
- **checkReady**: live-готовность в `/api/pods` (`ready.reason`);
  sidecar «ready» при панели на shell = stale (`stale_ready`);
  claude: prompt-курсор + нет открытого диалога (boot-диалоги — theme/API
  key/уведомления/folder trust/MCP/bypass-приёмка — отвечаются автоматически);
- **listInstalled**: нет бинарного рантайма — чистая ошибка на spawn.

## Claude-под (этап 3.3)

- **Изоляция**: `CLAUDE_CONFIG_DIR=<pod>/.claude` (конфиг+сессии+transcripts
  в поде), onboarding-состояние pre-seed (fresh-конфиг иначе упирается в
  connectivity-check на api.anthropic.com и умирает; partial-файл от
  оброненного запуска — merge, не skip);
- **Сессия = uuid transcript-файла** (`<pod>/.claude/projects/<slug>/<uuid>.jsonl`);
  relaunch = честный `--resume <uuid>`; `--fork` = `--resume <uuid>
  --fork-session` (fork-файл появляется после первого хода, parent не
  трогается);
- **Доставка**: raw paste + Enter, ack = рост transcript-файла (типизированный
  сигнал, не скрапинг); relaunch — typed-stop (C-c) в тот же persistent pane;
- **Runkeeper**: pane вернулась на shell = `crashed(claude exited, pane at shell)`;
- **Built-in `claude`**: vLLM через Anthropic-совместимый эндпоинт
  (`ANTHROPIC_BASE_URL` + `ANTHROPIC_API_KEY` в env manifest),
  `--effort medium` (локальная модель не принимает high).

```sh
./bin/flock pod spawn ctest --agent claude          # TUI-под (ready-gate)
./bin/flock pod spawn ctest --agent claude --posture full_bypass
./bin/flock pod relaunch ctest --fork               # форк своей сессии
```

## Worktree + S0 merge (этап 5.1)

Под с `--repo` живёт в git-worktree `flock/<role>` — изолированная ветка,
собственные коммиты, без загрязнения основного checkout:

- **spawn**: `flock pod spawn dev --repo /path/to/repo [--base main]` —
  worktree прицепляется к `~/.flock/pods/<role>/work`; база по умолчанию =
  текущая ветка репо (не detached);
- **S0 auto-merge**: при `task done` ядро делает **fast-forward merge**,
  если ветка чистая (только tracked-изменения — инфраструктура пода
  untracked по дизайну), `ahead > 0`, `behind = 0` и база проверена в
  HEAD репо. Всё остальное — `merge skipped (reason)` в transition
  (ветка остаётся кандидатом на ручной merge);
- **арбитр**: закрытый worktree-под с unmerged-коммитами → таск re-queue
  (не blocked) — коммиты не теряются;
- **relaunch** сохраняет worktree-биндинг (восстанавливается из `.git`
  файла, если поля стерты); `pod close --purge` — удалить checkout
  (ветка остаётся);
- worktree-поды: sandbox off (git и есть trust boundary), bash-guard
  autonomous (рутинный git — работа; жёсткий floor на `rm -rf` и т.п.
  остаётся).

```sh
./bin/flock pod spawn dev --repo /path/to/repo --base main
./bin/flock pod merge-status dev                  # ahead/behind/dirty/head
./bin/flock pod close dev --purge                 # worktree удалить, ветку оставить
```

## Team up (этап 5.1)

`flock team up [pods.yaml]` — декларативный состав команды, reconcile:
недостающих spawn, живым пере-merge guidance (managed block `team:<role>`
в AGENTS.md), никого не убивает. Идемпотентен — можно запускать на каждом
запуске как "раскладка":

```yaml
pods:
  dev:
    agent: pi
    model: cat-vllm/qwen3.8-27b-fp8
    repo: /path/to/repo
    base: main
    guidance: |
      Пиши на русском. Коммить с prefix dev:
  rev:
    agent: pi
    repo: /path/to/repo
    guidance: |
      Ревью: только замечания, без правок.
```

## Экономика + retention (этап 5.2)

- **Usage**: pi-runner пишет `usage`-события (input/output/cache токены,
  модель) на каждый assistant-сообщение в `activity.jsonl`; ядро (тик 60с)
  агрегирует в `usage_events` (per-seat byte-cursor переживает рестарты
  core, dedupe key, self-heal после head-trim). `flock usage [role]`,
  `/api/usage[?role=&since=]` (токены по подам — экономия pipeline);
- **Retention** (тик 24ч, env-настраивается): runs старше 14 дней →
  `runs_archive` (архив, не delete — audit trail сохраняется),
  `activity.jsonl` head-trim 20k→5k строк, `core.log` rotate >10MB.

```sh
./bin/flock usage            # токены по всем подам
./bin/flock usage dev        # один под
```

## Merge-гейт S1+S2 (этап 5.4b)

Worktree-под при `task done` проходит **merge-гейт** — merge решает гейт,
а не самооценка агента:

- **S1 — merge policy** (`--merge ff|squash|never` при spawn, `merge:` в team
  file, `merge` в agent-манифесте; default `ff` = S0):
  - `ff` — S0: fast-forward только, все отклонения — skip с причиной;
  - `squash` — **dry-run `git merge-tree base branch` ДО merge**: конфликт →
    таск `blocked (merge conflict)` + CONFLICT-суммарий в result (ноль
    полу-состояний); чисто → один коммит `flock(<task-id>): <title>` на base
    (wip-история агента в main не попадает). После squash **ветка
    передвигается на squash-коммит** (иначе следующий merge ловит add/add
    конфликт на уже-смерженных файлах);
  - `never` — ветка всегда остаётся ручным merge-кандидатом;
  - защита оператора: base-репо с незакоммиченными изменениями → merge
    отклоняется (squash чистится через `reset --hard`, только после pre-check
    чистоты);
- **S2 — quality gate**: workflow с `--require-test` (или `requireTest` в
  `--steps-json`-спеке/flag при start) — перед merge core запускает `testCmd`
  из agent-манифеста **в worktree**: green → merge, red/timeout → таск
  `blocked (quality gate: tests failed)` + лог (tail 4KB) в `run.meta`
  (`kind: quality_gate`). **Fail-closed (5.4c)**: требуется гейт, но `testCmd`
  не задан в манифесте → merge НЕ идёт: таск
  `blocked (quality gate unconfigured)` + громкий health-алерт + durable
  эскалация (не только transition-note). Оператор чинит манифест и
  `flock task unblock`. (pre-check чистоты base-репо перед `reset --hard`
  при squash-конфликте покрыт отдельным тестом — gitops.test.ts)  заметкой о дыре в конфиге (не молча).

```sh
# squash-под + workflow с тестовым гейтом
./bin/flock pod spawn dev --agent pi --repo ~/Code/myrepo --base main --merge squash
./bin/flock workflow define ship --steps "dev:dev,rev:rev" --require-test
./bin/flock workflow start ship "фича X"          # red gate -> blocked (tests failed)
```

Agent-манифесты: `merge` и `testCmd` поля (например `~/.flock/agents/pi.json`:
`"testCmd": "npm test"`). Таймаут гейта: `FLOCK_TEST_TIMEOUT_S` (default 600).

## Review-гейт S3 (этап 5.4b)

Workflow `build:dev → review:rev` — **merge откладывается до verdict'а**:

- `review: <role>` у шага = «ревьюет worktree-под `role`». При enqueue core
  кладёт в body таска **`git diff base...branch` (≤4KB) + чек-лист** (свежий
  контекст ревьюера видит изменения, а не самоописание автора);
- build-шаг при `done` не мерджит: `merge deferred (step review follows)`;
- вердикт ревью через тот же протокол: `task done` (approve) / `task blocked
  '<почему>'` (reject) — reject блокирует инстанс, merge не происходит;
- approve (последний шаг) → **deferred merge** того же merge-гейта S1+S2
  (squash/ff + quality gate); конфликт или red-гейт на этом этапе блокирует
  **инстанс** (`workflow_blocked`), а не ре-квезит агента — работа сделана,
  упала интеграция;
- frozen step task: остановленный инстанс (blocked/cancelled/done) не
  принимает поздние смены статуса шага (задача и инстанс не разъезжаются).

```sh
./bin/flock workflow define ship --steps-json '[{"id":"build","role":"dev"},{"id":"review","role":"rev","review":"dev"}]'
./bin/flock workflow start ship "фича X"
```

## Merge queue S4 (этап 5.4b)

Интегратор — **сам core** (он владеет base-репо; отдельный LLM-интегратор
здесь не экономит — merge это детерминированные git-операции). С ≥2
параллельными worktree-подами мутации на общем base-репо сериализуются:

- **FIFO-queue** (merge-queue.ts): только мутирующая часть merge
  (`ffMerge`/`squashMerge`) идёт под serial-лок; read-only часть (status,
  merge-tree dry-run, прогон тестов quality-гейта) остаётся параллельной —
  10-минутные тесты пода A не держат чистый ff-мердж пода B;
- **re-verify at claim time** (аналог claim/verify арбитра): критическая
  секция исполняет git против ТЕКУЩЕГО состояния base — если base сдвинулся,
  пока merge ждал в очереди, ff падает чисто (skipped с причиной), а squash
  переделывает 3-way против свежего base и чистится при конфликте;
- observability: события `merge_queued`/`merge_done` (role, policy, queue)
  и `mergeQueue` (глубина) в `GET /api/health`.

```sh
curl -H "Authorization: Bearer $(cat ~/.flock/token)" http://127.0.0.1:7460/api/health
# -> {"alerts":[...],"opts":{...},"mergeQueue":0}
```

## Conflict-резолвер S5 (этап 5.4b, opt-in)

**Включается болью, а не по умолчанию**: `FLOCK_RESOLVER_AGENT=pi` при
запуске core (без env резолвер выключен). При `blocked (merge conflict)`
(S1) core разворачивает цепочку:

1. **resolver-под** `resolver-<role>`: LLM-агент (manifest из env) со своей
   worktree на ветке `flock/<role>-resolve` (форк от конфликтующей ветки —
   саму ветку занять нельзя: она checked out в worktree оригинала),
   `merge: never` (резолвер никогда не мерджит сам);
2. таск резолвера: **конфликт-информация из dry-run + инструкция** —
   `git merge <base>`, разрешить конфликты **соблюдительно** (сохранить и
   работу ветки, и изменения base), `flock task done/blocked`;
3. по `done` резолвера core (tick 30s): ff-применяет fork на ветку
   оригинала (worktree должна быть чистой, ff-only) → **перезапускает тот
   же merge-гейт S1+S2** → успех: origin-таск `done` (reason: `S5 resolver:
   conflict resolved in N attempt(s)`), цепочка `resolved`;
4. **жесткий лимит попыток**: `FLOCK_RESOLVER_MAX_ATTEMPTS` (default 2) —
   исчерпан → цепочка `exhausted`, таск остаётся `blocked` у оператора.
   Нечего жечь деньги в бесконечном цикте.

```sh
FLOCK_RESOLVER_AGENT=pi FLOCK_RESOLVER_MAX_ATTEMPTS=2 ./bin/flock core up
./bin/flock resolver ls        # цепочки: running | resolved | exhausted | failed
```

Семантика разрешения: если резолв полностью абсорбирован в base (например,
резолвер честно взял версию base — работа ветки была дублем), merge-гейт
видит `no diff vs base` → это **успешное** разрешение (origin-таск `done`),
a не ошибка.

## Durable-лестница эскалации (этап 5.4c)

pm-триггеры были fire-and-forget: pm умер → триггер потерян до 5-мин
sweep, аудита «почему провисло» нет. Теперь каждый hang-worthy триггер
(pod_crashed / task_blocked / task_needs / quality-gate) первым делом
пишется в `escalations` (миграция 013) — BDD-строка, переживающая
рестарт, а потом доставляется. Tick 30s (всегда включён) водит лестницу:

```
open → pm_notified   (pm жив: триггер доставлен + pm-silence-таймер запущен)
open → escalated     (pm не жив: сразу к оператору — ничто не теряется)
pm_notified → escalated (pm молчит > FLOCK_ESC_PM_TIMEOUT_S, default 300s)
escalated → health-алерт (kind=ladder) + core.log, re-reminder каждые
             FLOCK_ESC_REMINDER_S (default 3600s) до ack/resolve
```

**Автоматическое закрытие**: tick перечитывает исходное условие (таск
ушёл из blocked/needs, run пода снова жив, инстанс ушёл из blocked) →
`resolved` с причиной. Дедуп: один активный ряд на key (повторный
краш того же пода обновляет audit-note, а не множит ряды).

```sh
./bin/flock esc ls [--all]     # активные (или все) эскалации — аудит «почему провисло»
./bin/flock esc ack <id>       # оператор: принял, напоминания прекращаются
```

`/api/health` теперь включает `activeEscalations` (id/key/state/kind/subject).

## Worktree GC в retention-свип (этап 5.4c)

Закрытые worktree-поды не копят checkout'ы и ветки: 24h sweep удаляет
worktree закрытого пода (ветка сохраняет коммиты, re-spawn idempotently
переподключается) и ветку с `ahead=0` (полностью смержена). **Ветка с
ahead>0 (UNMERGED) сохраняется** — это merge-кандидат (arbiter re-queue
полагается на него). Плюс `git worktree prune` по всем известным репо.
Отчёт: `gcWorktrees`/`gcBranches`/`gcKept` в core.log.

## Замечание по claude-адаптеру

claude — единственный «не-typed» адаптер: boot-диалоги по клавишам и
TUI-маркерам, ack по транскрипту. Маркеры — хрупкая поверхность: при
бампе версии claude сверяйте маркеры (claude-protocol.ts) с новым TUI —
hermetic-тест claude-protocol фиксирует текущие ожидания.

Пошаговые "ручки" надёжности в манифесте workflow (JSON через `--steps-json`):

```json
[
  {"id":"dev","role":"dev","timeoutMin":30,"retry":1,"priority":2},
  {"id":"rev","role":"rev","deps":["dev"]}
]
```

- **deps** (5.4d, DAG): ids шагов, которые должны быть done, прежде чем шаг
  стартует. Без deps — шаг на стартовом фронте. Последовательный пайплайн —
  вырожденный случай (шаг N зависит от N-1). Валидация на define: unknown
  dep, self-dep и **циклы** отклоняются (400). Движок: при done шага ready
  set = pending-шаги со всеми done-зависимостями → закидываются **параллельно**
  (арбитер сам распределяет по подам); тело таска получает результаты
  зависимых шагов. Состояние шага — `wf_step_state.state`
  (pending/running/done/blocked), видно в `flock workflow status`.
  Merge-отложенность S3 в DAG: merge откладывается, пока в инстансе есть
  другие незавершённые шаги. Задержка: если ready пуст, но есть running —
  ждём (не deadlock); deadlock (валидацией невозможен) блокирует инстанс.
  Live: ромб a(pi)→b,c(параллельно)→d закрылся, b и c созданы в одну секунду,
  d — только после обоих.

- **priority** (0..10): шаг-таск получает `priority = instance + step`; арбитер
  берёт из очереди **сначала приоритет, потом FIFO**. У `flock task add`
  тоже `--priority N`, у `flock workflow start` — `--priority N` (базовый
  уровень инстанса);
- **timeoutMin** (1..10080): TTL активного step-таска (счёт с `claimed_at`).
  Превышение → таск `blocked (step timeout)`; plain-таски (не workflow) TTL
  не имеют — долгий human-in-the-loop не режется;
- **retry** (0..5): бюджет повторных попыток на blocked/cancelled шаг. Пока
  бюджет не исчерпан — шаг ре-квизится (новый таск), инстанс `running`;
  исчерпан → инстанс `blocked` (оператор: `flock task unblock`). Счётчик —
  `wf_step_state`, виден в `flock workflow status`.

```sh
./bin/flock workflow define pipe --steps-json '[{"id":"dev","role":"dev","timeoutMin":30,"retry":1,"priority":2},{"id":"rev","role":"rev"}]'
./bin/flock workflow start pipe "фича X" --priority 3
./bin/flock workflow status <instance_id>   # state + stepState (attempts)
./bin/flock workflow rm <name>              # удалить определение (инстансы живут)
```

## Pod-scoped авторизация (этап 5.3)

Один токен core, но **scope зависит от сокета**: запросы, пришедшие на
unix-сокет пода (`<pod>/core.sock`), видят только ops со scope `pod`
(чтение + свои задачи: add/report/unblock/merge-status/capture/close
своего пода). Operator-only ops (spawn/relaunch/watchdog/…) через сокет
пода → 403. Операторский CLI (main-порт) не ограничен. Узкое место
(own-pod/own-task) проверяется в самом op, не в middleware.

```sh
# live-проверка (нужен live dev-под):
node dist/core/pod-scope-check.js
```

## Этап 0 (готово)

core: HTTP+WS (Hono, bearer), node:sqlite с миграциями, единый путь мутаций
`apply(op)`, tick-реестр с /healthz-доказательствами, tmux-транспорт
(paste через load-buffer/paste-buffer), daemonize, рестарт-безопасность.
