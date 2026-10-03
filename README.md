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


## Этап 0 (готово)

core: HTTP+WS (Hono, bearer), node:sqlite с миграциями, единый путь мутаций
`apply(op)`, tick-реестр с /healthz-доказательствами, tmux-транспорт
(paste через load-buffer/paste-buffer), daemonize, рестарт-безопасность.
