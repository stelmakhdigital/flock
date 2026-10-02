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

## Этап 0 (готово)

core: HTTP+WS (Hono, bearer), node:sqlite с миграциями, единый путь мутаций
`apply(op)`, tick-реестр с /healthz-доказательствами, tmux-транспорт
(paste через load-buffer/paste-buffer), daemonize, рестарт-безопасность.
