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

## Этап 0 (текущее)

core: HTTP+WS (Hono, bearer), node:sqlite с миграциями, единый путь мутаций
`apply(op)`, tick-реестр с /healthz-доказательствами, tmux-транспорт
(paste через load-buffer/paste-buffer), daemonize, рестарт-безопасность.
