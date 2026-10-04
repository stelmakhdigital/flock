// Topology catalog (OpenRIG-style topologies): named declarative team presets.
//
// A topology is just a TeamSpec (pods + guidance) — no second mechanism.
// `flock topology up <name>` reconciles it through the SAME team path
// (reconcileTeam), so team down/snapshots keep working on the pods.
// Rendering goes through the documented pods.yaml subset (team.ts parser);
// topologySpec() round-trips through the real parser, which the test guards.
import { parseTeamYaml, type TeamPodSpec, type TeamSpec } from './team.js';

export interface Topology {
  name: string;
  summary: string;
  pods: Record<string, TeamPodSpec>;
}

export const TOPOLOGIES: Topology[] = [
  {
    name: 'conveyor',
    summary: 'intake → plan → build → review: конвейер над задачей (handoff-цепочка)',
    pods: {
      intake: {
        agent: 'pi',
        guidance: 'Приём. Первая ступень конвейера: берёшь сырые запросы (inbox: `flock message ls intake`, задачи: `flock task ls --status queued`), уточняешь формулировку и передаёшь: `flock task handoff <id> plan`. Не планируй и не реализуй.',
      },
      plan: {
        agent: 'pi',
        guidance: 'План. Получаешь задачу handoff\'ом от intake. Декомпозиция: шаги + observable-критерии приёмки — в тело таска. Передаёшь: `flock task handoff <id> build`. Не пиши код.',
      },
      build: {
        agent: 'pi',
        guidance: 'Сборка. Реализуешь по плану (критерии приёмки в таске), покрываешь изменения тестами. Не закрывай сам: `flock task handoff <id> review`. Брак с review — дорабатывай и снова handoff в review.',
      },
      review: {
        agent: 'pi',
        guidance: 'Проверка. Сверяй diff с критериями приёмки, прогоняй тесты, smoke. OK → `flock task done <id> finished`. Брак → `flock task handoff <id> build` с конкретным списком дефектов в таске.',
      },
    },
  },
  {
    name: 'adversarial-review',
    summary: 'owner → checker → skeptic: реализация + независимая проверка + атака на результат',
    pods: {
      owner: {
        agent: 'pi',
        guidance: 'Владелец задачи. Реализуешь, покрываешь тестами. Не закрывай сам: `flock task handoff <id> checker`. Брак с checker/skeptic — зафиксируй замечания, доработай, снова handoff в checker.',
      },
      checker: {
        agent: 'pi',
        guidance: 'Независимый чекер. Прочитай diff и тесты, сверь с критериями приёмки в таске, прогони тесты сам. Пропуск → `flock task handoff <id> skeptic`. Брак → `flock task handoff <id> owner` с перечнем причин.',
      },
      skeptic: {
        agent: 'pi',
        guidance: 'Адверсари. Ищешь контрпримеры, edge-кейсы, провалы на границах и регрессии смежного кода. Чисто → `flock task done <id> finished`. Нашёл дыру → `flock task handoff <id> owner` с конкретными кейсами.',
      },
    },
  },
  {
    name: 'research-team',
    summary: 'scout → analyst → scribe: широкий сбор, анализ, итоговый документ',
    pods: {
      scout: {
        agent: 'pi',
        guidance: 'Разведка. Широкий сбор: web-поиск, репо, доки, аналоги. В тело таска — находки со ссылками. Не делай выводов. Готово → `flock task handoff <id> analyst`.',
      },
      analyst: {
        agent: 'pi',
        guidance: 'Анализ. Верифицируй находки scout, отбрось шум, сделай выводы с аргументами. В тело таска — структурированное резюме. Готово → `flock task handoff <id> scribe`.',
      },
      scribe: {
        agent: 'pi',
        guidance: 'Писатель. По материалам analyst напиши итоговый markdown-документ в каталоге под\'а. Не изменяй выводы. Готово → `flock task done <id> finished` (путь к документу — в отчёте).',
      },
    },
  },
  {
    name: 'secrets-manager',
    summary: 'keeper → auditor: хранение/ротация секретов + аудит на утечки',
    pods: {
      keeper: {
        agent: 'pi',
        guidance: 'Хранитель секретов. Хранит секреты в `secrets/` (вне git, .gitignore), ротирует по запросу. Никогда не печатай секреты в лог/чат, не коммить. Готово → `flock task handoff <id> auditor`.',
      },
      auditor: {
        agent: 'pi',
        guidance: 'Аудитор. Сканируй репо и git-историю на утечки секретов (git grep, git log -p по известным именам/паттернам). Чисто → `flock task done <id> finished`. Утечка → `flock task handoff <id> keeper` с местами (file:line / commit).',
      },
    },
  },
];

export function listTopologies(): { name: string; summary: string; pods: string[] }[] {
  return TOPOLOGIES.map((t) => ({ name: t.name, summary: t.summary, pods: Object.keys(t.pods) }));
}

export function getTopology(name: string): Topology {
  const t = TOPOLOGIES.find((x) => x.name === name);
  if (!t) throw new Error(`no topology: ${name} (available: ${TOPOLOGIES.map((x) => x.name).join(', ')})`);
  return t;
}

// Render a topology as a pods.yaml document in the documented subset.
export function renderTopologyYaml(name: string): string {
  const t = getTopology(name);
  const out: string[] = [`# topology: ${t.name} — ${t.summary}`, 'pods:'];
  for (const [role, p] of Object.entries(t.pods)) {
    out.push(`  ${role}:`);
    if (p.agent) out.push(`    agent: ${p.agent}`);
    if (p.model) out.push(`    model: ${p.model}`);
    if (p.profile) out.push(`    profile: ${p.profile}`);
    if (p.posture) out.push(`    posture: ${p.posture}`);
    if (p.dir) out.push(`    dir: ${p.dir}`);
    if (p.guidance) {
      out.push('    guidance: |');
      for (const line of p.guidance.split('\n')) out.push(`      ${line}`);
    }
  }
  return out.join('\n') + '\n';
}

// The reconcilable spec: round-trip through the real parser (the renderer
// is only trusted insofar as the parser accepts it and returns the pods).
export function topologySpec(name: string): TeamSpec {
  return parseTeamYaml(renderTopologyYaml(name));
}
