// C14: team snapshots — hermetic checks (filesystem + team parser, no tmux,
// no live core). Run: node dist/core/team-snap.test.js
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  validateTeamName,
  listTeams,
  readTeamSpec,
  saveSnapshot,
  readSnapshot,
  listSnapshots,
} from './team-snap.js';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-snap-'));
const home = tmp;

// 1) name validation
assert.strictEqual(validateTeamName('alpha'), true);
assert.strictEqual(validateTeamName('../evil'), false);
assert.strictEqual(validateTeamName(''), false);

// 2) no teams yet
assert.deepStrictEqual(listTeams(home), []);
assert.throws(() => readTeamSpec(home, 'nope'), /no team: nope/);

// 3) a named team round-trips
{
  const dir = path.join(home, 'teams');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'alpha.yaml'), `
pods:
  dev:
    agent: pi
    model: cat-vllm/qwen3.8-27b-fp8
  ops:
    agent: bash
`);
  const teams = listTeams(home);
  assert.strictEqual(teams.length, 1);
  assert.strictEqual(teams[0].name, 'alpha');
  assert.deepStrictEqual(teams[0].pods.sort(), ['dev', 'ops']);
  const { spec, file } = readTeamSpec(home, 'alpha');
  assert.strictEqual(spec.pods.dev.agent, 'pi');
  assert.ok(file.endsWith('alpha.yaml'));
}

// 4) a broken team file: tolerant ls, loud read
{
  fs.writeFileSync(path.join(home, 'teams', 'broken.yaml'), 'pods: [unclosed');
  const teams = listTeams(home);
  assert.strictEqual(teams.length, 1, 'ls skips the broken file');
  assert.strictEqual(teams[0].name, 'alpha');
  assert.throws(() => readTeamSpec(home, 'broken'), /cannot parse/);
}

// 5) snapshot save + mono-id + read back
{
  // a fake session file (the pod's) to copy
  const seat = path.join(home, 'pods', 'dev');
  fs.mkdirSync(path.join(seat, '.pi', 'sessions'), { recursive: true });
  const sessionFile = path.join(seat, '.pi', 'sessions', '2026-10-04T10-00-00-000Z_dev.jsonl');
  fs.writeFileSync(sessionFile, '{"type":"session","id":"dev"}\n');

  const snap1 = saveSnapshot(home, 'alpha', {
    teamFile: path.join(home, 'teams', 'alpha.yaml'),
    pods: [
      {
        role: 'dev', agent: 'pi', manifestId: 'pi', profile: null,
        model: 'cat-vllm/qwen3.8-27b-fp8', dir: seat,
        sessionFile, sessionCopy: null, restorable: true,
      },
    ],
    skipped: [{ role: 'ops', reason: 'runtime not snapshot-able: bash (pi only in C14)' }],
  });
  assert.strictEqual(snap1.id, '1', 'first snapshot is mono-id 1');
  assert.ok(snap1.pods[0].sessionCopy, 'the session was copied into the snapshot');
  assert.ok(fs.existsSync(snap1.pods[0].sessionCopy!), 'copy on disk');
  assert.strictEqual(snap1.pods[0].restorable, true);

  const snap2 = saveSnapshot(home, 'alpha', {
    teamFile: path.join(home, 'teams', 'alpha.yaml'),
    pods: [],
    skipped: [],
  });
  assert.strictEqual(snap2.id, '2', 'second snapshot is mono-id 2 (monotonic)');

  const snaps = listSnapshots(home, 'alpha');
  assert.strictEqual(snaps.length, 2);
  assert.strictEqual(snaps[0].id, '1');
  assert.strictEqual(snaps[1].id, '2');

  const { snap, dir } = readSnapshot(home, 'alpha', 'latest');
  assert.strictEqual(snap.id, '2', 'latest = the highest mono-id');
  assert.ok(dir.endsWith(path.join('snapshots', 'alpha', '2')));

  const first = readSnapshot(home, 'alpha', '1');
  assert.strictEqual(first.snap.pods[0].role, 'dev');
  assert.strictEqual(first.snap.skipped[0].role, 'ops');
}

// 6) a pod with no session: not restorable, no copy
{
  const snap = saveSnapshot(home, 'alpha', {
    teamFile: path.join(home, 'teams', 'alpha.yaml'),
    pods: [
      {
        role: 'ghost', agent: 'pi', manifestId: 'pi', profile: null,
        model: null, dir: path.join(home, 'pods', 'ghost'),
        sessionFile: null, sessionCopy: null, restorable: false,
      },
    ],
    skipped: [],
  });
  assert.strictEqual(snap.pods[0].restorable, false);
  assert.strictEqual(snap.pods[0].sessionCopy, null);
}

// 7) missing snapshot: loud
assert.throws(() => readSnapshot(home, 'alpha', '999'), /no snapshot: alpha\/999/);
assert.throws(() => readSnapshot(home, 'alpha', 'not-a-number'), /bad snapshot ref/);
assert.throws(() => readSnapshot(home, 'other', 'latest'), /no snapshots for team other/);

fs.rmSync(tmp, { recursive: true, force: true });
console.log('team-snap.test.ts: all checks passed');
