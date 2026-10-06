// board.test.ts env: FLOCK_HOME → fresh tmp home, set BEFORE terminal.ts
// computes TMUX_SESSION (import order: this module is imported first by
// board.test.ts, before any ops/terminal import). That way the REAL
// pod_spawn in the tests lands in an isolated tmux session
// flock-<tmpdir-basename>, never the operator's session.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

export const testHome = fs.mkdtempSync(path.join(os.tmpdir(), 'flock-board-'));
process.env.FLOCK_HOME = testHome;
