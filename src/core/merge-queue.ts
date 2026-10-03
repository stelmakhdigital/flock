// S4 (5.4b): merge queue — the core is the integrator. With >=2 parallel
// worktree pods the merge GATE git mutations on the shared base repo must
// not interleave (two `git merge` child processes racing the same
// index/HEAD => spurious "not possible to fast-forward" / index.lock).
//
// Design (single core process, no second lock layer needed):
//   - only the MUTATING part of a merge (ffMerge / squashMerge, which
//     re-checks the base tree state itself) runs under a FIFO mutex;
//   - the read-only parts (worktreeStatus, merge-tree dry-run, the quality
//     gate test run) stay parallel: a 10-minute test run on pod A must not
//     hold up pod B's clean ff-merge;
//   - "re-verify at claim time" (the arbiter claim/verify analogy): the
//     critical section runs git against the CURRENT base state, so a base
//     that moved while the merge waited is handled by git itself — ff
//     fails cleanly (skipped with a reason), a squash re-does its 3-way
//     against the fresh base and cleans up on conflict.
//
// The queue is observable (size + events) so the operator can see a merge
// waiting behind another pod's merge.
let chain: Promise<void> = Promise.resolve();
let waiting = 0;

export function mergeQueueSize(): number {
  return waiting;
}

/** Run fn exclusively w.r.t. other queued merges (FIFO). Returns fn's result. */
export function withMergeLock<T>(fn: () => Promise<T>): Promise<T> {
  waiting += 1;
  const run = chain.then(fn, fn).finally(() => {
    waiting -= 1;
  });
  chain = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}
