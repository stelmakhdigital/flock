// usage — economy: aggregate pi-runner usage events (activity.jsonl) into
// the usage_events table. A per-seat cursor file (bytes already ingested)
// makes each line count exactly once across core restarts. activity.jsonl
// is append-only until the retention head-trim: if the file shrank below
// the cursor, restart from the beginning and let the UNIQUE dedupe key
// (role, at, input, output, cacheRead, cacheWrite) absorb the overlap.
import fs from 'node:fs';
import path from 'node:path';
import * as store from './store.js';
import { seatPaths } from './runner-protocol.js';

export function ingestUsage(ctx: { store: store.Store; home: string }, roles: string[]): { events: number } {
  let events = 0;
  for (const role of roles) {
    const p = seatPaths(ctx.home, role);
    const cursorPath = path.join(p.seatRoot, 'usage.cursor');
    let stat: fs.Stats;
    let raw: string;
    try {
      stat = fs.statSync(p.activityPath);
      raw = fs.readFileSync(p.activityPath, 'utf8');
    } catch {
      continue;
    }
    let start = 0;
    try {
      start = Number(fs.readFileSync(cursorPath, 'utf8').trim()) || 0;
    } catch {
      start = 0;
    }
    if (start > stat.size) start = 0; // head-trim happened: rescan, dedupe absorbs
    if (stat.size <= start) continue;
    const tail = raw.slice(start);
    const lines = tail.split('\n');
    let consumed = 0;
    for (const line of lines) {
      // a truncated trailing line is re-read next tick: count only full lines
      if (lines.indexOf(line) === lines.length - 1 && !tail.endsWith('\n')) break;
      consumed += Buffer.byteLength(line) + 1;
      if (!line.trim()) continue;
      let ev: Record<string, unknown>;
      try {
        ev = JSON.parse(line);
      } catch {
        continue;
      }
      if (ev.event !== 'usage' || typeof ev.input !== 'number') continue;
      try {
        store.insertUsageEventDeduped(ctx.store, {
          role,
          at: typeof ev.at === 'string' ? ev.at : new Date().toISOString(),
          input: ev.input,
          output: Number(ev.output ?? 0),
          cacheRead: Number(ev.cacheRead ?? 0),
          cacheWrite: Number(ev.cacheWrite ?? 0),
          model: typeof ev.model === 'string' ? ev.model : null,
        });
        events++;
      } catch {
        /* duplicate (rescan after trim): already counted */
      }
    }
    if (consumed > 0) {
      try {
        fs.writeFileSync(cursorPath, String(start + consumed));
      } catch {
        /* best-effort: dedupe absorbs a re-read */
      }
    }
  }
  return { events };
}
