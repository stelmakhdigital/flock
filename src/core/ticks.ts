// Tick registry: every tick is a module with its own status, surfaced in /healthz.

export interface TickStatus {
  name: string;
  intervalMs: number;
  runs: number;
  lastRun: string | null;
  lastError: string | null;
  durationMs: number | null;
}

const msg = (e: unknown) => (e instanceof Error ? e.message : String(e));

export class Ticks {
  private timers: NodeJS.Timeout[] = [];
  private statuses = new Map<string, TickStatus>();

  register(name: string, intervalMs: number, fn: () => void | Promise<void>): void {
    const st: TickStatus = { name, intervalMs, runs: 0, lastRun: null, lastError: null, durationMs: null };
    this.statuses.set(name, st);
    const run = async () => {
      const t0 = Date.now();
      try {
        await fn();
        st.lastError = null;
      } catch (e) {
        st.lastError = msg(e);
      } finally {
        st.lastRun = new Date().toISOString();
        st.durationMs = Date.now() - t0;
        st.runs++;
      }
    };
    void run(); // first pass immediately
    this.timers.push(setInterval(run, intervalMs));
  }

  stop(): void {
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
  }

  all(): TickStatus[] {
    return [...this.statuses.values()];
  }
}
