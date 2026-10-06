// flock web board (UI plan [U1]+[U2]+minimal health)
// Core principle (plan §1.2): store-эндпоинты — source of truth; the SSE
// /events stream is ONLY a nudge channel — the board never renders data
// from a live frame, it re-fetches the touched panels from their store
// endpoints. Fallback: 15s poll of everything.
(() => {
  const $ = (sel) => document.querySelector(sel);
  const token = () => sessionStorage.getItem('flock-token') || '';
  const conn = (state) => { const el = $('#conn'); el.textContent = state === 'ok' ? 'live' : state === 'bad' ? 'reconnect…' : '—'; el.className = 'conn ' + (state || ''); };

  // ---------- data layer ----------
  async function apiGet(path) {
    const r = await fetch(path, { headers: { authorization: `Bearer ${token()}` } });
    if (r.status === 401) { showLogin('токен не принят (401)'); throw new Error('401'); }
    if (!r.ok) throw new Error(`${path}: ${r.status}`);
    return r.json();
  }

  // operator action transport: POST /api/ops {type, …args} → {ok, result|error}
  // (single-writer; the board never mutates state any other way — UI plan §1.3)
  async function apiOp(op) {
    const r = await fetch('/api/ops', {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${token()}` },
      body: JSON.stringify(op),
    });
    if (r.status === 401) { showLogin('токен не принят (401)'); throw new Error('401'); }
    const body = await r.json().catch(() => ({ ok: false, error: `статус ${r.status}` }));
    if (!body.ok) throw new Error(body.error || 'операция отклонена');
    return body; // { ok: true, result }
  }

  // toast(msg, kind): short operator feedback on an action result (ok/error)
  function toast(msg, kind = 'ok') {
    const el = document.createElement('div');
    el.className = `toast ${kind}`;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 4200);
  }

  // act(op, {destructive?}) — THE operator-action pattern (foundation for
  // [U4]/[U6]): destructive → confirm() → POST /api/ops → toast(ok/error).
  // Returns {ok, result?|error?|cancelled?}; the caller re-fetches the panel.
  async function act(op, { destructive = false } = {}) {
    const label = op.type + (op.id ? ` (${op.id})` : '');
    if (destructive) {
      const yes = window.confirm(`Подтвердить действие: ${label}?`);
      if (!yes) { toast(`${label}: отменено`, 'error'); return { ok: false, cancelled: true }; }
    }
    try {
      const body = await apiOp(op);
      toast(`${label}: ok`, 'ok');
      return { ok: true, result: body.result };
    } catch (e) {
      toast(`${label}: ${e.message}`, 'error');
      return { ok: false, error: e.message };
    }
  }

  // event.kind → panels to refresh (plan §1.2 map; any kind → events)
  const KIND_PANELS = {
    pod: ['pods'],
    task: ['tasks'],
    team: ['pods'],
    escalation: ['health', 'escalations'], // plan §1.2: escalation_*/health_* → health+escalations
    health: ['health', 'escalations'],
  };
  function invalidate(kind) {
    const set = new Set(['events']);
    const prefix = String(kind).split('_')[0];
    for (const p of KIND_PANELS[prefix] ?? []) set.add(p);
    return [...set];
  }
  const refetchers = { pods: refreshPods, tasks: refreshTasks, events: () => refreshEvents(false), health: refreshHealth, escalations: refreshEscalations };

  // ---------- login ----------
  function showLogin(err) { $('#app').classList.add('hidden'); $('#login').classList.remove('hidden'); if (err) $('#login-error').textContent = err; }
  function showApp() { $('#login').classList.add('hidden'); $('#app').classList.remove('hidden'); }

  $('#login-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const t = $('#token').value.trim();
    if (!t) return;
    try {
      sessionStorage.setItem('flock-token', t);
      await apiGet('/healthz');
      $('#login-error').textContent = '';
      showApp();
      start();
    } catch {
      sessionStorage.removeItem('flock-token');
      $('#login-error').textContent = 'нет доступа — проверь токен (или core не запущен)';
    }
  });
  $('#logout').addEventListener('click', () => { sessionStorage.removeItem('flock-token'); location.reload(); });

  // ---------- panels ----------
  let lastEventId = 0;
  let taskFilter = '';

  // initial=true: history paint from the store (since=0, last 200 rows),
  // then lastEventId advances to what we saw. initial=false: nudge/append —
  // only rows newer than lastEventId (dedup by id is natural).
  async function refreshEvents(initial) {
    const since = initial ? 0 : lastEventId;
    const { events } = await apiGet(`/api/events?since=${since}&limit=200`);
    const list = $('#events-list');
    for (const ev of events) {
      if (!initial && ev.id <= lastEventId) continue;
      lastEventId = Math.max(lastEventId, ev.id);
      const li = document.createElement('li');
      const at = document.createElement('span'); at.className = 'ev-at'; at.textContent = ev.at.slice(11, 19);
      const kind = document.createElement('span'); kind.className = 'ev-kind'; kind.textContent = ev.kind;
      const subj = document.createElement('span'); subj.className = 'ev-subj';
      subj.textContent = [ev.subject, ev.actor && ev.actor !== 'core' ? ev.actor : null].filter(Boolean).join(' · ');
      li.append(at, kind, subj);
      list.prepend(li); // newest on top; scroll position preserved
    }
    while (list.children.length > 300) list.lastChild.remove();
  }

  async function refreshPods() {
    const { pods } = await apiGet('/api/pods');
    const tbody = $('#pods-table tbody');
    tbody.innerHTML = '';
    for (const p of pods) {
      const tr = document.createElement('tr');
      if (p.state === 'live') tr.className = 'live';
      if (p.state === 'closed') tr.className = 'closed';
      tr.innerHTML = `<td></td><td></td><td class="state"></td><td></td><td class="dir"></td>`;
      const [c0, c1, c2, c3, c4] = tr.children;
      c0.textContent = p.role; c1.textContent = p.agent ?? '—'; c2.textContent = p.state;
      c3.textContent = p.model ?? '—'; c4.textContent = p.dir;
      tbody.appendChild(tr);
    }
  }

  async function refreshTasks() {
    const { tasks } = await apiGet(`/api/tasks?limit=200${taskFilter ? `&status=${taskFilter}` : ''}`);
    const tbody = $('#tasks-table tbody');
    tbody.innerHTML = '';
    for (const t of tasks) {
      let target = '—';
      try { const c = t.closed ? JSON.parse(t.closed) : null; if (c && c.target) target = c.target; } catch { /* keep — */ }
      const tr = document.createElement('tr');
      tr.innerHTML = `<td></td><td class="state"></td><td></td><td></td><td class="dir"></td>`;
      const [c0, c1, c2, c3, c4] = tr.children;
      c0.textContent = t.id; c1.textContent = t.status; c2.textContent = t.pod_role;
      c3.textContent = t.title; c4.textContent = target;
      tbody.appendChild(tr);
    }
  }

  // compact age of an alert (from first_at) for the Health panel
  function ageIso(iso) {
    const ms = Date.now() - Date.parse(iso);
    if (!Number.isFinite(ms) || ms < 0) return '—';
    const m = Math.floor(ms / 60000);
    if (m < 1) return `${Math.max(0, Math.floor(ms / 1000))}s`;
    if (m < 60) return `${m}m`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ${m % 60}m`;
    return `${Math.floor(h / 24)}d ${h % 24}h`;
  }

  // Health panel (plan [U3]): GET /api/health → gate/idle alerts (role, kind,
  // age) + the health opts (thresholds). Active escalations live in their own
  // panel (refreshEscalations) — "attention" is kept separate from "health".
  async function refreshHealth() {
    const h = await apiGet('/api/health');
    const parts = [];
    if (!h.alerts.length) parts.push('<div class="empty">алертов нет — все поды живы</div>');
    else {
      const rows = h.alerts.map((a) =>
        `<tr><td>${a.pod_role}</td><td>${a.kind}</td><td>${ageIso(a.first_at)}</td><td class="state">${a.state}</td><td class="dir">${a.note ?? ''}</td></tr>`).join('');
      parts.push(`<table><thead><tr><th>role</th><th>kind</th><th>age</th><th>state</th><th>note</th></tr></thead><tbody>${rows}</tbody></table>`);
    }
    const o = h.opts;
    parts.push(`<div class="opts">opts — gate≥${o.gateDetectMin}m (re-alert ${o.gateRealertMin}m) · idle≥${o.idleMin}m · nudge ${o.nudgeEveryMin}m · escalate after ${o.escalateAfterNudges} nudges / ${o.escalateAfterMin}m</div>`);
    box(parts, '#health-body');
  }

  // Escalations panel (plan [U3]): op esc_ls (ladder audit) — every row gets
  // the columns id/key/state/kind/subject/created_at; the ACTIVE states
  // (open/pm_notified/escalated) are highlighted and carry the Ack button,
  // which runs the operator-action pattern act() with confirm.
  const ESC_ACTIVE = new Set(['open', 'pm_notified', 'escalated']);
  async function refreshEscalations() {
    const { result } = await apiOp({ type: 'esc_ls' });
    const escs = Array.isArray(result) ? result : [];
    const parts = [];
    if (!escs.length) parts.push('<div class="empty">эскалаций нет</div>');
    else {
      const rows = escs.map((e) => {
        const active = ESC_ACTIVE.has(e.state);
        const btn = active ? `<button class="ack" data-id="${e.id}">Ack</button>` : '<span class="dir">—</span>';
        const created = e.created_at.slice(5, 16).replace('T', ' ');
        return `<tr class="${active ? 'active' : 'done'}"><td>${e.id}</td><td>${e.key}</td><td class="state">${e.state}</td><td>${e.kind}</td><td class="dir">${e.subject}</td><td>${created}</td><td>${btn}</td></tr>`;
      }).join('');
      parts.push(`<table><thead><tr><th>id</th><th>key</th><th>state</th><th>kind</th><th>subject</th><th>created</th><th></th></tr></thead><tbody>${rows}</tbody></table>`);
    }
    box(parts, '#escalations-body');
    document.querySelectorAll('#escalations-body .ack').forEach((b) =>
      b.addEventListener('click', async () => {
        const r = await act({ type: 'esc_ack', id: b.dataset.id }, { destructive: true });
        if (r.ok) { refreshEscalations().catch(() => {}); } // instant re-fetch, don't wait for the nudge
      }));
  }
  function box(parts, sel) { $(sel).innerHTML = parts.join(''); }

  function invalidatePanels(kind) {
    for (const p of invalidate(kind)) refetchers[p]().catch(() => {});
  }

  // ---------- SSE nudge channel (fetch-based: EventSource can't set
  // Authorization; plan §1.2). We parse only the `event:` line — the data
  // is always re-fetched from store endpoints. ----------
  let sseTimer = null;
  async function openSse() {
    try {
      const r = await fetch(`/events?since=${lastEventId}`, { headers: { authorization: `Bearer ${token()}` } });
      if (!r.ok || !r.body) throw new Error(String(r.status));
      conn('ok');
      openSse.attempts = 0;
      const reader = r.body.getReader();
      const dec = new TextDecoder();
      let buf = '';
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf('\n')) >= 0) {
          const line = buf.slice(0, i);
          buf = buf.slice(i + 1);
          if (line.startsWith('event:')) invalidatePanels(line.slice(6).trim());
        }
      }
    } catch { /* fell through */ }
    conn('bad');
    clearTimeout(sseTimer);
    sseTimer = setTimeout(openSse, Math.min(15000, 1000 * Math.pow(2, openSse.attempts++ % 4)));
  }
  openSse.attempts = 0;

  // ---------- tabs / filters ----------
  document.querySelectorAll('.tab').forEach((b) =>
    b.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach((x) => x.classList.toggle('active', x === b));
      document.querySelectorAll('.panel').forEach((x) => x.classList.toggle('hidden', x.id !== `panel-${b.dataset.panel}`));
    }));
  document.querySelectorAll('.fbtn').forEach((b) =>
    b.addEventListener('click', () => {
      taskFilter = b.dataset.status;
      document.querySelectorAll('.fbtn').forEach((x) => x.classList.toggle('active', x === b));
      refreshTasks().catch(() => {});
    }));

  // ---------- start ----------
  let started = false;
  async function start() {
    if (started) return;
    started = true;
    await refreshEvents(true); // history paint + lastEventId catch-up
    await Promise.allSettled([refreshPods(), refreshTasks(), refreshHealth(), refreshEscalations()]);
    openSse();
    setInterval(() => { void Promise.allSettled([refreshPods(), refreshTasks(), refreshEvents(false), refreshHealth(), refreshEscalations()]); }, 15000);
  }

  if (token()) {
    apiGet('/healthz').then(() => { showApp(); start(); }).catch(() => showLogin());
  }
})();
