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

  // event.kind → panels to refresh (plan §1.2 map; any kind → events)
  const KIND_PANELS = {
    pod: 'pods',
    task: 'tasks',
    team: 'pods',
    escalation: 'health',
    health: 'health',
  };
  function invalidate(kind) {
    const set = new Set(['events']);
    const prefix = String(kind).split('_')[0];
    if (KIND_PANELS[prefix]) set.add(KIND_PANELS[prefix]);
    return [...set];
  }
  const refetchers = { pods: refreshPods, tasks: refreshTasks, events: () => refreshEvents(false), health: refreshHealth };

  // ---------- login ----------
  function showLogin(err) { $('#app').classList.add('hidden'); $('#login').classList.remove('hidden'); if (err) $('#login-error').textContent = err; }
  function showApp() { $('#login').classList.add('hidden'); $('#app').classList.remove('hidden'); }

  $('#login-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const t = $('#token').value.trim();
    if (!t) return;
    try {
      await apiGet('/healthz');
      sessionStorage.setItem('flock-token', t);
      $('#login-error').textContent = '';
      showApp();
      start();
    } catch {
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

  async function refreshHealth() {
    const h = await apiGet('/api/health');
    const parts = [];
    if (!h.alerts.length) parts.push('<div class="empty">alert\'ов нет — все поды живы</div>');
    else {
      const rows = h.alerts.map((a) => `<tr><td>${a.pod_role}</td><td>${a.kind}</td><td>${a.state}</td><td>${a.count}</td><td>${a.first_at.slice(11, 19)}</td><td class="dir">${a.note ?? ''}</td></tr>`).join('');
      parts.push(`<table><thead><tr><th>pod</th><th>kind</th><th>state</th><th>count</th><th>first</th><th>note</th></tr></thead><tbody>${rows}</tbody></table>`);
    }
    if (h.activeEscalations.length) {
      parts.push('<h3 style="color:#8b93a7;font-size:12px;margin-top:14px">активные эскалации</h3><table><thead><tr><th>id</th><th>kind</th><th>state</th><th>subject</th><th>created</th></tr></thead><tbody>');
      for (const e of h.activeEscalations) parts.push(`<tr><td>${e.id}</td><td>${e.kind}</td><td>${e.state}</td><td>${e.subject}</td><td>${e.created_at.slice(11, 19)}</td></tr>`);
      parts.push('</tbody></table>');
    }
    box(parts, '#health-body');
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
    await Promise.allSettled([refreshPods(), refreshTasks(), refreshHealth()]);
    openSse();
    setInterval(() => { void Promise.allSettled([refreshPods(), refreshTasks(), refreshEvents(false), refreshHealth()]); }, 15000);
  }

  if (token()) {
    apiGet('/healthz').then(() => { showApp(); start(); }).catch(() => showLogin());
  }
})();
