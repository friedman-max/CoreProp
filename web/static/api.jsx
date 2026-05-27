// api.jsx — Supabase auth + authenticated fetch helper.
// Loaded before any page component. Exposes window.cpApi.

(function () {
  let sbClient = null;
  let currentSession = null;
  const listeners = new Set();

  function init() {
    if (sbClient) return sbClient;
    const cfg = window.__COREPROP_CONFIG;
    if (!cfg || !window.supabase) {
      console.error("[cpApi] missing Supabase CDN or config");
      return null;
    }
    sbClient = window.supabase.createClient(cfg.supabase_url, cfg.supabase_anon_key);
    sbClient.auth.getSession().then(({ data: { session } }) => {
      currentSession = session;
      notify();
    });
    sbClient.auth.onAuthStateChange((_evt, session) => {
      const prevId = currentSession?.user?.id || null;
      const newId = session?.user?.id || null;
      currentSession = session;
      // Force a hard reload on user switch to clear any stale render state.
      if (prevId && newId && prevId !== newId) {
        window.location.reload();
        return;
      }
      notify();
    });
    return sbClient;
  }

  function notify() {
    listeners.forEach((fn) => { try { fn(currentSession); } catch (e) { console.error(e); } });
  }

  function getSession() { return currentSession; }
  function isLoggedIn() { return !!currentSession?.user; }
  function getUser() { return currentSession?.user || null; }
  function subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); }

  async function signIn(email, password) {
    const sb = init();
    const { data, error } = await sb.auth.signInWithPassword({ email, password });
    if (error) throw error;
    currentSession = data.session;
    notify();
    return data;
  }

  async function signUp(email, password, username) {
    const sb = init();
    const { data, error } = await sb.auth.signUp({
      email, password,
      options: { data: username ? { username } : {} },
    });
    if (error) throw error;
    if (data.session) { currentSession = data.session; notify(); }
    return data;
  }

  async function signOut() {
    const sb = init();
    await sb.auth.signOut();
    currentSession = null;
    notify();
  }

  async function apiFetch(url, opts = {}) {
    const headers = Object.assign({}, opts.headers || {});
    if (currentSession?.access_token) {
      headers["Authorization"] = `Bearer ${currentSession.access_token}`;
    }
    if (opts.body && typeof opts.body !== "string" && !(opts.body instanceof FormData)) {
      headers["Content-Type"] = headers["Content-Type"] || "application/json";
      opts = Object.assign({}, opts, { body: JSON.stringify(opts.body) });
    }
    const res = await fetch(url, Object.assign({}, opts, { headers }));
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      const err = new Error(`HTTP ${res.status}: ${txt.slice(0, 200)}`);
      err.status = res.status;
      throw err;
    }
    const ct = res.headers.get("content-type") || "";
    return ct.includes("json") ? res.json() : res.text();
  }

  // Map a backend bet (snake_case) to the UI's prototype shape (camelCase).
  function betToUi(b) {
    const books = [];
    if (b.fd_odds_book != null) books.push(["FD", b.fd_odds_book]);
    if (b.dk_odds_book != null) books.push(["DK", b.dk_odds_book]);
    if (b.pin_odds_book != null) books.push(["PIN", b.pin_odds_book]);
    return {
      id:       b.bet_id,
      player:   b.player_name,
      league:   b.league,
      prop:     b.prop_type,
      line:     b.pp_line,
      side:     (b.side || "").toUpperCase(),
      truePct:  (b.true_prob || 0) * 100,
      books,
      startTime: b.start_time,
      inBacktest: !!b.in_backtest,
      raw: b,
    };
  }

  // Map a generic prop line (PP / FD / DK / PIN / matched).
  function lineToUi(l) {
    const prop = l.stat_type || l.prop_type || "";
    return {
      id:        l.line_id || l.id || (l.player_name + "|" + prop + "|" + (l.line ?? l.pp_line ?? "")),
      player:    l.player_name,
      league:    l.league,
      prop:      prop,
      line:      l.line ?? l.pp_line ?? l.fd_line ?? l.dk_line ?? l.pin_line ?? "",
      side:      (l.side || "").toUpperCase(),
      truePct:   l.true_prob != null ? l.true_prob * 100 : null,
      trueOdds:  l.true_odds != null ? l.true_odds : null,
      fd:        l.fd_odds ?? l.fd_odds_book ?? null,
      dk:        l.dk_odds ?? l.dk_odds_book ?? null,
      pin:       l.pin_odds ?? l.pin_odds_book ?? null,
      best:      l.best_odds ?? null,
      startTime: l.start_time,
      raw: l,
    };
  }

  // React hook: fetch a board endpoint and return {rows, state, error}.
  function useBoardLines(url, fieldKey) {
    const [rows, setRows] = React.useState([]);
    const [state, setState] = React.useState("loading");
    const [error, setError] = React.useState("");
    React.useEffect(() => {
      let cancelled = false;
      const load = async () => {
        try {
          const data = await apiFetch(url);
          if (cancelled) return;
          const arr = data[fieldKey] || data.lines || data.matches || data.bets || [];
          setRows(arr.map(lineToUi));
          setState("ok");
        } catch (ex) {
          if (cancelled) return;
          setError(ex.message || "Failed to load");
          setState("error");
        }
      };
      load();
      const id = setInterval(load, 30000);
      return () => { cancelled = true; clearInterval(id); };
    }, [url, fieldKey]);
    return { rows, state, error };
  }

  init();

  window.cpApi = {
    init, getSession, getUser, isLoggedIn, subscribe,
    signIn, signUp, signOut,
    apiFetch, betToUi, lineToUi, useBoardLines,
  };
})();
