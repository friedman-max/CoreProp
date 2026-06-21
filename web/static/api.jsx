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
    try {
      // scope: 'global' invalidates the JWT everywhere (including any other
      // tabs / devices for this account). Critical for users who share a
      // machine with someone else — without it, the old refresh token can
      // be used to silently restore the session after they "logged out".
      await sb.auth.signOut({ scope: "global" });
    } catch (e) {
      // Network error: fall through to local cleanup. Better to log the
      // user out client-side than to leave them stuck on the previous
      // session because Supabase happened to be slow.
      console.warn("[cpApi] signOut network error, clearing locally:", e);
    }
    currentSession = null;
    // Belt-and-suspenders: clear any Supabase keys the SDK may have left
    // behind (storage key differs by version/config). Then hard-reload so
    // every component re-initializes against a clean store.
    try {
      const keysToScrub = [];
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (k && (k.startsWith("sb-") || k.startsWith("supabase."))) keysToScrub.push(k);
      }
      keysToScrub.forEach(k => localStorage.removeItem(k));
    } catch (e) {}
    notify();
    // Hard reload to landing so no stale per-user component state survives.
    window.location.assign("/");
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
    if (b.nv_odds_book != null) books.push(["NV", b.nv_odds_book]);
    return {
      id:       b.bet_id,
      player:   b.player_name,
      league:   b.league,
      prop:     b.prop_type,
      line:     b.pp_line,
      side:     (b.side || "").toUpperCase(),
      truePct:  (b.true_prob || 0) * 100,
      oddsType: b.odds_type || "standard",
      // Green devil = PrizePicks "goblin": a discounted, higher-hit-rate line.
      isGreenDevil: (b.odds_type || "standard") === "goblin",
      books,
      startTime: b.start_time,
      // bet_key is "player|YYYY-MM-DD" — the same key /api/backtest/keys
      // returns, so the +EV page can join logged bets locally without a
      // server round-trip per row.
      betKey:   b.bet_key || null,
      inBacktest: !!b.in_backtest, // may be false until joined client-side
      raw: b,
    };
  }

  // Map a generic prop line (PP / FD / DK / PIN / matched).
  // Field shapes per endpoint (from web/app.py serializers):
  //   PP        : player_name, stat_type, line_score, side, start_time
  //   matched   : player_name, stat_type, (pp_)line, side, fd_odds, dk_odds,
  //               pin_odds, best_odds, true_odds, start_time
  //   FD/DK/PIN : player_name, stat_type, line_score, side, line_odds,
  //               true_odds, start_time   ← single book's odds in line_odds
  function lineToUi(l) {
    const prop = l.stat_type || l.prop_type || "";
    const line = l.line ?? l.pp_line ?? l.line_score ?? l.fd_line ?? l.dk_line ?? l.pin_line ?? "";
    return {
      id:        l.line_id || l.id || (l.player_name + "|" + prop + "|" + line + "|" + (l.side || "")),
      player:    l.player_name,
      league:    l.league,
      prop:      prop,
      line:      line,
      side:      (l.side || "").toUpperCase(),
      truePct:   l.true_prob != null ? l.true_prob * 100 : null,
      trueOdds:  l.true_odds != null ? l.true_odds : null,
      // matched rows carry per-book odds; single-book rows carry one odds
      // value in line_odds — expose it as bookOdds for the Sportsbooks view.
      fd:        l.fd_odds ?? l.fd_odds_book ?? null,
      dk:        l.dk_odds ?? l.dk_odds_book ?? null,
      pin:       l.pin_odds ?? l.pin_odds_book ?? null,
      bookOdds:  l.line_odds ?? null,
      best:      l.best_odds ?? null,
      startTime: l.start_time,
      raw: l,
    };
  }

  // ── Stale-while-revalidate cache ─────────────────────────────────────────
  // Last successful JSON response per URL is kept in-memory for the whole
  // session. getCached() returns it synchronously (for instant first paint
  // when a tab is re-opened); cachedFetch() returns the cached value
  // immediately if fresh, otherwise fetches. Either way it revalidates in
  // the background and notifies subscribers so the UI updates in place.
  const _cache = new Map();        // url -> { data, ts }
  const _cacheSubs = new Map();    // url -> Set<fn>
  const CACHE_TTL_MS = 25000;

  function getCached(url) {
    const e = _cache.get(url);
    return e ? e.data : null;
  }

  function subscribeCache(url, fn) {
    if (!_cacheSubs.has(url)) _cacheSubs.set(url, new Set());
    _cacheSubs.get(url).add(fn);
    return () => { const s = _cacheSubs.get(url); if (s) s.delete(fn); };
  }

  function _notifyCache(url, data) {
    const s = _cacheSubs.get(url);
    if (s) s.forEach(fn => { try { fn(data); } catch (e) { console.error(e); } });
  }

  // Returns cached data immediately when fresh; always kicks a background
  // revalidation unless told the cache is still within TTL.
  async function cachedFetch(url, opts) {
    const now = Date.now();
    const e = _cache.get(url);
    const fresh = e && (now - e.ts) < CACHE_TTL_MS;
    const revalidate = async () => {
      const data = await apiFetch(url, opts);
      _cache.set(url, { data, ts: Date.now() });
      _notifyCache(url, data);
      return data;
    };
    if (fresh) {
      revalidate().catch(() => {}); // refresh in background, ignore errors
      return e.data;
    }
    return revalidate();
  }

  // Prefetch a set of GET endpoints into the cache (fire-and-forget). Called
  // right after login so the first visit to each tab paints instantly.
  function prefetch(urls) {
    if (!isLoggedIn()) return;
    urls.forEach(u => { cachedFetch(u).catch(() => {}); });
  }

  // React hook: fetch a board endpoint and return {rows, state, error}.
  // Seeds initial rows synchronously from the SWR cache so a re-opened tab
  // paints with last-known data instantly, then revalidates.
  function useBoardLines(url, fieldKey) {
    const pick = (data) => {
      const arr = (data && (data[fieldKey] || data.lines || data.matches || data.bets)) || [];
      return arr.map(lineToUi);
    };
    const seed = getCached(url);
    const [rows, setRows] = React.useState(seed ? pick(seed) : []);
    const [state, setState] = React.useState(seed ? "ok" : "loading");
    const [error, setError] = React.useState("");
    React.useEffect(() => {
      let cancelled = false;
      const apply = (data) => { if (!cancelled) { setRows(pick(data)); setState("ok"); } };
      // Subscribe so background revalidations (incl. from other tabs/pollers)
      // update this view in place.
      const unsub = subscribeCache(url, apply);
      const load = async () => {
        try {
          const data = await cachedFetch(url);
          apply(data);
        } catch (ex) {
          if (!cancelled) { setError(ex.message || "Failed to load"); if (!getCached(url)) setState("error"); }
        }
      };
      load();
      const id = setInterval(load, 30000);
      return () => { cancelled = true; clearInterval(id); unsub(); };
    }, [url, fieldKey]);
    return { rows, state, error };
  }

  init();

  // ── Billing (Stripe) ─────────────────────────────────────────────────────
  async function billingConfig() {
    try { return await apiFetch("/api/billing/config"); }
    catch (e) { return { enabled: false, enforce: false }; }
  }
  async function billingStatus() {
    if (!isLoggedIn()) return { active: true, enforce: false, configured: false };
    try { return await apiFetch("/api/billing/status"); }
    catch (e) { return { active: true, enforce: false, configured: false }; }
  }
  // Kick off Stripe Checkout for a plan; redirects the page to Stripe.
  async function startCheckout(plan) {
    const res = await apiFetch("/api/billing/checkout", { method: "POST", body: { plan } });
    if (res && res.url) { window.location.assign(res.url); return true; }
    throw new Error("Checkout did not return a URL.");
  }
  // Open the Stripe customer portal (manage / cancel).
  async function openBillingPortal() {
    const res = await apiFetch("/api/billing/portal", { method: "POST", body: {} });
    if (res && res.url) { window.location.assign(res.url); return true; }
    throw new Error("Portal did not return a URL.");
  }

  window.cpApi = {
    init, getSession, getUser, isLoggedIn, subscribe,
    signIn, signUp, signOut,
    apiFetch, betToUi, lineToUi, useBoardLines,
    cachedFetch, getCached, subscribeCache, prefetch,
    billingConfig, billingStatus, startCheckout, openBillingPortal,
  };
})();
