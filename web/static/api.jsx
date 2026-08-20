// api.jsx — Supabase auth + authenticated fetch helper.
// Loaded before any page component. Exposes window.cpApi.

(function () {
  let sbClient = null;
  let currentSession = null;
  const listeners = new Set();

  // Resolves once getSession() has restored (or definitively failed to
  // restore) the persisted session.
  //
  // getSession() is ASYNC — Supabase reads the persisted session a tick after
  // load — so for a short window after page load `currentSession` is null even
  // for a signed-in user. Any apiFetch in that window went out with NO
  // Authorization header and came back 401/402, which is what made logging in
  // look broken while a reload "fixed" it: the reload just re-raced and won.
  //
  // Never rejects, and is resolved from a .catch too: a hung auth restore must
  // degrade to an unauthenticated request, not deadlock every fetch forever.
  let resolveAuthReady;
  const authReady = new Promise((res) => { resolveAuthReady = res; });
  // Belt and braces: if the SDK never settles (offline at load, CDN blocked),
  // release the gate anyway rather than hanging the whole app.
  setTimeout(() => resolveAuthReady && resolveAuthReady(), 5000);

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
      resolveAuthReady();
      notify();
    }).catch(() => resolveAuthReady());
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
      options: {
        data: username ? { username } : {},
        // Without this, Supabase builds the confirmation link from the
        // project's Site URL, whose default is http://localhost:3000 — so
        // every confirmation email sent from production pointed at a dead
        // localhost server and no new account could ever be activated.
        //
        // Derived from the live origin rather than hardcoded so localhost
        // development, any preview deploy and production each confirm back to
        // themselves. Root, not a deep link: `/` is the only page route this
        // app registers (see web/app.py) and the SDK's detectSessionInUrl
        // picks the tokens out of the returned hash there.
        //
        // NOTE: this URL must ALSO be on Supabase's Redirect URLs allow-list.
        // Supabase silently ignores an un-allow-listed emailRedirectTo and
        // falls back to Site URL, which would reproduce the same bug.
        emailRedirectTo: `${window.location.origin}/`,
      },
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
    // Wait for the persisted session to be restored before reading the token.
    // Without this, calls that fire in the first tick after load go out
    // unauthenticated and 401/402 — the "logging in is broken, reloading fixes
    // it" bug. Resolves immediately once settled, so this costs nothing after
    // the first moment of page life.
    await authReady;

    // Serialise the body ONCE, outside the retry, so a retried request sends
    // exactly the same bytes.
    if (opts.body && typeof opts.body !== "string" && !(opts.body instanceof FormData)) {
      opts = Object.assign({}, opts, {
        headers: Object.assign({ "Content-Type": "application/json" }, opts.headers || {}),
        body: JSON.stringify(opts.body),
      });
    }

    const send = () => {
      const headers = Object.assign({}, opts.headers || {});
      if (currentSession?.access_token) {
        headers["Authorization"] = `Bearer ${currentSession.access_token}`;
      }
      return fetch(url, Object.assign({}, opts, { headers }));
    };

    let res = await send();

    // A 401 on a request we believed was authenticated means the access token
    // expired — which happens routinely when a phone or laptop suspends the
    // tab for longer than the token's lifetime, so the SDK's refresh timer
    // never fired. Refresh once and retry; that is precisely what the user was
    // achieving by reloading, so do it for them.
    //
    // Guarded on having had a session: an unauthenticated caller getting a 401
    // is the correct answer, not something to retry.
    if (res.status === 401 && currentSession && sbClient) {
      try {
        const { data } = await sbClient.auth.refreshSession();
        if (data && data.session) {
          currentSession = data.session;
          notify();
          res = await send();          // one retry only — never a loop
        }
      } catch (e) { /* fall through to the original 401 */ }
    }

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
    // American odds display as whole numbers — round the devigged floats.
    if (b.fd_odds_book != null) books.push(["FD", Math.round(b.fd_odds_book)]);
    if (b.dk_odds_book != null) books.push(["DK", Math.round(b.dk_odds_book)]);
    if (b.pin_odds_book != null) books.push(["PIN", Math.round(b.pin_odds_book)]);
    if (b.nv_odds_book != null) books.push(["NV", Math.round(b.nv_odds_book)]);
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
  // American odds are always whole numbers; the devig math produces floats
  // (e.g. -201.0930087709964), so round every odds value to an integer before
  // it reaches a table cell. null stays null.
  function roundOdds(v) {
    return v == null ? null : Math.round(v);
  }

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
      trueOdds:  roundOdds(l.true_odds),
      // matched rows carry per-book odds; single-book rows carry one odds
      // value in line_odds — expose it as bookOdds for the Sportsbooks view.
      fd:        roundOdds(l.fd_odds ?? l.fd_odds_book),
      dk:        roundOdds(l.dk_odds ?? l.dk_odds_book),
      pin:       roundOdds(l.pin_odds ?? l.pin_odds_book),
      nv:        roundOdds(l.nv_odds ?? l.nv_odds_book),
      bookOdds:  roundOdds(l.line_odds),
      best:      roundOdds(l.best_odds),
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

  // ── Web Push (installable PWA notifications) ─────────────────────────────
  function _vapidKey() {
    return (window.__COREPROP_CONFIG && window.__COREPROP_CONFIG.vapid_public_key) || "";
  }
  function pushConfigured() { return !!_vapidKey(); }         // server has VAPID keys
  function pushSupported() {
    return "serviceWorker" in navigator && "PushManager" in window && "Notification" in window;
  }
  function isStandalone() {
    return (window.matchMedia && window.matchMedia("(display-mode: standalone)").matches) ||
           window.navigator.standalone === true;
  }
  // applicationServerKey wants a Uint8Array, not the base64url string.
  function _urlB64ToUint8(base64) {
    const padding = "=".repeat((4 - (base64.length % 4)) % 4);
    const b64 = (base64 + padding).replace(/-/g, "+").replace(/_/g, "/");
    const raw = atob(b64);
    const out = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
    return out;
  }
  async function pushSubscription() {
    if (!pushSupported()) return null;
    try { const reg = await navigator.serviceWorker.ready; return await reg.pushManager.getSubscription(); }
    catch (e) { return null; }
  }
  // Request permission, subscribe, and register it server-side. MUST be called
  // from a user gesture — iOS/Safari refuse Notification.requestPermission()
  // outside one, and iOS only allows this at all from the installed PWA.
  async function enablePush() {
    if (!pushSupported()) throw new Error("This device can't show notifications.");
    if (!pushConfigured()) throw new Error("Notifications aren't set up yet.");
    const perm = await Notification.requestPermission();
    if (perm !== "granted") throw new Error("Notifications weren't allowed.");
    const reg = await navigator.serviceWorker.ready;
    let sub = await reg.pushManager.getSubscription();
    if (!sub) {
      sub = await reg.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: _urlB64ToUint8(_vapidKey()),
      });
    }
    const j = sub.toJSON();   // { endpoint, keys: { p256dh, auth } }
    await apiFetch("/api/push/subscribe", {
      method: "POST",
      body: { endpoint: j.endpoint, keys: { p256dh: j.keys.p256dh, auth: j.keys.auth } },
    });
    return true;
  }
  async function disablePush() {
    const sub = await pushSubscription();
    if (sub) {
      const endpoint = sub.endpoint;
      try { await sub.unsubscribe(); } catch (e) {}
      try { await apiFetch("/api/push/unsubscribe", { method: "POST", body: { endpoint } }); } catch (e) {}
    }
    return true;
  }

  window.cpApi = {
    init, getSession, getUser, isLoggedIn, subscribe,
    signIn, signUp, signOut,
    apiFetch, betToUi, lineToUi, useBoardLines,
    cachedFetch, getCached, subscribeCache, prefetch,
    billingConfig, billingStatus, startCheckout, openBillingPortal,
    pushConfigured, pushSupported, isStandalone, pushSubscription, enablePush, disablePush,
  };
})();
