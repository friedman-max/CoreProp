// useState/useEffect are already destructured from React at global scope by
// components.js (loaded before this file). Redeclaring them here would throw
// "Identifier 'useState' has already been declared" now that the former inline
// App block is a plain global script rather than a Babel-wrapped closure.

const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{
  "accent": "#6366F1",
  "density": "regular",
  "showHalo": true,
  "tableTint": "off"
}/*EDITMODE-END*/;

function App() {
  // Start on the app (+EV Bets) if a session is already restored at first
  // render. Supabase's getSession() usually resolves a tick LATER (see the
  // reconcile effect below), so this only wins the fast path; the effect
  // handles the common case where the session lands after mount.
  const [view, setView] = useState(
    () => (window.cpApi && window.cpApi.isLoggedIn()) ? "ev" : "landing"
  );   // landing | pricing | ev
  const [authOpen, setAuthOpen] = useState(false);
  const [loggedIn, setLoggedIn] = useState(() => !!(window.cpApi && window.cpApi.isLoggedIn()));
  const [active, setActive] = useState("+EV Bets");
  // Tabs the user has opened at least once. We keep their components mounted
  // (hidden, not destroyed) so switching back is instant — no refetch flash.
  const [visited, setVisited] = useState(() => new Set(["+EV Bets"]));
  // Billing gate. Default unlocked (subActive=true) so nothing flashes locked
  // before /api/billing/status answers, and so the app stays fully open until
  // billing enforcement is switched on server-side (BILLING_ENFORCE).
  const [subActive, setSubActive] = useState(true);
  const [subEnforce, setSubEnforce] = useState(false);
  const [t, setTweak] = useTweaks(TWEAK_DEFAULTS);

  // Load billing status on login and after returning from Stripe Checkout.
  // The webhook may lag a second or two, so on a fresh ?checkout=success we
  // poll a few times before giving up.
  useEffect(() => {
    if (!loggedIn || !window.cpApi) return;
    let cancelled = false, tries = 0;
    const justCheckedOut = /[?&]checkout=success/.test(window.location.search);
    const poll = async () => {
      try {
        const s = await window.cpApi.billingStatus();
        if (cancelled) return;
        setSubActive(!!s.active);
        setSubEnforce(!!s.enforce);
        // Keep polling briefly after checkout until the subscription lands.
        if (justCheckedOut && s.enforce && !s.active && tries < 6) {
          tries++; setTimeout(poll, 1500);
        }
      } catch (e) { /* leave unlocked on error */ }
    };
    poll();
    return () => { cancelled = true; };
  }, [loggedIn]);

  // Strip the ?checkout= flag from the URL once handled (cosmetic).
  useEffect(() => {
    if (/[?&]checkout=/.test(window.location.search)) {
      const url = new URL(window.location.href);
      url.searchParams.delete("checkout");
      window.history.replaceState({}, "", url.pathname + url.search);
    }
  }, []);

  const locked = subEnforce && !subActive;

  // Apply tweaks via CSS vars
  useEffect(() => {
    document.documentElement.style.setProperty("--primary", t.accent);
    document.documentElement.style.setProperty("--primary-hi", t.accent + "33");
  }, [t.accent]);

  // Track visited tabs for keep-alive mounting.
  useEffect(() => {
    if (view === "ev" && !visited.has(active)) {
      setVisited(prev => { const n = new Set(prev); n.add(active); return n; });
    }
  }, [view, active]);

  // If billing enforcement locks the account while the user is in the app,
  // bounce them to the pricing page to subscribe.
  useEffect(() => {
    if (locked && view === "ev") { setView("pricing"); }
  }, [locked, view]);

  // Prefetch every tab's data into the SWR cache shortly after login so the
  // first visit to each tab paints instantly instead of showing a spinner.
  useEffect(() => {
    if (!loggedIn || !window.cpApi) return;
    const id = setTimeout(() => {
      // Only the endpoints the six live tabs actually use. The old
      // /api/observatory* and /api/calibration/{curves,heatmap} entries were
      // removed server-side (simplify-v1) — prefetching them just fired four
      // 404s, each paying a Supabase-auth round trip, on every login.
      window.cpApi.prefetch([
        "/api/bootstrap/core", "/api/backtest/keys", "/api/config",
        "/api/matched", "/api/prizepicks", "/api/fanduel", "/api/draftkings", "/api/pinnacle",
        "/api/backtest/slips", "/api/analytics",
      ]);
    }, 400);
    return () => clearTimeout(id);
  }, [loggedIn]);

  // Subscribe to Supabase session changes + reconcile the initial view.
  //
  // getSession() resolves ASYNCHRONOUSLY (Supabase reads the persisted session
  // a tick after load), so at first render isLoggedIn() is usually still false
  // and `view` inits to "landing". Two things guarantee a signed-in refresh
  // lands on the app instead of the marketing page:
  //   1. reconcile() runs on mount for the case where the session already
  //      resolved between render and this effect firing (the subscribe
  //      notification would otherwise have been missed).
  //   2. the subscribe callback uses the FUNCTIONAL setView updater, so it
  //      decides from the LIVE view — never a stale closure of `view` (the
  //      old bug: the callback captured view="landing" and, once the session
  //      landed, flipped the loggedIn flag but never moved the view).
  useEffect(() => {
    if (!window.cpApi) return;
    const reconcile = (isIn) => {
      setLoggedIn(isIn);
      setView(v => {
        if (isIn && v === "landing") return "ev";
        if (!isIn && v === "ev") return "landing";
        return v;
      });
    };
    reconcile(window.cpApi.isLoggedIn());
    return window.cpApi.subscribe((session) => reconcile(!!session?.user));
  }, []);

  const onLogin = () => setAuthOpen(true);
  const onStart = () => { setView("pricing"); window.scrollTo({ top: 0 }); };
  const onAuthSubmit = () => { setAuthOpen(false); setLoggedIn(true); setView("ev"); };

  const onTab = (tab) => {
    if (tab === "landing") { setView("landing"); return; }
    if (tab === "pricing") { setView("pricing"); window.scrollTo({ top: 0 }); return; }
    if (!loggedIn) { setAuthOpen(true); return; }
    // Subscription gate: when enforcement is on and the user isn't active,
    // route any app-tab click to the pricing page instead of the locked app.
    if (locked) { setView("pricing"); window.scrollTo({ top: 0 }); return; }
    setActive(tab);
    setView("ev");
  };

  const screenLabel = view === "landing" ? "01 Landing" : view === "pricing" ? "02 Pricing" : "03 +EV Bets";

  return (
    <div className={"app density-" + t.density + (t.tableTint === "on" ? " tint-on" : "") + (t.showHalo ? "" : " no-halo")} data-screen-label={screenLabel}>
      <TopNav
        active={view === "ev" ? active : ""}
        onTab={onTab}
        onLogin={onLogin}
        loggedIn={loggedIn}
        onLogout={async () => { try { await window.cpApi.signOut(); } catch (e) {} setLoggedIn(false); setView("landing"); }}
        variant={view === "landing" ? "landing" : "app"}
      />
      {view === "landing" && <Landing onLogin={onLogin} onStart={onStart} onTab={onTab} />}
      {view === "pricing" && <PricingPage onStart={onAuthSubmit} onBack={() => setView(loggedIn && !locked ? "ev" : "landing")} loggedIn={loggedIn} locked={locked} />}
      {view === "ev" && !locked && (() => {
        // Keep-alive tab host: once a tab has been visited it stays mounted
        // and is hidden via display:none when inactive. This preserves each
        // page's state + fetched data so switching back is instant.
        const TABS = {
          "+EV Bets":         EVPage,
          "Combined Lines":   CombinedLinesPage,
          "PrizePicks Lines": PrizePicksPage,
          "Sportsbooks":      SportsbooksPage,
          "Backtest":         BacktestPage,
          "Analytics":        AnalyticsPage,
        };
        return Object.entries(TABS).map(([name, Comp]) =>
          visited.has(name) ? (
            <div key={name} style={{ display: active === name ? "block" : "none" }}>
              <Comp />
            </div>
          ) : null
        );
      })()}
      <AuthModal open={authOpen} onClose={() => setAuthOpen(false)} onSubmit={onAuthSubmit} />

      <TweaksPanel>
        <TweakSection label="Theme" />
        <TweakColor
          label="Accent"
          value={t.accent}
          options={["#6366F1", "#3DA9F0", "#A855F7", "#22C55E"]}
          onChange={(v) => setTweak("accent", v)}
        />
        <TweakRadio
          label="Density"
          value={t.density}
          options={["compact", "regular"]}
          onChange={(v) => setTweak("density", v)}
        />
        <TweakSection label="Effects" />
        <TweakToggle
          label="Logo halo glow"
          value={t.showHalo}
          onChange={(v) => setTweak("showHalo", v)}
        />
        <TweakRadio
          label="Row tint on hover"
          value={t.tableTint}
          options={["off", "on"]}
          onChange={(v) => setTweak("tableTint", v)}
        />
        <TweakSection label="Navigate" />
        <TweakButton label="Landing" onClick={() => setView("landing")} />
        <TweakButton label="Pricing" onClick={() => setView("pricing")} />
        <TweakButton label="+EV (skip auth)" onClick={() => { setLoggedIn(true); setView("ev"); }} />
      </TweaksPanel>
    </div>
  );
}

// Density + halo overrides
const styleEl = document.createElement("style");
styleEl.textContent = `
.density-compact .ev-row{padding-top:9px;padding-bottom:9px;font-size:13px}
.density-compact .ev-row-data{font-size:12.5px}
.app:not(.tint-on) .ev-row-data:hover{background:rgba(255,255,255,.025)}
`;
document.head.appendChild(styleEl);

const root = ReactDOM.createRoot(document.getElementById("root"));
root.render(<App />);
