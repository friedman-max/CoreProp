// useState/useEffect are already destructured from React at global scope by
// components.js (loaded before this file). Redeclaring them here would throw
// "Identifier 'useState' has already been declared" now that the former inline
// App block is a plain global script rather than a Babel-wrapped closure.

// `accent` MUST match --primary in index.html's :root block. The effect below
// writes it back onto document.documentElement as an INLINE style, which beats
// the stylesheet — so a stale value here silently overrides the design token
// everywhere, and the only symptom is that the CSS "doesn't work".
//
// It also has to stay dark enough for white button text to clear WCAG AA: this
// value ends up behind every `.cp-btn-primary` label. #2E90D9 measured 3.44:1.
const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{
  "accent": "#1E6FB0",
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
  // Message to seed the auth dialog with, e.g. after a dead confirmation link.
  const [authNotice, setAuthNotice] = useState("");
  // Which tab the auth dialog opens on. Signup by default — see TopNav.
  const [authMode, setAuthMode] = useState("signup");
  const [t, setTweak] = useTweaks(TWEAK_DEFAULTS);

  // Rescue the "your confirmation link is dead" dead-end.
  //
  // Supabase's confirmation link is SINGLE USE. Mail security scanners and
  // link prefetchers (university and corporate filters especially) fetch every
  // URL in an email, which CONSUMES the token — so by the time the human
  // clicks it, Supabase 303s to
  //   /#error=access_denied&error_code=otp_expired&error_description=...
  // Verified by hand: the first GET of a generated link returns
  // #access_token=…, the second returns exactly that error.
  //
  // The account IS confirmed at that point (the scanner's fetch did it). The
  // user just has no session and, with nothing handling this hash, saw a raw
  // error fragment on the landing page and had no way forward — which meant a
  // brand-new account could never reach checkout.
  //
  // So: read it, wipe it from the URL (a refresh must not resurrect it), and
  // open the login dialog explaining they can simply sign in.
  useEffect(() => {
    const hash = window.location.hash || "";
    if (!hash || hash.indexOf("error") === -1) return;
    const p = new URLSearchParams(hash.replace(/^#/, ""));
    const code = p.get("error_code");
    const desc = p.get("error_description");
    if (!code && !desc) return;

    // replaceState, not assignment: assigning location.hash leaves a "#" and
    // pushes a history entry, so Back would land on the error again.
    window.history.replaceState(null, "", window.location.pathname + window.location.search);

    setAuthNotice(
      code === "otp_expired"
        ? "That confirmation link had already been used — email providers often open links automatically. Your account is confirmed, so just log in below."
        : (desc ? desc.replace(/\+/g, " ") : "That link could not be used. Try logging in below.")
    );
    setAuthMode("login");   // the account already exists — don't offer signup
    setAuthOpen(true);
  }, []);

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

  // If billing enforcement locks the account, route to pricing — that is the
  // only thing an unpaid user can usefully do, and it is the whole revenue
  // path for a brand-new account.
  //
  // "landing" is included deliberately: a user who has just confirmed their
  // email arrives signed-in but unsubscribed, and reconcile() can leave them
  // on the marketing page. Without this they would have to find the pricing
  // link themselves. The landing page stays fully functional for signed-OUT
  // visitors — this only fires once a locked session actually exists.
  useEffect(() => {
    if (locked && (view === "ev" || view === "landing")) {
      setView("pricing");
      window.scrollTo({ top: 0 });
    }
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

  // Two entry points into the same dialog, differing only in which tab opens.
  // Named for the tab they show, NOT for the button that calls them — an
  // earlier `onLogin` that opened the SIGNUP tab got wired to a button
  // labelled "Log in" and showed a signup form.
  const openSignup = () => { setAuthNotice(""); setAuthMode("signup"); setAuthOpen(true); };
  const openLogin  = () => { setAuthNotice(""); setAuthMode("login");  setAuthOpen(true); };
  const onStart = () => { setView("pricing"); window.scrollTo({ top: 0 }); };
  // Only ever called by AuthModal, AFTER Supabase has returned a session. It is
  // the one place allowed to assert loggedIn.
  const onAuthSubmit = () => { setAuthOpen(false); setLoggedIn(true); setView("ev"); };
  // What the PRICING page's CTA calls when the visitor isn't signed in yet.
  //
  // This used to be onAuthSubmit, which set loggedIn=true and dropped the
  // visitor into the app WITHOUT any authentication: clicking "Try 7 days
  // free" while signed out rendered every app tab with cpApi.isLoggedIn()
  // still false, so each tab fired an unauthenticated fetch and showed
  // permanent empty/error state. Open the auth modal instead — after a real
  // sign-in, onAuthSubmit runs and the user lands in the app for real.
  //
  // Opens SIGNUP explicitly: someone clicking "Try 7 days free" while signed
  // out is almost certainly new. It previously called bare setAuthOpen(true),
  // which inherited whichever tab happened to be set last.
  const onNeedsAuth = openSignup;

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
        onLogin={openLogin}
        onSignup={openSignup}
        loggedIn={loggedIn}
        onLogout={async () => { try { await window.cpApi.signOut(); } catch (e) {} setLoggedIn(false); setView("landing"); }}
        variant={view === "landing" ? "landing" : "app"}
      />
      {/* Landing's own "Log in" must open the LOGIN tab. onLogin here is the
          signup entry point (see TopNav), so passing it would label a button
          "Log in" and then show a signup form. */}
      {view === "landing" && <Landing onLogin={openLogin} onStart={onStart} />}
      {view === "pricing" && <PricingPage onStart={onNeedsAuth} onBack={() => setView(loggedIn && !locked ? "ev" : "landing")} loggedIn={loggedIn} locked={locked} />}
      {/* `view === "ev" && locked` deliberately renders NOTHING here — the
          effect above bounces it to "pricing". That bounce runs after render,
          so without this fallback there is a frame (and, if the effect is ever
          skipped, a permanent state) where the page is completely blank apart
          from the nav. Render the pricing page directly instead of nothing. */}
      {view === "ev" && locked && (
        <PricingPage onStart={onNeedsAuth} onBack={() => setView("landing")} loggedIn={loggedIn} locked={locked} />
      )}
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
        // `visited` is grown by an effect, which runs AFTER render — so the
        // first render after switching to a never-opened tab matched nothing
        // and painted a blank page for a frame. Treat the active tab as
        // visited unconditionally so there is always exactly one tab mounted.
        return Object.entries(TABS).map(([name, Comp]) =>
          (visited.has(name) || name === active) ? (
            <div key={name} style={{ display: active === name ? "block" : "none" }}>
              <Comp />
            </div>
          ) : null
        );
      })()}
      <AuthModal
        open={authOpen}
        onClose={() => { setAuthOpen(false); setAuthNotice(""); }}
        onSubmit={onAuthSubmit}
        notice={authNotice}
        initialMode={authMode}
      />

      <TweaksPanel>
        <TweakSection label="Theme" />
        <TweakColor
          label="Accent"
          value={t.accent}
          // All four are dark enough for white label text (>=4.5:1).
          options={["#1E6FB0", "#2563A8", "#0F766E", "#15803D"]}
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
// ErrorBoundary OUTSIDE App: a throw during App's own render (not just a
// child's) must still produce a visible error rather than a blank document.
root.render(
  <ErrorBoundary>
    <App />
  </ErrorBoundary>
);
