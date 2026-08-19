// Shared components: Logo, TopNav, AuthModal, badges, etc.

const { useState, useEffect, useRef, useMemo } = React;

// ───────── Logo ─────────
function Logo({ size = 32, animated = true }) {
  // Use the real brand wordmark (logo_full.png) — transparent background,
  // blue wave mark + white "CoreProp" text, built for dark surfaces.
  // Fall back to logo.png (mark only) if the wordmark fails to load.
  const h = size;
  return (
    <div className="cp-logo">
      <img
        src="/static/logo_full.png?v=3"
        alt="CoreProp"
        className={"cp-mark-img " + (animated ? "is-spin" : "")}
        style={{ height: h + "px", width: "auto", background: "transparent" }}
        onError={(e) => {
          if (!e.currentTarget.dataset.fallback) {
            e.currentTarget.dataset.fallback = "1";
            e.currentTarget.src = "/static/logo.png?v=3";
          }
        }}
      />
    </div>
  );
}

// ───────── Top Nav ─────────
const NAV_TABS = ["+EV Bets", "Combined Lines", "PrizePicks Lines", "Sportsbooks", "Backtest", "Analytics"];

function TopNav({ active, onTab, onLogin, onSignup, loggedIn, onLogout, variant = "app" }) {
  // Signed-out visitors on the landing page get no app tabs. All six used to
  // render for everyone: they look like navigation, but every click just
  // reopened the auth modal (app-main.jsx onTab bails when !loggedIn), and on a
  // phone they pushed the nav to a second, horizontally-scrolling row that
  // covered a third of the first screen. The landing page has its own in-page
  // nav in the footer.
  const showTabs = loggedIn;
  // Logo destination: signed-in users land on +EV Bets; signed-out goes to
  // the marketing landing page regardless of which tab they were viewing.
  const logoDest = loggedIn ? "+EV Bets" : "landing";
  const [busy, setBusy] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef(null);
  const user = (window.cpApi && window.cpApi.getUser && window.cpApi.getUser()) || null;
  const email = user?.email || "";
  const displayName = (user?.user_metadata?.username || email || "Account");
  // Google returns the profile photo as `avatar_url` (and `picture` on some
  // token shapes). Email/password users have neither, so the lettered circle
  // stays as the fallback — and it also has to catch a broken/expired photo
  // URL at runtime, hence the onError swap below.
  const avatarUrl = user?.user_metadata?.avatar_url || user?.user_metadata?.picture || "";
  const [avatarBroken, setAvatarBroken] = useState(false);
  const initial = (email || user?.user_metadata?.username || "?").trim().charAt(0).toUpperCase();

  const handleLogout = async () => {
    if (busy) return;
    setBusy(true);
    try { await (onLogout && onLogout()); }
    finally { setBusy(false); setMenuOpen(false); }
  };

  // Close the profile dropdown on any outside click or Escape.
  useEffect(() => {
    if (!menuOpen) return;
    const onDown = (e) => { if (menuRef.current && !menuRef.current.contains(e.target)) setMenuOpen(false); };
    const onKey = (e) => { if (e.key === "Escape") setMenuOpen(false); };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => { document.removeEventListener("mousedown", onDown); document.removeEventListener("keydown", onKey); };
  }, [menuOpen]);

  return (
    <header className={"cp-nav " + (variant === "landing" ? "is-landing" : "")}>
      <div className="cp-nav-l">
        <button className="cp-logo-btn" onClick={() => onTab && onTab(logoDest)} aria-label="Home">
          <Logo />
        </button>
      </div>
      <nav className="cp-nav-c" aria-label="Sections">
        {showTabs && NAV_TABS.map(t => (
          <button
            key={t}
            className={"cp-tab " + (active === t ? "is-active" : "")}
            onClick={() => onTab && onTab(t)}
            aria-current={active === t ? "page" : undefined}
          >{t}</button>
        ))}
      </nav>
      <div className="cp-nav-r">
        {loggedIn ? (
          <div className="cp-user" ref={menuRef}>
            <button
              className="cp-avatar-btn"
              onClick={() => setMenuOpen(o => !o)}
              aria-haspopup="menu"
              aria-expanded={menuOpen}
              title={email}
            >
              {avatarUrl && !avatarBroken ? (
                <img
                  className="cp-avatar"
                  src={avatarUrl}
                  alt=""
                  referrerPolicy="no-referrer"
                  style={{ objectFit: "cover", padding: 0 }}
                  onError={() => setAvatarBroken(true)}
                />
              ) : (
                <span className="cp-avatar">{initial}</span>
              )}
            </button>
            {menuOpen && (
              <div className="cp-menu" role="menu">
                <div className="cp-menu-hd">
                  {avatarUrl && !avatarBroken ? (
                    <img className="cp-avatar cp-avatar-lg" src={avatarUrl} alt=""
                         referrerPolicy="no-referrer"
                         style={{ objectFit: "cover", padding: 0 }}
                         onError={() => setAvatarBroken(true)} />
                  ) : (
                    <div className="cp-avatar cp-avatar-lg">{initial}</div>
                  )}
                  <div className="cp-menu-id">
                    <div className="cp-menu-name">{displayName}</div>
                    {email && <div className="cp-menu-email">{email}</div>}
                  </div>
                </div>
                <div className="cp-menu-sep" />
                <button className="cp-menu-item" role="menuitem" onClick={handleLogout} disabled={busy}>
                  {busy ? "Logging out…" : "Log out"}
                </button>
              </div>
            )}
          </div>
        ) : (
          // Signed out: log in, plus a route to pricing. Previously the only
          // control here was "Log In", so the primary conversion path lived
          // exclusively in the page body and vanished as soon as you scrolled.
          <>
            {/* Sign up is the primary action for a signed-out visitor; "Log in"
                is the quieter secondary for people who already have an account.
                "See pricing" stays available but no longer competes as the only
                filled button. */}
            <button className="cp-btn cp-btn-ghost cp-btn-sm" onClick={onLogin}>Log in</button>
            <button className="cp-btn cp-btn-ghost cp-btn-sm" onClick={() => onTab && onTab("pricing")}>
              Pricing
            </button>
            <button className="cp-btn cp-btn-primary cp-btn-sm" onClick={onSignup || onLogin}>
              Sign up
            </button>
          </>
        )}
      </div>
    </header>
  );
}

// ───────── Auth Modal ─────────
function AuthModal({ open, onClose, onSubmit, notice, initialMode }) {
  // Signup is the default: a signed-out visitor is far more likely to be new
  // than returning, and the whole funnel depends on them creating an account.
  const [mode, setMode] = useState(initialMode || "signup");
  const [email, setEmail] = useState("");
  const [pw, setPw] = useState("");
  const [pw2, setPw2] = useState("");
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  // Set to the address we just signed up once Supabase accepts the account but
  // withholds a session pending email confirmation. Deliberately NOT reusing
  // `err`: that renders in #FCA5A5 with role="alert", so the single most
  // encouraging moment in the funnel — the account being created — was being
  // announced to the user as a red failure.
  const [signedUpEmail, setSignedUpEmail] = useState("");
  // "" | "sent" — drives the forgot-password sub-view. Separate from `mode` so
  // returning from it restores the login tab the user came from.
  const [resetSent, setResetSent] = useState("");
  const dialogRef = useRef(null);
  const firstFieldRef = useRef(null);
  // Re-read on every open: api.jsx probes Supabase asynchronously, so the
  // provider may have resolved after this component first mounted.
  const googleOn = !!(window.cpApi && window.cpApi.googleEnabled && window.cpApi.googleEnabled());

  // Escape to dismiss, and don't let the page behind scroll while the modal
  // owns the screen. The dropdown in TopNav already did both; this dialog did
  // neither, so the only way out was hitting the small ✕ or the backdrop.
  // Hooks must run unconditionally — the `if (!open) return null` below is
  // AFTER every hook for that reason.
  useEffect(() => {
    if (!open) return;
    const onKey = (e) => { if (e.key === "Escape") onClose && onClose(); };
    document.addEventListener("keydown", onKey);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = prevOverflow;
    };
  }, [open, onClose]);

  // Move focus into the dialog on open so a keyboard user lands on the email
  // field instead of staying on the button behind the backdrop.
  useEffect(() => {
    if (open && firstFieldRef.current) firstFieldRef.current.focus();
  }, [open]);

  // Keep Tab inside the dialog. Without this, tabbing walks out into the
  // landing page underneath, which is still rendered and still focusable.
  const onKeyDownTrap = (e) => {
    if (e.key !== "Tab" || !dialogRef.current) return;
    const focusable = dialogRef.current.querySelectorAll(
      'button:not([disabled]), input:not([disabled]), [href], [tabindex]:not([tabindex="-1"])'
    );
    if (!focusable.length) return;
    const first = focusable[0], last = focusable[focusable.length - 1];
    if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
  };

  // Clear the confirmation panel when the dialog is dismissed, so reopening it
  // shows the form again rather than a stale "check your email".
  useEffect(() => {
    if (!open) { setSignedUpEmail(""); setErr(""); setResetSent(""); }
  }, [open]);

  // Honour the mode the opener asked for each time the dialog opens. Without
  // this, `useState(initialMode)` would only apply on first mount, so the
  // dead-link rescue would land on the signup tab after the dialog had been
  // opened once already.
  useEffect(() => {
    if (open && initialMode) setMode(initialMode);
  }, [open, initialMode]);

  if (!open) return null;

  const submit = async (e) => {
    e.preventDefault();
    setErr("");
    if (!email || !pw) { setErr("Enter your email and password."); return; }
    if (mode === "signup" && pw !== pw2) { setErr("Those passwords don't match."); return; }
    setBusy(true);
    try {
      if (mode === "login") {
        await window.cpApi.signIn(email, pw);
      } else {
        const res = await window.cpApi.signUp(email, pw);
        if (!res.session) {
          // CAREFUL: a signup for an email that ALREADY has an account also
          // lands here, and Supabase deliberately makes it look identical —
          // it returns HTTP 200 with a FABRICATED user (random id, a
          // confirmation_sent_at of "now") and sends no email at all. That is
          // its anti-enumeration defence: an attacker must not be able to
          // probe which addresses are registered.
          //
          // The one honest signal is `identities`: length 1 for a genuine new
          // signup, length 0 when the address is already taken. Verified
          // against this project's live auth server both ways.
          //
          // Without this check an existing user who forgets they registered
          // gets the cheerful "check your email" panel and waits forever for
          // a message that is never sent.
          const identities = (res.user && res.user.identities) || res.identities;
          if (Array.isArray(identities) && identities.length === 0) {
            setErr("That email already has an account — log in instead.");
            setBusy(false);
            return;
          }
          // Genuine new signup awaiting confirmation. Swap the whole dialog
          // for a confirmation panel rather than dropping a line of red text
          // under a form that still looks like it needs resubmitting.
          setSignedUpEmail(email);
          setBusy(false);
          return;
        }
      }
      onSubmit && onSubmit({ mode, email });
    } catch (ex) {
      setErr(ex.message || "Couldn't sign you in. Try again.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="cp-modal-back" onClick={onClose}>
      <div
        className="cp-modal"
        onClick={e => e.stopPropagation()}
        onKeyDown={onKeyDownTrap}
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-label={mode === "login" ? "Log in to CoreProp" : "Create a CoreProp account"}
      >
        <button className="cp-modal-x" onClick={onClose} aria-label="Close">✕</button>
        <div className="cp-modal-logo">
          <Logo size={56} animated={false} />
        </div>

        {mode === "forgot" ? (
          <div style={{ padding: "4px 4px 8px" }}>
            {resetSent ? (
              // Same copy whether or not the address exists. Saying "no such
              // account" here would turn this form into an enumeration oracle.
              <div style={{ textAlign: "center" }} role="status">
                <div aria-hidden="true" style={{
                  width: 56, height: 56, margin: "0 auto 18px", borderRadius: "50%",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  background: "rgba(34,197,94,.14)", color: "#86EFAC", fontSize: 28,
                }}>✓</div>
                <h2 style={{ margin: "0 0 10px", fontSize: 20, fontWeight: 650, color: "var(--text)" }}>
                  Check your email
                </h2>
                <p style={{ margin: "0 0 20px", fontSize: 14, lineHeight: 1.55, color: "var(--text-2)" }}>
                  If an account exists for <b style={{ color: "var(--text-2)" }}>{resetSent}</b>, a
                  password reset link is on its way.
                </p>
                <button className="cp-btn cp-btn-primary cp-btn-lg" type="button"
                        style={{ width: "100%" }}
                        onClick={() => { setMode("login"); setResetSent(""); setErr(""); }}>
                  Back to log in
                </button>
              </div>
            ) : (
              <>
                <h2 style={{ margin: "0 0 8px", fontSize: 19, fontWeight: 650, color: "var(--text)" }}>
                  Reset your password
                </h2>
                <p style={{ margin: "0 0 16px", fontSize: 13, lineHeight: 1.5, color: "var(--text-3)" }}>
                  Enter your email and we'll send you a link to set a new one.
                </p>
                <form className="cp-form" onSubmit={async (e) => {
                  e.preventDefault();
                  setErr("");
                  if (!email) { setErr("Enter your email."); return; }
                  setBusy(true);
                  try {
                    await window.cpApi.requestPasswordReset(email);
                    setResetSent(email);
                  } catch (ex) {
                    setErr(ex.message || "Couldn't send the reset email. Try again.");
                  } finally { setBusy(false); }
                }}>
                  <input className="cp-input" placeholder="Email" type="email" aria-label="Email"
                         autoComplete="email" value={email}
                         onChange={e => setEmail(e.target.value)} disabled={busy} />
                  {err && <div role="alert" style={{ color: "#FCA5A5", fontSize: 13, padding: "2px 4px" }}>{err}</div>}
                  <button className="cp-btn cp-btn-primary cp-btn-lg" type="submit" disabled={busy}>
                    {busy ? "Sending…" : "Send reset link"}
                  </button>
                </form>
                <button type="button" className="cp-link"
                        style={{ display: "block", margin: "14px auto 0", fontSize: 13 }}
                        onClick={() => { setMode("login"); setErr(""); }}>
                  ← Back to log in
                </button>
              </>
            )}
          </div>
        ) : signedUpEmail ? (
          // Account created, awaiting email confirmation. role="status" (not
          // "alert") so assistive tech reads this as the good news it is.
          <div style={{ textAlign: "center", padding: "4px 4px 8px" }} role="status">
            <div
              aria-hidden="true"
              style={{
                width: 56, height: 56, margin: "0 auto 18px", borderRadius: "50%",
                display: "flex", alignItems: "center", justifyContent: "center",
                background: "rgba(34,197,94,.14)", color: "#86EFAC", fontSize: 28,
              }}
            >✓</div>
            <h2 style={{ margin: "0 0 10px", fontSize: 20, fontWeight: 650, color: "var(--text)" }}>
              Thank you for choosing CoreProp!
            </h2>
            <p style={{ margin: "0 0 6px", fontSize: 14, lineHeight: 1.55, color: "var(--text-2)" }}>
              Please check your email for verification to use your new account.
            </p>
            {/* Showing the address catches a typo'd signup right here, instead
                of after the user has waited for mail that can never arrive. */}
            <p style={{ margin: "0 0 20px", fontSize: 13, color: "var(--text-3)" }}>
              Sent to <b style={{ color: "var(--text-2)" }}>{signedUpEmail}</b>
            </p>
            <button
              className="cp-btn cp-btn-primary cp-btn-lg"
              type="button"
              style={{ width: "100%" }}
              onClick={onClose}
            >
              Got it
            </button>
            <p style={{ margin: "14px 0 0", fontSize: 12, color: "var(--text-3)" }}>
              Not there in a minute? Check your spam folder.
            </p>
          </div>
        ) : (
        <>
        {/* Explanatory copy from the opener (e.g. a consumed confirmation
            link). Neutral blue, role=status — this is information, not the
            user's mistake, so it must not read as a validation error. */}
        {notice && (
          <div
            role="status"
            style={{
              margin: "0 0 14px", padding: "10px 12px", borderRadius: 10,
              background: "rgba(59,130,246,.12)", border: "1px solid rgba(59,130,246,.28)",
              color: "#BFDBFE", fontSize: 13, lineHeight: 1.5,
            }}
          >{notice}</div>
        )}
        <div className="cp-seg">
          <button type="button" className={"cp-seg-btn " + (mode === "login" ? "is-active" : "")} onClick={() => { setMode("login"); setErr(""); }}>Log in</button>
          <button type="button" className={"cp-seg-btn " + (mode === "signup" ? "is-active" : "")} onClick={() => { setMode("signup"); setErr(""); }}>Sign up</button>
        </div>

        {/* Only rendered once Supabase actually reports the provider as
            configured (api.jsx probes /auth/v1/settings). Showing it while the
            provider is off would redirect the user to a Supabase error page,
            which reads as "this site is broken". */}
        {googleOn && (
          <>
            <button
              type="button"
              className="cp-btn cp-btn-lg"
              style={{
                width: "100%", background: "#fff", color: "#1f2937",
                display: "flex", alignItems: "center", justifyContent: "center", gap: 10,
                fontWeight: 600, marginBottom: 14,
              }}
              disabled={busy}
              onClick={async () => {
                setErr("");
                setBusy(true);
                try {
                  await window.cpApi.signInWithGoogle("/");   // navigates away
                } catch (ex) {
                  setErr(ex.message || "Couldn't start Google sign-in.");
                  setBusy(false);
                }
              }}
            >
              {/* Google's brand guidelines require the official four-colour
                  mark; a monochrome "G" is a trademark problem. */}
              <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
                <path fill="#4285F4" d="M17.64 9.2c0-.64-.06-1.25-.16-1.84H9v3.48h4.84a4.14 4.14 0 0 1-1.8 2.72v2.26h2.92c1.7-1.57 2.68-3.88 2.68-6.62z"/>
                <path fill="#34A853" d="M9 18c2.43 0 4.47-.8 5.96-2.18l-2.92-2.26c-.8.54-1.84.86-3.04.86-2.34 0-4.32-1.58-5.03-3.7H.96v2.33A9 9 0 0 0 9 18z"/>
                <path fill="#FBBC05" d="M3.97 10.72a5.4 5.4 0 0 1 0-3.44V4.95H.96a9 9 0 0 0 0 8.1l3.01-2.33z"/>
                <path fill="#EA4335" d="M9 3.58c1.32 0 2.5.45 3.44 1.35l2.58-2.58C13.46.89 11.43 0 9 0A9 9 0 0 0 .96 4.95l3.01 2.33C4.68 5.16 6.66 3.58 9 3.58z"/>
              </svg>
              <span>{mode === "signup" ? "Sign up with Google" : "Continue with Google"}</span>
            </button>
            <div style={{
              display: "flex", alignItems: "center", gap: 10,
              color: "var(--text-3)", fontSize: 12, margin: "0 0 14px",
            }}>
              <span style={{ flex: 1, height: 1, background: "rgba(255,255,255,.10)" }} />
              or
              <span style={{ flex: 1, height: 1, background: "rgba(255,255,255,.10)" }} />
            </div>
          </>
        )}
        <form className="cp-form" onSubmit={submit}>
          <input ref={firstFieldRef} className="cp-input" placeholder="Email" type="email" aria-label="Email" autoComplete="email" value={email} onChange={e => setEmail(e.target.value)} disabled={busy} />
          <input className="cp-input" placeholder="Password" type="password" aria-label="Password" autoComplete={mode === "login" ? "current-password" : "new-password"} value={pw} onChange={e => setPw(e.target.value)} disabled={busy} />
          {mode === "signup" && <input className="cp-input" placeholder="Confirm password" type="password" aria-label="Confirm password" autoComplete="new-password" value={pw2} onChange={e => setPw2(e.target.value)} disabled={busy} />}
          {/* role=alert so a screen reader announces the failure; it was a
              silent colored div before. */}
          {err && <div role="alert" style={{color:"#FCA5A5", fontSize:13, padding:"2px 4px"}}>{err}</div>}
          <button className="cp-btn cp-btn-primary cp-btn-lg" type="submit" disabled={busy}>
            {busy ? "Working…" : (mode === "login" ? "Log in" : "Create account")}
          </button>
        </form>
        {mode === "login" && (
          <button type="button" className="cp-link"
                  style={{ display: "block", margin: "12px auto 0", fontSize: 13 }}
                  onClick={() => { setMode("forgot"); setErr(""); }}>
            Forgot your password?
          </button>
        )}
        </>
        )}
      </div>
    </div>
  );
}

// ───────── Sportsbook badge ─────────
function BookBadge({ book, odds }) {
  const colors = {
    FD:  { bg: "rgba(239,68,68,.16)",  fg: "#FCA5A5" },
    DK:  { bg: "rgba(34,197,94,.16)",  fg: "#86EFAC" },
    PIN: { bg: "rgba(250,204,21,.18)", fg: "#FDE68A" },
    NV:  { bg: "rgba(45,212,191,.18)",  fg: "#5EEAD4" },
    MGM: { bg: "rgba(56,189,248,.16)", fg: "#7DD3FC" },
  };
  const c = colors[book] || colors.FD;
  const oddsStr = odds > 0 ? `+${odds}` : `${odds}`;
  return (
    <span className="cp-odd">
      <span className="cp-odd-n">{oddsStr}</span>
      <span className="cp-book" style={{ background: c.bg, color: c.fg }}>{book}</span>
    </span>
  );
}

// ───────── True % chip with heat color ─────────
function TruePct({ value }) {
  // 54–75 → green ramp
  const t = Math.min(1, Math.max(0, (value - 54) / 18));
  const hue = 145; // green
  return (
    <span className="cp-tp" style={{ color: `oklch(${0.72 + t * 0.05} ${0.14 + t * 0.04} ${hue})` }}>
      {value.toFixed(1)}%
    </span>
  );
}

// ───────── League pill ─────────
function LeaguePill({ league }) {
  // One hue per league, all distinguishable at 12.5px on the dark surface.
  // NCAAB is in config.ACTIVE_LEAGUES and was missing here, so every college
  // row rendered in the grey fallback.
  const colors = {
    NBA:   "#F97316",
    NHL:   "#60A5FA",
    MLB:   "#34D399",
    WNBA:  "#F472B6",
    NCAAB: "#FBBF24",
    NFL:   "#FCD34D",
  };
  return <span className="cp-league" style={{ color: colors[league] || "#9CA3AF" }}>{league}</span>;
}

// ───────── Animated number ─────────
function AnimatedNumber({ value, decimals = 0, suffix = "" }) {
  const [v, setV] = useState(value);
  const raf = useRef(null);
  useEffect(() => {
    const start = v;
    const t0 = performance.now();
    const dur = 700;
    const tick = (now) => {
      const k = Math.min(1, (now - t0) / dur);
      const e = 1 - Math.pow(1 - k, 3);
      setV(start + (value - start) * e);
      if (k < 1) raf.current = requestAnimationFrame(tick);
    };
    raf.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf.current);
  }, [value]);
  return <span>{v.toFixed(decimals)}{suffix}</span>;
}

// ───────── Public coverage facts ─────────
// GET /api/public/coverage (unauthenticated, web/routers/public.py): which prop
// source, which books, which leagues, and the board's real refresh interval.
//
// This exists so the marketing pages can state coverage facts they READ from
// the running server. Both the landing page and the pricing page used to
// hardcode theirs, and two of those were wrong: "refreshed every 30 seconds"
// against a scheduler that runs on _state["interval_min"] (5 min by default),
// and an NFL badge on a config that has never had an NFL league flag.
//
// Returns null until the fetch resolves. Callers MUST render a neutral
// fallback for that window rather than a placeholder number — a figure that
// silently changes a beat after paint is worse than no figure.
function useCoverage() {
  const [cov, setCov] = useState(null);
  useEffect(() => {
    let cancelled = false;
    fetch("/api/public/coverage")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => { if (!cancelled && d) setCov(d); })
      .catch(() => {});   // offline / 5xx: both pages read fine without it
    return () => { cancelled = true; };
  }, []);
  return cov;
}

// Minutes -> "5 min" / "1 hr 30 min". Whatever the interval is actually set to.
function fmtRefresh(minutes) {
  if (!minutes) return null;
  if (minutes < 60) return `${minutes} min`;
  const h = Math.floor(minutes / 60), m = minutes % 60;
  return m ? `${h} hr ${m} min` : `${h} hr`;
}

// ───────── Error boundary ─────────
//
// React 18's createRoot UNMOUNTS THE WHOLE TREE on an uncaught render error.
// With no boundary, one bad value anywhere — a null a component did not guard,
// a field missing from an API payload — replaced the entire site with a blank
// white page, and the only symptom users could report was "it flashes then goes
// blank". There was no error text, nothing in the UI, and nothing recorded.
//
// This converts that into a visible, reportable error and keeps the page alive.
// It CANNOT catch async errors (fetch rejections, event handlers) — React
// boundaries only cover render, lifecycle and constructors — so it is a safety
// net, not a substitute for handling those at the source.
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error) {
    return { error };
  }

  componentDidCatch(error, info) {
    // Keep the raw error in the console for anyone with devtools open, and
    // stash it on window so a user can be talked through reading it out over
    // chat without needing to screenshot a stack trace.
    console.error("CoreProp render error:", error, info);
    try {
      window.__COREPROP_LAST_ERROR = {
        message: String(error && error.message || error),
        stack: String(error && error.stack || ""),
        componentStack: String(info && info.componentStack || ""),
        at: new Date().toISOString(),
      };
    } catch (e) { /* never let the reporter itself throw */ }
  }

  render() {
    if (!this.state.error) return this.props.children;
    const msg = String(this.state.error && this.state.error.message || this.state.error);
    return (
      <main style={{ maxWidth: 560, margin: "12vh auto", padding: "0 24px", textAlign: "center" }}>
        <div style={{ fontSize: 40, marginBottom: 14 }} aria-hidden="true">⚠️</div>
        <h1 style={{ fontSize: 22, fontWeight: 650, margin: "0 0 10px", color: "var(--text)" }}>
          Something broke on this page
        </h1>
        <p style={{ fontSize: 14, lineHeight: 1.6, color: "var(--text-2)", margin: "0 0 18px" }}>
          This is a bug on our side, not something you did. Reloading usually fixes it.
        </p>
        <button className="cp-btn cp-btn-primary cp-btn-lg" onClick={() => window.location.reload()}>
          Reload CoreProp
        </button>
        <details style={{ marginTop: 22, textAlign: "left" }}>
          <summary style={{ cursor: "pointer", fontSize: 12.5, color: "var(--text-3)" }}>
            Technical details (helps us fix it)
          </summary>
          <pre style={{
            marginTop: 10, padding: 12, borderRadius: 8, fontSize: 11.5, lineHeight: 1.5,
            background: "rgba(255,255,255,.04)", color: "var(--text-3)",
            whiteSpace: "pre-wrap", wordBreak: "break-word", maxHeight: 220, overflow: "auto",
          }}>{msg}</pre>
        </details>
      </main>
    );
  }
}

Object.assign(window, {
  ErrorBoundary,
  Logo, TopNav, AuthModal, BookBadge, TruePct, LeaguePill, AnimatedNumber,
  NAV_TABS, useCoverage, fmtRefresh,
});
