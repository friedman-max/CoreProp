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

// Slip-alert toggle for the installed PWA. Web Push on iOS (16.4+) only works
// from the home-screen icon, so in a normal browser tab we show an
// Add-to-Home-Screen hint instead of a dead toggle. Renders nothing at all when
// the browser can't do push or the server has no VAPID keys configured.
function PushToggle() {
  const api = window.cpApi || {};
  const configured = api.pushConfigured && api.pushConfigured();
  const supported = api.pushSupported && api.pushSupported();
  const standalone = api.isStandalone && api.isStandalone();
  const [on, setOn] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  useEffect(() => {
    let alive = true;
    if (configured && supported && api.pushSubscription) {
      api.pushSubscription().then((sub) => { if (alive) setOn(!!sub); }).catch(() => {});
    }
    return () => { alive = false; };
  }, []);

  if (!configured || !supported) return null;

  if (!standalone) {
    return (
      <div style={{ padding: "8px 12px", fontSize: 12, color: "var(--text-3)", lineHeight: 1.45 }}>
        Add CoreProp to your Home Screen to get slip alerts.
      </div>
    );
  }

  const toggle = async () => {
    if (busy) return;
    setBusy(true); setErr("");
    try {
      if (on) { await api.disablePush(); setOn(false); }
      else { await api.enablePush(); setOn(true); }
    } catch (e) {
      setErr(e.message || "Couldn't update notifications.");
    } finally { setBusy(false); }
  };

  return (
    <>
      <button
        className="cp-menu-item"
        role="menuitemcheckbox"
        aria-checked={on}
        onClick={toggle}
        disabled={busy}
      >
        {busy ? "…" : (on ? "Turn off slip alerts" : "Turn on slip alerts")}
      </button>
      {err && <div style={{ padding: "0 12px 8px", fontSize: 12, color: "#FCA5A5" }}>{err}</div>}
    </>
  );
}

// ───────── Auto-place ─────────
// The on-switch for real-money auto-submission. It existed ONLY in the native
// iOS app (ios/CoreProp/Features/Account/SettingsView.swift), so on the web and
// on the installed PWA there was no way to arm it at all — the backend routes,
// the DB columns and the extension were all shipped and working, and the
// feature was still 100% inert because auto_place_consent_at could never be set.
//
// Server contract (web/app.py):
//   GET  /api/auto-place/status        -> armed, mode, stake, caps, blocked_reason
//   POST /api/user/auto-place-prefs    -> {mode, stake, daily_cap, consent}
// The server treats a missing consent timestamp as mode='off' no matter what
// the mode column says, so `consent: true` MUST accompany any arming write.
function AutoPlacePanel({ onClose }) {
  const api = window.cpApi;
  const [status, setStatus] = useState(null);
  const [mode, setMode] = useState("off");
  const [stake, setStake] = useState(1);
  const [cap, setCap] = useState(25);
  const [consent, setConsent] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const [okMsg, setOkMsg] = useState("");
  // Arming LIVE moves real money, so it takes a second deliberate press
  // rather than riding on the same click that selected the mode.
  const [confirmLive, setConfirmLive] = useState(false);

  const load = React.useCallback(() => {
    api.apiFetch("/api/auto-place/status")
      .then(st => {
        setStatus(st);
        setMode(st.mode || "off");
        if (typeof st.stake === "number" && st.stake > 0) setStake(st.stake);
        if (typeof st.daily_cap === "number") setCap(st.daily_cap);
        // Already armed ⇒ consent is on file; don't make them re-tick it.
        if (st.mode && st.mode !== "off") setConsent(true);
      })
      .catch(e => setErr(e.message || "Couldn't load auto-place status."));
  }, [api]);

  useEffect(() => { load(); }, [load]);

  const save = async (nextMode) => {
    if (busy) return;
    if (nextMode === "live" && !confirmLive) { setConfirmLive(true); return; }
    setBusy(true); setErr(""); setOkMsg("");
    try {
      await api.apiFetch("/api/user/auto-place-prefs", {
        method: "POST",
        body: {
          mode: nextMode,
          stake: Number(stake),
          daily_cap: Number(cap),
          // Only meaningful when arming; harmless when disarming.
          consent: nextMode === "off" ? undefined : true,
        },
      });
      setMode(nextMode);
      setConfirmLive(false);
      window.dispatchEvent(new CustomEvent("cp:auto-place-changed"));
      setOkMsg(nextMode === "off" ? "Auto-place disarmed."
             : nextMode === "paper" ? "Armed in paper mode — nothing is submitted."
             : "Armed LIVE. Real money will be staked.");
      load();
    } catch (e) {
      setErr((e.message || "Couldn't save.").replace(/^HTTP \d+:\s*/, ""));
    } finally { setBusy(false); }
  };

  const armed = status && status.armed;

  return (
    <div className="ap-modal-back" onClick={onClose}>
      <div className="ap-modal" onClick={e => e.stopPropagation()} role="dialog" aria-label="Auto-place settings">
        <div className="ap-hd">
          <h3>Auto-place</h3>
          <button className="ap-x" onClick={onClose} aria-label="Close">✕</button>
        </div>

        <div className={"ap-state " + (armed ? "is-on" : "")}>
          {status === null ? "Loading…"
            : armed ? `Armed · ${status.mode} · $${status.stake} per slip`
            : `Not armed${status.blocked_reason && status.blocked_reason !== "not armed"
                             ? " — " + status.blocked_reason : ""}`}
        </div>

        {status && status.spent_today !== null && (
          <div className="ap-sub">
            ${Number(status.spent_today).toFixed(2)} of ${Number(status.daily_cap).toFixed(2)} used today
            {status.fail_streak > 0 && ` · ${status.fail_streak} recent failure(s)`}
          </div>
        )}

        <div className="ap-field">
          <label>Stake per slip ($)</label>
          <input className="cp-input cp-input-sm" type="number" min="1" max="1000" step="1"
                 value={stake} onChange={e => setStake(e.target.value)} />
        </div>
        <div className="ap-field">
          <label>Daily cap ($)</label>
          <input className="cp-input cp-input-sm" type="number" min="0" max="10000" step="1"
                 value={cap} onChange={e => setCap(e.target.value)} />
        </div>

        <label className="ap-consent">
          <input type="checkbox" checked={consent} onChange={e => { setConsent(e.target.checked); setConfirmLive(false); }} />
          <span>
            I understand that arming <b>live</b> mode lets CoreProp submit real
            PrizePicks entries with my money, up to the caps above, without
            asking me each time.
          </span>
        </label>

        <div className="ap-actions">
          <button className="cp-btn cp-btn-sm" disabled={busy || mode === "off"}
                  onClick={() => save("off")}>Off</button>
          <button className="cp-btn cp-btn-sm" disabled={busy || !consent}
                  onClick={() => save("paper")}>Paper</button>
          <button className={"cp-btn cp-btn-sm " + (confirmLive ? "ap-danger" : "")}
                  disabled={busy || !consent}
                  onClick={() => save("live")}>
            {confirmLive ? "Tap again to arm LIVE" : "Live"}
          </button>
        </div>

        <p className="ap-note">
          Placement runs in the CoreProp browser extension on a desktop browser
          signed in to PrizePicks — arming here does nothing on its own.
          Paper mode stages and logs without submitting.
        </p>

        {err && <div className="ap-err">{err}</div>}
        {okMsg && <div className="ap-ok">{okMsg}</div>}
      </div>
    </div>
  );
}

// Prominent inline auto-place control. Shared so the +EV tab and the Backtest
// page show the same state — two copies drifting apart on a real-money switch
// is exactly the bug you do not want.
//
// Arms INLINE rather than opening the modal: consent + mode in one place, so
// turning it on is a couple of taps on a phone instead of a hunt through a
// dropdown. The modal (AutoPlacePanel) remains for stake/cap editing.
function AutoPlaceBar() {
  const api = window.cpApi;
  const [st, setSt] = useState(null);
  const [consent, setConsent] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const [confirmLive, setConfirmLive] = useState(false);

  const load = React.useCallback(() => {
    if (!api || !api.isLoggedIn()) return;
    api.apiFetch("/api/auto-place/status")
      .then(r => { setSt(r); if (r.mode && r.mode !== "off") setConsent(true); })
      .catch(() => {});
  }, [api]);

  useEffect(() => {
    load();
    window.addEventListener("cp:auto-place-changed", load);
    return () => window.removeEventListener("cp:auto-place-changed", load);
  }, [load]);

  if (!st) return null;

  const arm = async (mode) => {
    if (busy) return;
    if (mode === "live" && !confirmLive) { setConfirmLive(true); return; }
    setBusy(true); setErr("");
    try {
      await api.apiFetch("/api/user/auto-place-prefs", {
        method: "POST",
        body: { mode, consent: mode === "off" ? undefined : true },
      });
      setConfirmLive(false);
      window.dispatchEvent(new CustomEvent("cp:auto-place-changed"));
      load();
    } catch (e) {
      setErr((e.message || "Couldn't change auto-place.").replace(/^HTTP \d+:\s*/, ""));
    } finally { setBusy(false); }
  };

  const armed = st.armed;
  return (
    <div className={"ap-bar " + (armed ? "is-armed" : "")}>
      <div className="ap-bar-main">
        <span className="ap-bar-dot" aria-hidden="true" />
        <div className="ap-bar-txt">
          <div className="ap-bar-t">
            Auto-place {armed ? <b>{st.mode === "live" ? "LIVE" : "paper"}</b> : <b>off</b>}
          </div>
          <div className="ap-bar-s">
            {armed
              ? <>${st.stake}/slip{st.spent_today != null && <> · ${Number(st.spent_today).toFixed(2)} of ${Number(st.daily_cap).toFixed(2)} today</>}</>
              : "Places your auto-backtested slips on PrizePicks for you."}
          </div>
        </div>
        <button className="ap-bar-cfg" onClick={() => window.dispatchEvent(new CustomEvent("cp:open-auto-place"))}
                title="Stake, daily cap and full status">⚙</button>
      </div>

      {!armed && (
        <label className="ap-bar-consent">
          <input type="checkbox" checked={consent}
                 onChange={e => { setConsent(e.target.checked); setConfirmLive(false); }} />
          <span>Let CoreProp submit entries for me, up to ${st.daily_cap}/day at ${st.stake} a slip.</span>
        </label>
      )}

      <div className="ap-bar-actions">
        {armed ? (
          <button className="ap-bar-btn ap-bar-off" disabled={busy} onClick={() => arm("off")}>
            Turn off
          </button>
        ) : (
          <>
            <button className="ap-bar-btn" disabled={busy || !consent} onClick={() => arm("paper")}>
              Paper
            </button>
            <button className={"ap-bar-btn ap-bar-live " + (confirmLive ? "is-confirm" : "")}
                    disabled={busy || !consent} onClick={() => arm("live")}>
              {confirmLive ? "Tap again — real money" : "Go live"}
            </button>
          </>
        )}
      </div>
      {err && <div className="ap-bar-err">{err}</div>}
    </div>
  );
}

function TopNav({ active, onTab, onLogin, loggedIn, onLogout, variant = "app", locked = false }) {
  // Signed-out visitors on the landing page get no app tabs. All six used to
  // render for everyone: they look like navigation, but every click just
  // reopened the auth modal (app-main.jsx onTab bails when !loggedIn), and on a
  // phone they pushed the nav to a second, horizontally-scrolling row that
  // covered a third of the first screen. The landing page has its own in-page
  // nav in the footer.
  const showTabs = loggedIn;
  // Logo destination: signed-in users land on +EV Bets; signed-out goes to
  // the marketing landing page regardless of which tab they were viewing.
  // A LOCKED user (billing enforced, no active sub) can't enter +EV Bets —
  // onTab's locked-guard bounces "+EV Bets" back to pricing, making the logo a
  // dead no-op on the pay screen. Route them to "landing" instead (onTab
  // short-circuits "landing" ABOVE the locked guard), so the logo is a working
  // "home" affordance for exactly the users who most need an escape.
  const logoDest = (loggedIn && !locked) ? "+EV Bets" : "landing";
  const [busy, setBusy] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const [autoPlaceOpen, setAutoPlaceOpen] = useState(false);
  const menuRef = useRef(null);
  const user = (window.cpApi && window.cpApi.getUser && window.cpApi.getUser()) || null;
  const email = user?.email || "";
  const displayName = (user?.user_metadata?.username || email || "Account");
  const initial = (email || user?.user_metadata?.username || "?").trim().charAt(0).toUpperCase();

  const handleLogout = async () => {
    if (busy) return;
    setBusy(true);
    try { await (onLogout && onLogout()); }
    finally { setBusy(false); setMenuOpen(false); }
  };

  // Close the profile dropdown on any outside click or Escape.
  // Any page can ask for the auto-place panel (the Backtest page's strip does),
  // so the control is not reachable only through the avatar dropdown.
  useEffect(() => {
    const open = () => setAutoPlaceOpen(true);
    window.addEventListener("cp:open-auto-place", open);
    return () => window.removeEventListener("cp:open-auto-place", open);
  }, []);

  useEffect(() => {
    if (!menuOpen) return;
    const onDown = (e) => { if (menuRef.current && !menuRef.current.contains(e.target)) setMenuOpen(false); };
    const onKey = (e) => { if (e.key === "Escape") setMenuOpen(false); };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => { document.removeEventListener("mousedown", onDown); document.removeEventListener("keydown", onKey); };
  }, [menuOpen]);

  return (
    <>
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
              <span className="cp-avatar">{initial}</span>
            </button>
            {menuOpen && (
              <div className="cp-menu" role="menu">
                <div className="cp-menu-hd">
                  <div className="cp-avatar cp-avatar-lg">{initial}</div>
                  <div className="cp-menu-id">
                    <div className="cp-menu-name">{displayName}</div>
                    {email && <div className="cp-menu-email">{email}</div>}
                  </div>
                </div>
                <div className="cp-menu-sep" />
                <PushToggle />
                <button className="cp-menu-item" role="menuitem"
                        onClick={() => { setMenuOpen(false); setAutoPlaceOpen(true); }}>
                  Auto-place…
                </button>
                {/* `is-danger` is scoped to sign-out ON PURPOSE. The destructive
                    hover used to be a blanket `.cp-menu-item:hover` rule, which
                    painted the push toggle — and now would paint this
                    Auto-place item — red on hover. Do not add it here: arming
                    auto-place is consequential, but it is a navigation item, and
                    the confirmation for real money lives in the panel itself. */}
                <button className="cp-menu-item is-danger" role="menuitem" onClick={handleLogout} disabled={busy}>
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
            <button className="cp-btn cp-btn-ghost cp-btn-sm" onClick={onLogin}>Log in</button>
            <button className="cp-btn cp-btn-primary cp-btn-sm" onClick={() => onTab && onTab("pricing")}>
              See pricing
            </button>
          </>
        )}
      </div>
    </header>
    {autoPlaceOpen && <AutoPlacePanel onClose={() => setAutoPlaceOpen(false)} />}
    </>
  );
}

// ───────── Auth Modal ─────────
function AuthModal({ open, onClose, onSubmit }) {
  const [mode, setMode] = useState("login");
  const [email, setEmail] = useState("");
  const [pw, setPw] = useState("");
  const [pw2, setPw2] = useState("");
  const [err, setErr] = useState("");
  const [busy, setBusy] = useState(false);
  const dialogRef = useRef(null);
  const firstFieldRef = useRef(null);

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
          setErr("Check your email for a confirmation link, then log in.");
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
        <div className="cp-seg">
          <button type="button" className={"cp-seg-btn " + (mode === "login" ? "is-active" : "")} onClick={() => { setMode("login"); setErr(""); }}>Log in</button>
          <button type="button" className={"cp-seg-btn " + (mode === "signup" ? "is-active" : "")} onClick={() => { setMode("signup"); setErr(""); }}>Sign up</button>
        </div>
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
      </div>
    </div>
  );
}

// ───────── Sportsbook badge ─────────
function BookBadge({ book, odds }) {
  // `fg` lands in a React inline style, where a var() is just a string — the same
  // thing page-analytics.jsx relies on for stroke="var(--hair)". So the two rows
  // whose colour has a token in :root read it instead of restating it; NV's
  // #5EEAD4 and MGM's #7DD3FC have no token, and FD's #FCA5A5 is --red-2 spelled
  // out at five more inline sites across this file, ev-page.jsx and landing.jsx —
  // that one is a set, and migrating a third of a set is how --red-2's family got
  // half-done the first time (see FAMILY in tests/api_tests/test_css_tokens.py).
  const colors = {
    FD:  { bg: "rgba(239,68,68,.16)",  fg: "#FCA5A5" },
    DK:  { bg: "rgba(34,197,94,.16)",  fg: "var(--green-2)" },
    PIN: { bg: "rgba(250,204,21,.18)", fg: "var(--amber-2)" },
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
  // 50–76 → green ramp. The domain is measured, not chosen: over 950 live bets
  // the values run p0=50.0 / p5=50.4 / p50=61.6 / p95=76.0 / p100=83.8, so the
  // old 54–72 domain clamped 45.7% of rows to one of two identical colours
  // (30.1% at the low end, 15.6% at the high). p5→p95 rounded to 50–76 cuts that
  // to 6.6%, so the ramp now actually encodes instead of flattening half the
  // board. Phase 2a promoted this number to a 20px hero, which is what made the
  // clamping worth fixing.
  //
  // The hue and the lightness/chroma expressions are deliberately UNCHANGED —
  // only the domain moved. The end-to-end span is 22/255 either way; that is the
  // ramp's designed intensity, and widening it would be a colour-scheme change
  // rather than a fix to the mapping. This is a value→color encoding: never
  // replace it with a flat token (see the .cp-tp note in index.html).
  const t = Math.min(1, Math.max(0, (value - 50) / 26));
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

Object.assign(window, {
  Logo, TopNav, AuthModal, BookBadge, TruePct, LeaguePill, AnimatedNumber,
  NAV_TABS, useCoverage, fmtRefresh,
});
