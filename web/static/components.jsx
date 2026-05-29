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
        src="/static/logo_full.png"
        alt="CoreProp"
        className={"cp-mark-img " + (animated ? "is-spin" : "")}
        style={{ height: h + "px", width: "auto" }}
        onError={(e) => {
          if (!e.currentTarget.dataset.fallback) {
            e.currentTarget.dataset.fallback = "1";
            e.currentTarget.src = "/static/logo.png";
          }
        }}
      />
    </div>
  );
}

// ───────── Top Nav ─────────
const NAV_TABS = ["+EV Bets", "Combined Lines", "PrizePicks Lines", "Sportsbooks", "Backtest", "Analytics", "Observatory", "Sandbox"];

function TopNav({ active, onTab, onLogin, loggedIn, onLogout, variant = "app" }) {
  // Logo destination: signed-in users land on +EV Bets; signed-out goes to
  // the marketing landing page regardless of which tab they were viewing.
  const logoDest = loggedIn ? "+EV Bets" : "landing";
  const [busy, setBusy] = useState(false);
  const user = (window.cpApi && window.cpApi.getUser && window.cpApi.getUser()) || null;
  const initial = (user && (user.email || user.user_metadata?.username) || "?").trim().charAt(0).toUpperCase();

  const handleLogout = async () => {
    if (busy) return;
    setBusy(true);
    try { await (onLogout && onLogout()); }
    finally { setBusy(false); }
  };

  return (
    <header className={"cp-nav " + (variant === "landing" ? "is-landing" : "")}>
      <div className="cp-nav-l">
        <button className="cp-logo-btn" onClick={() => onTab && onTab(logoDest)} aria-label="Home">
          <Logo />
        </button>
      </div>
      <nav className="cp-nav-c">
        {NAV_TABS.map(t => (
          <button
            key={t}
            className={"cp-tab " + (active === t ? "is-active" : "")}
            onClick={() => onTab && onTab(t)}
          >{t}</button>
        ))}
      </nav>
      <div className="cp-nav-r">
        {loggedIn ? (
          <div className="cp-user">
            <div className="cp-avatar" title={user?.email || ""}>{initial}</div>
            <button className="cp-link" onClick={handleLogout} disabled={busy}>{busy ? "…" : "Log Out"}</button>
          </div>
        ) : (
          <button className="cp-btn cp-btn-primary cp-btn-sm" onClick={onLogin}>Log In</button>
        )}
      </div>
    </header>
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
  if (!open) return null;

  const submit = async (e) => {
    e.preventDefault();
    setErr("");
    if (!email || !pw) { setErr("Email and password are required."); return; }
    if (mode === "signup" && pw !== pw2) { setErr("Passwords do not match."); return; }
    setBusy(true);
    try {
      if (mode === "login") {
        await window.cpApi.signIn(email, pw);
      } else {
        const res = await window.cpApi.signUp(email, pw);
        if (!res.session) {
          setErr("Check your email to confirm your account, then log in.");
          setBusy(false);
          return;
        }
      }
      onSubmit && onSubmit({ mode, email });
    } catch (ex) {
      setErr(ex.message || "Authentication failed.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="cp-modal-back" onClick={onClose}>
      <div className="cp-modal" onClick={e => e.stopPropagation()}>
        <button className="cp-modal-x" onClick={onClose} aria-label="Close">✕</button>
        <div className="cp-modal-logo">
          <Logo size={56} withWord />
        </div>
        <div className="cp-seg">
          <button type="button" className={"cp-seg-btn " + (mode === "login" ? "is-active" : "")} onClick={() => { setMode("login"); setErr(""); }}>Log In</button>
          <button type="button" className={"cp-seg-btn " + (mode === "signup" ? "is-active" : "")} onClick={() => { setMode("signup"); setErr(""); }}>Sign Up</button>
        </div>
        <form className="cp-form" onSubmit={submit}>
          <input className="cp-input" placeholder="Email" type="email" autoComplete="email" value={email} onChange={e => setEmail(e.target.value)} disabled={busy} />
          <input className="cp-input" placeholder="Password" type="password" autoComplete={mode === "login" ? "current-password" : "new-password"} value={pw} onChange={e => setPw(e.target.value)} disabled={busy} />
          {mode === "signup" && <input className="cp-input" placeholder="Confirm password" type="password" autoComplete="new-password" value={pw2} onChange={e => setPw2(e.target.value)} disabled={busy} />}
          {err && <div style={{color:"#FCA5A5", fontSize:13, padding:"2px 4px"}}>{err}</div>}
          <button className="cp-btn cp-btn-primary cp-btn-lg" type="submit" disabled={busy}>
            {busy ? "…" : (mode === "login" ? "Log In" : "Create Account")}
          </button>
        </form>
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
  const colors = {
    NBA:  "#F97316",
    NHL:  "#60A5FA",
    MLB:  "#34D399",
    WNBA: "#C084FC",
    NFL:  "#FBBF24",
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

Object.assign(window, { Logo, TopNav, AuthModal, BookBadge, TruePct, LeaguePill, AnimatedNumber, NAV_TABS });
