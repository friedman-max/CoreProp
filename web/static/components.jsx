// Shared components: Logo, TopNav, AuthModal, badges, etc.

const { useState, useEffect, useRef, useMemo } = React;

// ───────── Logo ─────────
function Logo({ size = 32, animated = true }) {
  // The PNG already includes the wordmark, so we just render it.
  const h = size;
  return (
    <div className="cp-logo">
      <img
        src="logo-transparent.png"
        alt="CoreProp"
        className={"cp-mark-img " + (animated ? "is-spin" : "")}
        style={{ height: h + "px", width: "auto" }}
      />
    </div>
  );
}

// ───────── Top Nav ─────────
const NAV_TABS = ["+EV Bets", "Combined Lines", "PrizePicks Lines", "Sportsbooks", "Backtest", "Analytics", "Observatory", "Sandbox"];

function TopNav({ active, onTab, onLogin, loggedIn, onLogout, variant = "app" }) {
  return (
    <header className={"cp-nav " + (variant === "landing" ? "is-landing" : "")}>
      <div className="cp-nav-l">
        <button className="cp-logo-btn" onClick={() => onTab && onTab(variant === "landing" ? "landing" : "+EV Bets")}>
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
            <div className="cp-avatar">M</div>
            <button className="cp-link" onClick={onLogout}>Log Out</button>
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
  if (!open) return null;
  return (
    <div className="cp-modal-back" onClick={onClose}>
      <div className="cp-modal" onClick={e => e.stopPropagation()}>
        <button className="cp-modal-x" onClick={onClose} aria-label="Close">✕</button>
        <div className="cp-modal-logo">
          <Logo size={56} withWord />
        </div>
        <div className="cp-seg">
          <button className={"cp-seg-btn " + (mode === "login" ? "is-active" : "")} onClick={() => setMode("login")}>Log In</button>
          <button className={"cp-seg-btn " + (mode === "signup" ? "is-active" : "")} onClick={() => setMode("signup")}>Sign Up</button>
        </div>
        <form className="cp-form" onSubmit={e => { e.preventDefault(); onSubmit && onSubmit({ mode, email, pw }); }}>
          <input className="cp-input" placeholder="Email" type="email" value={email} onChange={e => setEmail(e.target.value)} />
          <input className="cp-input" placeholder="Password" type="password" value={pw} onChange={e => setPw(e.target.value)} />
          {mode === "signup" && <input className="cp-input" placeholder="Confirm password" type="password" />}
          <button className="cp-btn cp-btn-primary cp-btn-lg" type="submit">{mode === "login" ? "Log In" : "Create Account"}</button>
          {mode === "login" && <button type="button" className="cp-link cp-center">Forgot password?</button>}
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
