// +EV Bets page — wired to /api/bootstrap/core.
const { useState: useStateE, useMemo: useMemoE } = React;

function fmtGameTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleDateString([], { month: "numeric", day: "numeric" }) +
    " " + d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
}

// Slip-level EV% per 1u stake. Mirrors page-backtest.jsx btSlipEvPct and the
// backend engine/ev_calculator.py (Power = product×payout−1; Flex = expected
// payout over the Poisson-binomial hit-count distribution). Payout tables
// match engine/constants.py. Returns null for <2 legs / unknown leg count.
// KEEP IN SYNC with engine/constants.py POWER_PAYOUTS / FLEX_PAYOUTS. The
// 6-pick Power pays 37.5x (PrizePicks lowered it from 40x).
const EV_POWER_PAYOUTS = { 2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 37.5 };
const EV_FLEX_PAYOUTS = {
  3: { 2: 1.0, 3: 3.0 },
  4: { 3: 1.5, 4: 6.0 },
  5: { 3: 0.4, 4: 2.0, 5: 10.0 },
  6: { 4: 0.4, 5: 2.0, 6: 25.0 },
};
function slipEvPct(slipTypeRaw, legs) {
  const probs = (legs || []).map(l => Math.max(0, Math.min(1, (l.truePct || 0) / 100)));
  const n = probs.length;
  if (n < 2) return null;
  const slipType = String(slipTypeRaw || "power").toLowerCase();
  if (slipType === "power") {
    const pay = EV_POWER_PAYOUTS[n];
    if (!pay) return null;
    return (probs.reduce((a, p) => a * p, 1) * pay - 1) * 100;
  }
  // Flex: distribution over exact hit counts (Poisson-binomial).
  let dist = [1];
  for (const p of probs) {
    const next = new Array(dist.length + 1).fill(0);
    for (let k = 0; k < dist.length; k++) {
      next[k]     += dist[k] * (1 - p);
      next[k + 1] += dist[k] * p;
    }
    dist = next;
  }
  if (n === 2) return (dist[2] * EV_POWER_PAYOUTS[2] - 1) * 100;
  const table = EV_FLEX_PAYOUTS[n];
  if (!table) return null;
  let expected = 0;
  for (let k = 0; k < dist.length; k++) expected += dist[k] * (table[k] || 0);
  return (expected - 1) * 100;
}

// Per-leg break-even % for an n-leg slip of a given type, DERIVED from the
// payout tables above (not a hand-typed table that drifts out of sync).
//   Power: every leg must hit, so E[payout] = p^n · payout(n). Break-even is
//          p = (1/payout)^(1/n)  → closed form.
//   Flex:  partial payouts, so E[payout](p) is monotincreasing in p; bisect
//          for the p where the Poisson-binomial expected payout == 1.
// Matches engine/constants.py BREAK_EVEN exactly (e.g. Power 2 = 57.74%,
// Power 4 = 56.23%, Flex 5 = 54.25%). Returns a percentage (0–100).
function slipBreakEvenPct(slipTypeRaw, n) {
  const slipType = String(slipTypeRaw || "power").toLowerCase();
  if (!n || n < 2) return null;
  if (slipType === "power" || n === 2) {
    const pay = EV_POWER_PAYOUTS[n];
    if (!pay) return null;
    return Math.pow(1 / pay, 1 / n) * 100;
  }
  const table = EV_FLEX_PAYOUTS[n];
  if (!table) return null;
  const evAt = (p) => {
    // Expected payout for n independent legs each at prob p (Poisson-binomial
    // collapses to the binomial when every leg shares the same p).
    let dist = [1];
    for (let i = 0; i < n; i++) {
      const nx = new Array(dist.length + 1).fill(0);
      for (let k = 0; k < dist.length; k++) { nx[k] += dist[k] * (1 - p); nx[k + 1] += dist[k] * p; }
      dist = nx;
    }
    let e = 0;
    for (let k = 0; k < dist.length; k++) e += dist[k] * (table[k] || 0);
    return e;
  };
  let lo = 0, hi = 1;
  for (let i = 0; i < 60; i++) { const mid = (lo + hi) / 2; if (evAt(mid) < 1) lo = mid; else hi = mid; }
  return ((lo + hi) / 2) * 100;
}

function EVPage() {
  const [league, setLeague] = useState("All");
  const [propQ, setPropQ] = useState("");
  const [minOdds, setMinOdds] = useState(50);
  const [side, setSide] = useState("Both");
  // Group already-logged bets to the bottom (unlogged/actionable first) while
  // keeping them visible — complements the row tint, independent of any hide.
  const [loggedLast, setLoggedLast] = useState(false);
  const [slipType, setSlipType] = useState("Power");
  const [legs, setLegs] = useState(6);
  const [selected, setSelected] = useState([]);
  const [hovered, setHovered] = useState(null);
  // Mobile-only: the slip builder is hidden by default and revealed via a
  // "Slip Builder" toggle in the filter bar. On desktop the aside is always
  // shown (the class is a no-op there — see the .ev-slip mobile CSS).
  const [slipOpen, setSlipOpen] = useState(false);
  const [allBets, setAllBets] = useState([]);
  const [loadState, setLoadState] = useState("loading"); // loading | ok | error
  const [errMsg, setErrMsg] = useState("");
  // Server-side data-age from /api/bootstrap/core, so the meta bar shows honest
  // freshness: the pipeline preserves the previous snapshot on a failed scrape,
  // so an unconditional "live" pill over hours-old odds would be misleading.
  const [lastRefresh, setLastRefresh] = useState(null);   // ISO string or null
  const [intervalMin, setIntervalMin] = useState(5);
  // Ticks frequently so the relative data-age (and the "live" pulse) updates as
  // the snapshot ages, without waiting on the next 30s data fetch.
  const [nowTick, setNowTick] = useState(() => Date.now());
  const [saving, setSaving] = useState(false);
  const [autoBacktest, setAutoBacktest] = useState(false);
  // Green devils (PrizePicks goblins) — discounted, higher-hit-rate lines.
  // showGreenDevils switches the table to a green-devil-only view ranked by
  // P(hit) ("best bets" for bonus maximization). autoBacktestGreenDevils opts
  // them into auto-backtest (as their own separate slip).
  const [showGreenDevils, setShowGreenDevils] = useState(false);
  const [autoBacktestGreenDevils, setAutoBacktestGreenDevils] = useState(false);
  // User-overridable minimum True % per leg. `null` means "use computed BE
  // for the current (slipType, legs) combo"; a number overrides.
  const [minLegOverride, setMinLegOverride] = useState(null);
  // Slip-prefs save state — "idle" | "saving" | "saved" | "error"
  // for the explicit Save button next to Min Leg %.
  const [prefsSaveState, setPrefsSaveState] = useState("idle");
  // True until /api/config has hydrated this user's saved prefs, so the
  // "unsaved → idle" effect below doesn't reset state on initial load.
  const prefsHydrated = React.useRef(false);
  // True only once /api/config has answered. While false the Save button
  // shows a loading state instead of "Save preferences".
  const [prefsLoaded, setPrefsLoaded] = useState(false);

  React.useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        // Bets and the user's logged-bet-key list are fetched in parallel.
        // /api/backtest/keys returns { keys: ["player|YYYY-MM-DD", ...] };
        // we join each bet locally by its bet_key. /api/backtest/keys
        // requires auth — if the user is signed out we skip it and no
        // rows get the LOGGED treatment.
        const isAuthed = window.cpApi && window.cpApi.isLoggedIn();
        const [coreRes, keysRes] = await Promise.allSettled([
          window.cpApi.apiFetch("/api/bootstrap/core"),
          isAuthed ? window.cpApi.apiFetch("/api/backtest/keys") : Promise.resolve({ keys: [] }),
        ]);
        if (cancelled) return;
        if (coreRes.status !== "fulfilled") throw coreRes.reason;
        const data = coreRes.value;
        const loggedKeys = new Set(
          (keysRes.status === "fulfilled" ? (keysRes.value.keys || []) : [])
        );
        const ui = (data.bets || []).map(b => {
          const row = window.cpApi.betToUi(b);
          if (row.betKey && loggedKeys.has(row.betKey)) row.inBacktest = true;
          return row;
        });
        setAllBets(ui);
        if (data.last_refresh) setLastRefresh(data.last_refresh);
        if (typeof data.interval_min === "number") setIntervalMin(data.interval_min);
        setLoadState("ok");
      } catch (ex) {
        if (cancelled) return;
        setErrMsg(ex.message || "Failed to load bets.");
        setLoadState("error");
      }
    };
    load();
    const id = setInterval(load, 30000); // refresh every 30s
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  // Recompute relative data-age every 5s so the pill (and the "live" pulse)
  // transitions promptly as the snapshot ages — even if a scrape cycle fails
  // and last_refresh stops advancing — instead of lagging the 30s data fetch.
  React.useEffect(() => {
    const id = setInterval(() => setNowTick(Date.now()), 5000);
    return () => clearInterval(id);
  }, []);

  // Freshness pill model. "live" only while the data is genuinely current
  // (under one scrape interval); amber once it's older than an interval, red at
  // 3x (a stuck/failed pipeline).
  const freshness = useMemoE(() => {
    if (!lastRefresh) return null;
    const ageMs = Math.max(0, nowTick - new Date(lastRefresh).getTime());
    const ageMin = ageMs / 60000;
    const label = ageMin < 1 ? "just now"
      : ageMin < 60 ? `${Math.round(ageMin)}m old`
      : `${Math.floor(ageMin / 60)}h ${Math.round(ageMin % 60)}m old`;
    const intv = intervalMin || 5;
    const level = ageMin > intv * 3 ? "stale" : ageMin > intv + 1 ? "aging" : "fresh";
    // "live" pulse only while the snapshot is brand-new ("just now"); it drops
    // the moment the data reads "1m old" so the badge tracks reality.
    const isLive = ageMin < 1;
    return { label, level, isLive };
  }, [lastRefresh, nowTick, intervalMin]);

  // Load the user's saved prefs from /api/config on mount so values
  // persist across reloads / devices for that account. Once hydrated, the
  // Save button stops surfacing "idle" until the user edits a field.
  React.useEffect(() => {
    if (!window.cpApi || !window.cpApi.isLoggedIn()) {
      setPrefsLoaded(true);
      prefsHydrated.current = true;
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const cfg = await window.cpApi.apiFetch("/api/config");
        if (cancelled) return;
        if (cfg.auto_slip_type === "Power" || cfg.auto_slip_type === "Flex") {
          setSlipType(cfg.auto_slip_type);
        }
        if (typeof cfg.auto_slip_legs === "number" && cfg.auto_slip_legs >= 2 && cfg.auto_slip_legs <= 7) {
          setLegs(cfg.auto_slip_legs);
        }
        if (typeof cfg.auto_slip_min_prob === "number" && cfg.auto_slip_min_prob > 0 && cfg.auto_slip_min_prob < 1) {
          // Round to 2 dp: 0.57 * 100 is 56.99999999999999 in float, which
          // would otherwise show as "56.999999%" in the Min Leg % input on
          // reload. The input uses step="0.1", so 2 dp loses no real precision.
          setMinLegOverride(Math.round(cfg.auto_slip_min_prob * 10000) / 100);
        }
        if (typeof cfg.auto_backtest === "boolean") {
          setAutoBacktest(cfg.auto_backtest);
        }
        if (typeof cfg.auto_backtest_green_devils === "boolean") {
          setAutoBacktestGreenDevils(cfg.auto_backtest_green_devils);
        }
      } catch (err) {
        console.warn("prefs load failed:", err.message);
      } finally {
        if (!cancelled) {
          setPrefsLoaded(true);
          // Mark hydration complete *after* the state setters above flush,
          // so the "mark dirty on change" effect ignores the initial loads.
          setTimeout(() => { prefsHydrated.current = true; }, 0);
        }
      }
    })();
    return () => { cancelled = true; };
  }, []);

  // Persist auto-backtest toggle whenever the user flips it. Skip during
  // hydration so the value pulled from /api/config doesn't immediately
  // round-trip back.
  React.useEffect(() => {
    if (!prefsHydrated.current) return;
    if (!window.cpApi || !window.cpApi.isLoggedIn()) return;
    window.cpApi.apiFetch("/api/user/auto-backtest", {
      method: "POST",
      body: { auto_backtest: autoBacktest },
    }).catch(err => console.warn("auto-backtest save failed:", err.message));
  }, [autoBacktest]);

  // Persist the "auto-backtest green devils" opt-in whenever it changes.
  // Sent via slip-prefs (the endpoint that owns auto_backtest_green_devils),
  // alongside the current slip prefs so the row stays self-consistent.
  React.useEffect(() => {
    if (!prefsHydrated.current) return;
    if (!window.cpApi || !window.cpApi.isLoggedIn()) return;
    window.cpApi.apiFetch("/api/user/slip-prefs", {
      method: "POST",
      body: slipPrefsBody(),
    }).catch(err => console.warn("green-devil pref save failed:", err.message));
  }, [autoBacktestGreenDevils]);

  // Whenever slipType / legs / minLegOverride changes, mark prefs as
  // unsaved so the Save button surfaces "Save preferences" again. Skip
  // during the initial hydration pass so loaded values don't show as
  // dirty before the user touches anything.
  React.useEffect(() => {
    if (!prefsHydrated.current) return;
    setPrefsSaveState("idle");
  }, [slipType, legs, minLegOverride]);

  // Build the /api/user/slip-prefs body, clamping to what the backend accepts:
  // auto_slip_legs must be 2-6 (the builder also offers 7 for live EV calc, but
  // 7 is not a persistable pref) and Flex requires >= 3 legs. Sending an
  // out-of-range value 400s and the pref silently never saves.
  const slipPrefsBody = () => {
    const minPct = (typeof minLegOverride === "number") ? minLegOverride : slipBE;
    let saveLegs = Math.max(2, Math.min(6, legs));
    if (slipType === "Flex") saveLegs = Math.max(3, saveLegs);
    return {
      auto_slip_type: slipType,
      auto_slip_legs: saveLegs,
      auto_slip_min_prob: Math.max(0.01, Math.min(0.99, minPct / 100)),
      auto_backtest_green_devils: autoBacktestGreenDevils,
    };
  };

  const saveSlipPrefs = async () => {
    if (!window.cpApi || !window.cpApi.isLoggedIn()) return;
    setPrefsSaveState("saving");
    try {
      await window.cpApi.apiFetch("/api/user/slip-prefs", {
        method: "POST",
        body: slipPrefsBody(),
      });
      setPrefsSaveState("saved");
      setTimeout(() => setPrefsSaveState(s => s === "saved" ? "idle" : s), 2500);
    } catch (ex) {
      console.warn("slip-prefs save failed:", ex.message);
      setPrefsSaveState("error");
      setTimeout(() => setPrefsSaveState(s => s === "error" ? "idle" : s), 3000);
    }
  };

  const bets = useMemoE(() => {
    return allBets.filter(b => {
      // Mode switch: green-devil view shows ONLY green devils; the default
      // +EV view hides them. Both are sorted by P(hit) desc below, so the
      // safest / "best" bets float to the top in either mode.
      if (showGreenDevils ? !b.isGreenDevil : b.isGreenDevil) return false;
      if (league !== "All" && b.league !== league) return false;
      if (propQ && !(b.prop || "").toLowerCase().includes(propQ.toLowerCase())) return false;
      if (side !== "Both" && b.side !== side.toUpperCase()) return false;
      if ((b.truePct || 0) < minOdds) return false;
      return true;
    }).sort((a, b) => {
      // Optional first key: sink logged (already-in-backtest) rows to the
      // bottom so unlogged, actionable bets are on top.
      if (loggedLast && !!a.inBacktest !== !!b.inBacktest) return a.inBacktest ? 1 : -1;
      return (b.truePct || 0) - (a.truePct || 0);
    });
  }, [allBets, league, propQ, minOdds, side, showGreenDevils, loggedLast]);

  const greenDevilCount = useMemoE(
    () => allBets.filter(b => b.isGreenDevil).length, [allBets]);

  const toggleBet = (b) => {
    const key = b.id || (b.player + b.prop + b.line);
    setSelected(prev => prev.find(p => p.key === key) ? prev.filter(p => p.key !== key) : [...prev, { ...b, key }]);
  };

  const saveSlip = async () => {
    if (selected.length < 2 || selected.length > 6) return;

    // Enforce the user's Min Leg % before logging. Every leg in a saved slip
    // must clear the threshold (override if set, else the slip's break-even),
    // so a logged slip is always one the user would actually want backtested.
    const effMin = (typeof minLegOverride === "number") ? minLegOverride : slipBE;
    const below = selected.filter(s => (s.truePct || 0) < effMin - 1e-9);
    if (below.length) {
      alert(
        `Can't log this slip — ${below.length} leg(s) are below your Min Leg % (${effMin.toFixed(2)}%):\n` +
        below.map(s => `  • ${s.player} ${s.prop} — ${(s.truePct || 0).toFixed(1)}%`).join("\n") +
        `\n\nRaise the threshold or remove those legs.`
      );
      return;
    }

    setSaving(true);
    try {
      // /api/backtest/add-slip is the endpoint that actually WRITES the slip
      // (header + legs) to Supabase so it shows up on the Backtest tab.
      // (/api/slip only *computes* EV and persists nothing — using it was
      //  why nothing was being logged.) slip_type carries the user's choice
      // (Power / Flex) so the logged slip reflects their intent.
      await window.cpApi.apiFetch("/api/backtest/add-slip", {
        method: "POST",
        body: {
          bet_ids:   selected.map(s => s.id).filter(Boolean),
          slip_type: slipType,
        },
      });
      // Mark the just-logged bets as in-backtest locally so they turn red
      // immediately, before the next 30s poll re-joins /api/backtest/keys.
      const savedKeys = new Set(selected.map(s => s.betKey).filter(Boolean));
      if (savedKeys.size) {
        setAllBets(prev => prev.map(b =>
          (b.betKey && savedKeys.has(b.betKey)) ? { ...b, inBacktest: true } : b
        ));
      }
      setSelected([]);
      // Invalidate the cached backtest slips so the Backtest tab shows the
      // new slip immediately when the user switches to it.
      if (window.cpApi.cachedFetch) {
        window.cpApi.cachedFetch("/api/backtest/slips").catch(() => {});
        window.cpApi.cachedFetch("/api/backtest/keys").catch(() => {});
      }
    } catch (ex) {
      alert("Couldn't log slip to backtest: " + (ex.message || ex));
    } finally {
      setSaving(false);
    }
  };

  const slipBE = useMemoE(() => {
    // Real per-leg break-even from the payout tables. PrizePicks caps slips at
    // 6 legs (no 7-leg payout), so a 7-leg selection uses the 6-leg BE.
    const n = Math.min(6, Math.max(2, legs));
    const be = slipBreakEvenPct(slipType, n);
    return be == null ? 54.07 : be;
  }, [slipType, legs]);

  // Slip-builder body, shared by the desktop sidebar and the mobile dropdown
  // (rendered in two places, CSS decides which is visible). It closes over all
  // state directly, so both copies stay in sync automatically.
  const slipBody = (
    <React.Fragment>
        <div className="ev-slip-hd">
          <h3>Slip Builder</h3>
          <label className="ev-auto">
            <input type="checkbox" checked={autoBacktest} onChange={e => setAutoBacktest(e.target.checked)} />
            <span className="ev-check" aria-hidden="true" />
            Auto-Backtest
          </label>
          <label className="ev-auto ev-auto-gd" title="Also auto-backtest green devils — logged as their own separate slip, never mixed into +EV slips. Off = green devils are display-only.">
            <input type="checkbox" checked={autoBacktestGreenDevils} onChange={e => setAutoBacktestGreenDevils(e.target.checked)} />
            <span className="ev-check" aria-hidden="true" />
            <span style={{ color: "#22c55e" }}>Green devils</span>
          </label>
        </div>
        <div className="ev-slip-row">
          <div className="ev-slip-field">
            <label>Slip Type</label>
            <select value={slipType} onChange={e => setSlipType(e.target.value)} className="cp-input cp-input-sm">
              <option>Power</option>
              <option>Flex</option>
            </select>
          </div>
          <div className="ev-slip-field">
            <label>Legs</label>
            <select value={legs} onChange={e => setLegs(+e.target.value)} className="cp-input cp-input-sm">
              {[2,3,4,5,6,7].map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>
        </div>
        <div className="ev-slip-be">
          <div>
            <label>Min Leg %</label>
            <span className="ev-be-lbl">BE {slipBE.toFixed(2)}%</span>
          </div>
          <div className="ev-slip-be-row">
            <input
              className="cp-input cp-input-sm"
              type="number"
              step="0.1"
              min="0"
              max="100"
              // Until the account's saved prefs load, leave the field blank
              // with a "…" placeholder rather than showing the BE default
              // (54.07) as if it were the user's stored value.
              value={!prefsLoaded ? "" : (minLegOverride != null ? minLegOverride : slipBE.toFixed(2))}
              placeholder={prefsLoaded ? "" : "…"}
              disabled={!prefsLoaded}
              onChange={e => {
                const v = e.target.value;
                if (v === "") { setMinLegOverride(""); return; }
                const n = parseFloat(v);
                setMinLegOverride(isNaN(n) ? v : n);
              }}
              onBlur={() => {
                // Snap an empty / NaN value back to "use BE" (null = no override).
                if (minLegOverride === "" || typeof minLegOverride !== "number" || isNaN(minLegOverride)) {
                  setMinLegOverride(null);
                }
              }}
            />
            <button
              type="button"
              className="cp-btn cp-btn-ghost cp-btn-sm"
              onClick={() => setMinLegOverride(null)}
              disabled={minLegOverride == null}
            >Reset</button>
          </div>
          <button
            type="button"
            className={"ev-prefs-save ev-prefs-save-" + prefsSaveState}
            onClick={saveSlipPrefs}
            disabled={!prefsLoaded || prefsSaveState === "saving"}
          >
            {!prefsLoaded ? "Loading account…"
              : prefsSaveState === "saving" ? "Saving…"
              : prefsSaveState === "saved" ? "Saved ✓"
              : prefsSaveState === "error" ? "Retry save"
              : "Save preferences"}
          </button>
        </div>

        <div className="ev-slip-legs">
          {selected.length === 0 ? (
            <div className="ev-slip-empty">
              <div className="ev-empty-stack">
                <div className="ev-empty-slip" />
                <div className="ev-empty-slip" />
                <div className="ev-empty-slip" />
              </div>
              <p>Click rows to add bets to your slip.</p>
            </div>
          ) : (
            selected.map((s, i) => (
              <div key={s.key} className="ev-leg">
                <span className="ev-leg-i">{i+1}</span>
                <div className="ev-leg-body">
                  <div className="ev-leg-n">{s.player}</div>
                  <div className="ev-leg-p">{s.prop} {s.side === "OVER" ? "O" : "U"}{s.line}</div>
                </div>
                <span className="ev-leg-pct"><TruePct value={s.truePct} /></span>
                <button className="ev-leg-x" onClick={(e) => { e.stopPropagation(); setSelected(prev => prev.filter(p => p.key !== s.key)); }}>✕</button>
              </div>
            ))
          )}
        </div>

        {selected.length > 0 && (() => {
          const combinedHit = selected.reduce((a, b) => a * (b.truePct || 0) / 100, 1) * 100;
          const ev = slipEvPct(slipType, selected);   // true slip EV% (null if <2 legs)
          return (
            <div className="ev-slip-summary">
              <div className="ev-sum-row">
                <span>Combined hit %</span>
                <b>{combinedHit.toFixed(2)}%</b>
              </div>
              <div className="ev-sum-row">
                <span>Expected value</span>
                {/* Slip-level EV% per 1u stake (Power all-hit×payout−1, Flex
                 * Poisson-binomial), matching the Backtest tab. */}
                <b className={ev != null && ev >= 0 ? "ev-edge" : ""}
                   style={ev != null && ev < 0 ? { color: "#FCA5A5" } : undefined}>
                  {ev == null ? "—" : (ev >= 0 ? "+" : "") + ev.toFixed(2) + "%"}
                </b>
              </div>
            </div>
          );
        })()}

        <button
          className={"cp-btn cp-btn-save " + (selected.length < 2 || selected.length > 6 || saving ? "is-dis" : "")}
          disabled={selected.length < 2 || selected.length > 6 || saving}
          onClick={saveSlip}
        >
          {saving ? "Saving…" : (selected.length < 2 ? "Add 2+ legs" : selected.length > 6 ? "Max 6 legs" : "Save slip")}
        </button>
    </React.Fragment>
  );

  return (
    <main className="ev">
      <div className="ev-main">
        {/* Filter bar */}
        <div className="ev-filters">
          <div className="ev-filter">
            <label>League</label>
            <div className="ev-chips">
              {["All", "NBA", "WNBA", "NHL", "MLB", "NFL"].map(l => (
                <button key={l} className={"ev-chip " + (league === l ? "is-on" : "")} onClick={() => setLeague(l)}>{l}</button>
              ))}
            </div>
          </div>
          <div className="ev-filter ev-filter-grow">
            <label>Prop type</label>
            <input className="cp-input cp-input-sm" placeholder="Rebounds, Points, Hits Allowed…" value={propQ} onChange={e => setPropQ(e.target.value)} />
          </div>
          <div className="ev-filter">
            <label>Min True %</label>
            <div className="ev-stepper">
              <button onClick={() => setMinOdds(v => Math.max(50, v - 1))}>−</button>
              <span>{minOdds}</span>
              <button onClick={() => setMinOdds(v => Math.min(80, v + 1))}>+</button>
            </div>
          </div>
          <div className="ev-filter">
            <label>Side</label>
            <div className="ev-chips">
              {["Both", "Over", "Under"].map(s => (
                <button key={s} className={"ev-chip " + (side === s ? "is-on" : "")} onClick={() => setSide(s)}>{s}</button>
              ))}
            </div>
          </div>
          <div className="ev-filter">
            <label>Green Devils</label>
            <button
              type="button"
              className={"ev-chip " + (showGreenDevils ? "is-on" : "")}
              onClick={() => setShowGreenDevils(v => !v)}
              title="Green devils are PrizePicks goblins — discounted, higher-hit-rate lines. Toggle to see the safest 'just need to win' picks, ranked by hit probability."
              style={showGreenDevils
                ? { background: "#16a34a", borderColor: "#16a34a", color: "#fff" }
                : { borderColor: "#16a34a", color: "#22c55e" }}
            >{showGreenDevils ? "On" : "Off"}{greenDevilCount ? ` · ${greenDevilCount}` : ""}</button>
          </div>
          <div className="ev-filter">
            <label>Sort</label>
            <button
              type="button"
              className={"ev-chip " + (loggedLast ? "is-on" : "")}
              onClick={() => setLoggedLast(v => !v)}
              title="Sort unlogged bets first — logged (already in your backtest) bets sink to the bottom."
            >{loggedLast ? "Unlogged first" : "By true %"}</button>
          </div>
          <button className="ev-clear" onClick={() => { setLeague("All"); setPropQ(""); setMinOdds(50); setSide("Both"); setLoggedLast(false); }}>Clear</button>
          {/* Mobile-only: a full-width grey bar at the bottom of the config
              tile that reveals/hides the slip builder (hidden by default). The
              panel drops out directly beneath it, connected to the tile. */}
          <button
            type="button"
            className={"ev-slip-toggle " + (slipOpen ? "is-on" : "")}
            onClick={() => setSlipOpen(v => !v)}
            aria-expanded={slipOpen}
          >
            Slip Builder{selected.length ? ` · ${selected.length}` : ""}
            <span className={"ev-slip-toggle-caret " + (slipOpen ? "is-open" : "")}>▾</span>
          </button>
        </div>

        {/* Mobile slip-builder dropdown: rendered immediately after the config
            tile so it appears to fall out of it. Hidden on desktop (the sidebar
            aside shows instead) and until the toggle is opened. */}
        <div className={"ev-slip ev-slip-mobile " + (slipOpen ? "is-open" : "")}>
          {slipBody}
        </div>

        <div className="ev-meta">
          <span><b>{bets.length}</b> {showGreenDevils ? "green devils" : "bets"}</span>
          <span className="ev-meta-dot">·</span>
          <span>
            {loadState === "loading" && "Loading…"}
            {loadState === "ok" && (
              freshness
                ? <>
                    Data <span
                      className="ev-fresh"
                      style={{
                        color: freshness.level === "stale" ? "#FCA5A5"
                             : freshness.level === "aging" ? "#FCD34D"
                             : "#22c55e",
                        fontStyle: "normal", fontWeight: 600,
                      }}
                    >{freshness.label}</span>
                    {freshness.isLive && <em className="ev-pulse" style={{ marginLeft: 6 }}>live</em>}
                  </>
                : <>Updated <em className="ev-pulse">live</em></>
            )}
            {loadState === "error" && <span style={{color:"#FCA5A5"}}>Error: {errMsg}</span>}
          </span>
          {/* Red-row legend. Only rendered when at least one visible row is
              actually logged, so it explains the highlight exactly when the
              highlight is on screen. This replaces the per-row LOGGED badge,
              which didn't fit the PLAYER column on phones. */}
          {bets.some(b => b.inBacktest) && (
            <>
              <span className="ev-meta-dot">·</span>
              <span className="ev-legend" title="Already in your backtest — the slip builder won't pick these again.">
                <i className="ev-legend-swatch" aria-hidden="true" />
                logged to backtest
              </span>
            </>
          )}
          <span className="ev-meta-pag">{bets.length} of {allBets.length}</span>
        </div>

        {/* Table */}
        <div className="ev-table">
          {bets.map((b, i) => {
            const key = b.id || (b.player + b.prop + b.line);
            const isSel = selected.find(p => p.key === key);
            return (
              <div
                key={key + i}
                className={"ev-row ev-row-data "
                  + (isSel ? "is-sel " : "")
                  + (hovered === key ? "is-hov " : "")
                  + (b.inBacktest ? "is-logged " : "")}
                onMouseEnter={() => setHovered(key)}
                onMouseLeave={() => setHovered(null)}
                onClick={() => toggleBet(b)}
                style={{ animationDelay: (i * 14) + "ms" }}
                title={b.inBacktest ? "Already logged — won't be picked for new slips" : undefined}
              >
                {/* Identity zone. The nine grid columns this replaced are now
                    three stacked lines that own the full row width; the row's
                    only other child is the fixed right zone below. */}
                <div className="ev-row-main">
                  <div className="ev-player">
                    {/* No LOGGED badge here: it was `flex-shrink:0` inside the
                        PLAYER cell, so on a phone it ate most of a ~100px track
                        and pushed the player name out over the LEAGUE column.
                        The red row treatment carries the status instead, and
                        .ev-legend below the meta row explains what red means. */}
                    {/* title is no longer an escape hatch for anything: the name
                        wraps rather than truncating at EVERY width now (Task 5
                        promoted .ev-player-n out of the 560px media query, since
                        the airy identity line owns the full row). Kept anyway —
                        harmless, and still useful to assistive tech. */}
                    <span className="ev-player-n" title={b.player}>{b.player}</span>
                    {/* borderRadius is the STRING "var(--r-sm)", and there is no
                        .ev-gd CSS rule, so this inline value is the only source
                        of the badge's corner and no test can see it. Three ways
                        to get it wrong, browser-verified, because the earlier
                        note here named the wrong one:
                          - bare `var(--r-sm)`, unquoted, is not valid JS at all
                            ("Unexpected var") and ./build.sh fails loudly — the
                            safe failure;
                          - a UNITLESS string ("8") is rejected by the CSS parser,
                            so the declaration never lands and border-radius falls
                            back to its initial 0;
                          - a TYPO'd token ("var(--typoo)") does land, but is
                            invalid at computed-value time and also computes 0px.
                        The last two are the dangerous pair: both silently square
                        the badge with nothing failing anywhere. A plain NUMBER
                        (8) is fine — React appends px to numeric style values —
                        it just hardcodes the radius off the scale. */}
                    {b.isGreenDevil && <span className="ev-gd" title="Green devil (PrizePicks goblin) — discounted, higher-hit-rate line" style={{ marginLeft: 6, padding: "1px 6px", borderRadius: "var(--r-sm)", background: "#16a34a", color: "#fff", fontSize: 11, fontWeight: 700 }}>GD</span>}
                  </div>
                  {/* Meta line reads "LEAGUE Prop SIDE line · time", so .ev-side
                      precedes .ev-line ("OVER 25.5"). The dot separator keeps
                      --text-4 and stays a bare glyph — it is on TEXT4_ALLOWED
                      precisely because it is decorative, not read, which is also
                      why it carries aria-hidden: it is inside a sentence now, so
                      a screen reader would otherwise announce the punctuation.
                      Separator and time render as a PAIR or not at all.
                      start_time is genuinely nullable (web/app.py serializes it
                      with getattr(p, "start_time", None)) and fmtGameTime returns
                      an em dash for a falsy value: in the old 9-column grid that
                      dash sat alone in the GAME column and read as "no data", but
                      in this sentence "… OVER 25.5 · —" reads as a dangling
                      separator. No time is better than a stub. */}
                  <div className="ev-row-meta">
                    <LeaguePill league={b.league} />
                    <span className="ev-prop">{b.prop}</span>
                    <span className={"ev-side " + (b.side === "OVER" ? "is-over" : "is-under")}>{b.side}</span>
                    <span className="ev-line">{b.line}</span>
                    {b.startTime && <><span className="ev-meta-dot" aria-hidden="true">·</span><span className="ev-time">{fmtGameTime(b.startTime)}</span></>}
                  </div>
                  <div className="ev-books">
                    {b.books.map(([bk, od], j) => <BookBadge key={j} book={bk} odds={od} />)}
                  </div>
                </div>
                {/* Right zone: the true% is the hero number of the row. The add
                    button is a <span> with no onClick — the row's own onClick is
                    the only trigger, so the whole row stays one hit target.
                    .ev-add-btn is a DIRECT child: the .ev-add wrapper it used to
                    sit in existed only to center it inside the old grid's 9th
                    track, and .ev-row-side (align-items:center) plus the button's
                    own display:grid do that now. */}
                <div className="ev-row-side">
                  <TruePct value={b.truePct} />
                  <span className={"ev-add-btn " + (isSel ? "is-sel" : "")}>{isSel ? "✓" : "+"}</span>
                </div>
              </div>
            );
          })}
          {loadState === "ok" && bets.length === 0 && (
            <div className="ev-empty-row">
              {showGreenDevils ? "No green devils available right now." : "No bets match your filters."}
            </div>
          )}
        </div>

      </div>

      {/* Slip Builder — desktop sidebar (always visible on wide screens; the
          mobile copy lives inside .ev-main, right under the config tile). */}
      <aside className="ev-slip ev-slip-desktop">
        {slipBody}
      </aside>
    </main>
  );
}

Object.assign(window, { EVPage });
