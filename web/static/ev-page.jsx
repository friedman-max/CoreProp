// +EV Bets page — wired to /api/bootstrap/core.
const { useState: useStateE, useMemo: useMemoE } = React;

function fmtGameTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleDateString([], { month: "numeric", day: "numeric" }) +
    " " + d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
}

function EVPage() {
  const [league, setLeague] = useState("All");
  const [propQ, setPropQ] = useState("");
  const [minOdds, setMinOdds] = useState(54);
  const [side, setSide] = useState("Both");
  const [slipType, setSlipType] = useState("Power");
  const [legs, setLegs] = useState(6);
  const [selected, setSelected] = useState([]);
  const [hovered, setHovered] = useState(null);
  const [allBets, setAllBets] = useState([]);
  const [loadState, setLoadState] = useState("loading"); // loading | ok | error
  const [errMsg, setErrMsg] = useState("");
  const [saving, setSaving] = useState(false);
  const [autoBacktest, setAutoBacktest] = useState(false);
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
          setMinLegOverride(cfg.auto_slip_min_prob * 100);
        }
        if (typeof cfg.auto_backtest === "boolean") {
          setAutoBacktest(cfg.auto_backtest);
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

  // Whenever slipType / legs / minLegOverride changes, mark prefs as
  // unsaved so the Save button surfaces "Save preferences" again. Skip
  // during the initial hydration pass so loaded values don't show as
  // dirty before the user touches anything.
  React.useEffect(() => {
    if (!prefsHydrated.current) return;
    setPrefsSaveState("idle");
  }, [slipType, legs, minLegOverride]);

  const saveSlipPrefs = async () => {
    if (!window.cpApi || !window.cpApi.isLoggedIn()) return;
    setPrefsSaveState("saving");
    try {
      const minPct = (typeof minLegOverride === "number") ? minLegOverride : slipBE;
      await window.cpApi.apiFetch("/api/user/slip-prefs", {
        method: "POST",
        body: {
          auto_slip_type: slipType,
          auto_slip_legs: legs,
          auto_slip_min_prob: Math.max(0.01, Math.min(0.99, minPct / 100)),
        },
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
      if (league !== "All" && b.league !== league) return false;
      if (propQ && !(b.prop || "").toLowerCase().includes(propQ.toLowerCase())) return false;
      if (side !== "Both" && b.side !== side.toUpperCase()) return false;
      if ((b.truePct || 0) < minOdds) return false;
      return true;
    }).sort((a, b) => (b.truePct || 0) - (a.truePct || 0));
  }, [allBets, league, propQ, minOdds, side]);

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
      await window.cpApi.apiFetch("/api/slip", {
        method: "POST",
        body: { bet_ids: selected.map(s => s.id).filter(Boolean), bankroll: 100 },
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
    } catch (ex) {
      alert("Save failed: " + (ex.message || ex));
    } finally {
      setSaving(false);
    }
  };

  const slipBE = useMemoE(() => {
    // Break-even depending on slip type & legs (illustrative)
    const table = { Power: { 2: 60.0, 3: 56.0, 4: 54.5, 5: 54.2, 6: 54.07, 7: 54.0 }, Flex: { 2: 50, 3: 48, 4: 46, 5: 45, 6: 44, 7: 43 } };
    return (table[slipType] && table[slipType][legs]) || 54.07;
  }, [slipType, legs]);

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
          <button className="ev-clear" onClick={() => { setLeague("All"); setPropQ(""); setMinOdds(54); setSide("Both"); }}>Clear</button>
        </div>

        <div className="ev-meta">
          <span><b>{bets.length}</b> bets</span>
          <span className="ev-meta-dot">·</span>
          <span>
            {loadState === "loading" && "Loading…"}
            {loadState === "ok" && <>Updated <em className="ev-pulse">live</em></>}
            {loadState === "error" && <span style={{color:"#FCA5A5"}}>Error: {errMsg}</span>}
          </span>
          <span className="ev-meta-pag">{bets.length} of {allBets.length}</span>
        </div>

        {/* Table */}
        <div className="ev-table">
          <div className="ev-row ev-row-hd">
            <span>PLAYER</span>
            <span>LEAGUE</span>
            <span>PROP</span>
            <span>LINE</span>
            <span>SIDE</span>
            <span className="ev-th-sort">TRUE % <span className="ev-arrow">↓</span></span>
            <span>BOOK ODDS</span>
            <span>GAME</span>
            <span></span>
          </div>
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
                <span className="ev-player">
                  <span className="ev-player-n">{b.player}</span>
                  {b.inBacktest && <span className="ev-logged">LOGGED</span>}
                </span>
                <span><LeaguePill league={b.league} /></span>
                <span className="ev-prop">{b.prop}</span>
                <span className="ev-line">{b.line}</span>
                <span className={"ev-side " + (b.side === "OVER" ? "is-over" : "is-under")}>{b.side}</span>
                <span><TruePct value={b.truePct} /></span>
                <span className="ev-books">
                  {b.books.map(([bk, od], j) => <BookBadge key={j} book={bk} odds={od} />)}
                </span>
                <span className="ev-time">{fmtGameTime(b.startTime)}</span>
                <span className="ev-add">
                  <span className={"ev-add-btn " + (isSel ? "is-sel" : "")}>{isSel ? "✓" : "+"}</span>
                </span>
              </div>
            );
          })}
        </div>

        {/* Big Save-preferences bar at the bottom of the +EV main column.
         * Always visible (not just when "dirty") so the user can re-confirm
         * their Auto-Backtest min % is what's stored on their account. */}
        <div className="ev-bottom-save">
          <div className="ev-bottom-save-info">
            <div className="ev-bottom-save-title">Auto-Backtest preferences</div>
            <div className="ev-bottom-save-sub">
              <span>Min Leg %: <b className="mono">{minLegOverride != null ? (typeof minLegOverride === "number" ? minLegOverride.toFixed(2) : minLegOverride) : slipBE.toFixed(2)}%</b></span>
              <span className="ev-meta-dot">·</span>
              <span>{slipType} · {legs}L</span>
              <span className="ev-meta-dot">·</span>
              <span>Auto-Backtest <b>{autoBacktest ? "on" : "off"}</b></span>
            </div>
          </div>
          <button
            type="button"
            className={"ev-prefs-save ev-prefs-save-bottom ev-prefs-save-" + prefsSaveState}
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
      </div>

      {/* Slip Builder */}
      <aside className="ev-slip">
        <div className="ev-slip-hd">
          <h3>Slip Builder</h3>
          <label className="ev-auto">
            <input type="checkbox" checked={autoBacktest} onChange={e => setAutoBacktest(e.target.checked)} />
            Auto-Backtest
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
              value={minLegOverride != null ? minLegOverride : slipBE.toFixed(2)}
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

        {selected.length > 0 && (
          <div className="ev-slip-summary">
            <div className="ev-sum-row">
              <span>Combined hit %</span>
              <b>{(selected.reduce((a, b) => a * b.truePct / 100, 1) * 100).toFixed(2)}%</b>
            </div>
            <div className="ev-sum-row">
              <span>vs. break-even</span>
              <b className="ev-edge">+{((selected.reduce((a, b) => a * b.truePct / 100, 1) * 100) - slipBE).toFixed(2)}%</b>
            </div>
          </div>
        )}

        <button
          className={"cp-btn cp-btn-save " + (selected.length < 2 || selected.length > 6 || saving ? "is-dis" : "")}
          disabled={selected.length < 2 || selected.length > 6 || saving}
          onClick={saveSlip}
        >
          {saving ? "Saving…" : (selected.length < 2 ? "Add 2+ legs" : selected.length > 6 ? "Max 6 legs" : "Save slip")}
        </button>
      </aside>
    </main>
  );
}

Object.assign(window, { EVPage });
