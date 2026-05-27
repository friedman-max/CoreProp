// +EV Bets page — polished version of the screenshot.
const { useState: useStateE, useMemo: useMemoE } = React;

function EVPage() {
  const [league, setLeague] = useState("All");
  const [propQ, setPropQ] = useState("");
  const [minOdds, setMinOdds] = useState(54);
  const [side, setSide] = useState("Both");
  const [slipType, setSlipType] = useState("Power");
  const [legs, setLegs] = useState(6);
  const [selected, setSelected] = useState([]);
  const [hovered, setHovered] = useState(null);

  const bets = useMemoE(() => {
    return EV_BETS.filter(b => {
      if (league !== "All" && b.league !== league) return false;
      if (propQ && !b.prop.toLowerCase().includes(propQ.toLowerCase())) return false;
      if (side !== "Both" && b.side !== side.toUpperCase()) return false;
      if (b.truePct < minOdds) return false;
      return true;
    });
  }, [league, propQ, minOdds, side]);

  const toggleBet = (b) => {
    const key = b.player + b.prop + b.line;
    setSelected(prev => prev.find(p => p.key === key) ? prev.filter(p => p.key !== key) : [...prev, { ...b, key }]);
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
          <span>Updated <em className="ev-pulse">live</em></span>
          <span className="ev-meta-pag">P. 1 / 1</span>
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
            const key = b.player + b.prop + b.line;
            const isSel = selected.find(p => p.key === key);
            return (
              <div
                key={key + i}
                className={"ev-row ev-row-data " + (isSel ? "is-sel " : "") + (hovered === key ? "is-hov " : "")}
                onMouseEnter={() => setHovered(key)}
                onMouseLeave={() => setHovered(null)}
                onClick={() => toggleBet(b)}
                style={{ animationDelay: (i * 14) + "ms" }}
              >
                <span className="ev-player">
                  <span className="ev-player-n">{b.player}</span>
                  <span className="ev-logged">LOGGED</span>
                </span>
                <span><LeaguePill league={b.league} /></span>
                <span className="ev-prop">{b.prop}</span>
                <span className="ev-line">{b.line}</span>
                <span className={"ev-side " + (b.side === "OVER" ? "is-over" : "is-under")}>{b.side}</span>
                <span><TruePct value={b.truePct} /></span>
                <span className="ev-books">
                  {b.books.map(([bk, od], j) => <BookBadge key={j} book={bk} odds={od} />)}
                </span>
                <span className="ev-time">{b.time}</span>
                <span className="ev-add">
                  <span className={"ev-add-btn " + (isSel ? "is-sel" : "")}>{isSel ? "✓" : "+"}</span>
                </span>
              </div>
            );
          })}
        </div>
      </div>

      {/* Slip Builder */}
      <aside className="ev-slip">
        <div className="ev-slip-hd">
          <h3>Slip Builder</h3>
          <label className="ev-auto"><input type="checkbox" /> Auto-Backtest</label>
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
            <input className="cp-input cp-input-sm" value={slipBE.toFixed(2)} readOnly />
            <button className="cp-btn cp-btn-ghost cp-btn-sm">Reset</button>
          </div>
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

        <button className={"cp-btn cp-btn-save " + (selected.length === 0 ? "is-dis" : "")} disabled={selected.length === 0}>
          Save slip
        </button>
      </aside>
    </main>
  );
}

Object.assign(window, { EVPage });
