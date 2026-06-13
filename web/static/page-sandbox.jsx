// Sandbox — strategy simulator. Replays the resolved market_observatory
// through engine/strategy_tester via POST /api/sandbox/run (live wiring).
const { useState: useStateS, useMemo: useMemoS, useRef: useRefS, useEffect: useEffectS } = React;

const SB_LEAGUES = ["NBA", "WNBA", "MLB", "NHL", "NCAAB", "SOCCER"];

// Per-leg break-even probability (as a fraction) for each (type, size).
// This MUST mirror engine/constants.py BREAK_EVEN exactly — it is the same
// table the backend gates slips on, so the "Break-even for this slip" hint
// matches the number the simulator actually uses to decide +EV.
// Note flex has no 2-leg entry: a 2-leg flex slip is not a real PrizePicks
// product (it degenerates to a 2-leg power), so flex starts at 3 legs.
const SB_BREAK_EVEN = {
  power: { 2: 0.5774, 3: 0.5503, 4: 0.5623, 5: 0.5493, 6: 0.5407 },
  flex:  {            3: 0.5774, 4: 0.5503, 5: 0.5425, 6: 0.5421 },
};

// Allowed slip sizes for a given type, derived from the break-even table so
// the two can never drift apart. Power: 2-6. Flex: 3-6 (no 2-leg flex).
function sbAllowedSizes(type) {
  return Object.keys(SB_BREAK_EVEN[type] || {}).map(Number).sort((a, b) => a - b);
}

// Break-even fraction for a (type, size), falling back to the Power-6
// geometric break-even (the standard reference) if the combo is unknown.
function sbBreakEven(type, size) {
  const tbl = SB_BREAK_EVEN[type] || {};
  return tbl[size] != null ? tbl[size] : 0.5407;
}

// Build the POST /api/sandbox/run request body from UI state. min_prob is
// sent as a fraction; the backend defaults slip_strategy/bootstrap/bet_size.
function sbBuildRequest({ leagues, minProb, size, type, kelly }) {
  return {
    leagues:   [...leagues],
    min_prob:  minProb / 100,
    slip_size: size,
    slip_type: type,
    use_kelly: !!kelly,
    bet_size:  1.0,
  };
}

// Time-window the equity series for the range pills. Anchored to the LAST
// data point (replay history may be weeks old, so anchoring to "now" would
// blank the chart). MAX returns everything; if a window excludes every
// point we fall back to the final point so the chart never goes empty.
const SB_RANGE_MS = {
  "1D": 86400000, "1W": 7 * 86400000, "1M": 30 * 86400000,
  "3M": 90 * 86400000, "1Y": 365 * 86400000, "MAX": null,
};
function sbFilterByRange(series, range) {
  if (!series.length) return series;
  const span = SB_RANGE_MS[range];
  if (span == null) return series;
  const end = series[series.length - 1].x;
  const cut = series.filter(p => p.x >= end - span);
  return cut.length ? cut : series.slice(-1);
}

// Map the /api/sandbox/run payload into the shape the UI renders. The
// equity curve carries cumulative PROFIT per slip; the chart shows bankroll,
// so we add the starting bankroll (summary.bankroll, default 100).
function sbMapResult(data) {
  const summary = data.summary || {};
  const startBank = summary.bankroll != null ? summary.bankroll : 100;
  const series = (data.equity_curve || []).map(pt => ({
    x: new Date(pt.x).getTime(),
    y: startBank + (pt.y || 0),
  }));
  const slipLog = (data.slips || []).map(s => ({
    ts:     s.timestamp,
    legs:   s.n_legs != null ? s.n_legs : (s.legs ? s.legs.length : 0),
    hits:   s.hits || 0,
    nEff:   s.n_eff != null ? s.n_eff : (s.n_legs || 0),
    stake:  s.bet_size || 0,
    payout: s.payout || 0,
    pl:     s.profit || 0,
  }));
  return {
    series,
    startBank,
    slips:    summary.total_slips || 0,
    profit:   summary.total_profit || 0,
    roi:      summary.roi_pct || 0,
    winRate:  summary.win_rate_pct || 0,
    drawdown: summary.max_drawdown_pct || 0,
    ci:       summary.ci || {},
    slipLog,
  };
}

// Render a bootstrap CI as a compact "[lo, hi]" sub-label, or "" if absent.
function sbFmtCI(ci, suffix) {
  if (!ci || ci.lo == null || ci.hi == null) return "";
  const s = (v) => (v >= 0 ? "+" : "") + v.toFixed(1) + (suffix || "");
  return `95% CI [${s(ci.lo)}, ${s(ci.hi)}]`;
}

function SandboxPage() {
  const [leagues, setLeagues] = useState(new Set(["NBA", "WNBA", "MLB", "NHL"]));
  const [minProb, setMinProb] = useState(54.1);
  const [size, setSize] = useState(2);
  const [type, setType] = useState("power");
  const [kelly, setKelly] = useState(true);
  const [results, setResults] = useState(null);
  const [hover, setHover] = useState(null);
  const [range, setRange] = useState("MAX");
  const [loadState, setLoadState] = useState("idle"); // idle|loading|ok|error
  const [errMsg, setErrMsg] = useState("");

  const toggleLeague = (l) => {
    const n = new Set(leagues);
    if (n.has(l)) n.delete(l); else n.add(l);
    setLeagues(n);
  };

  // Switching to Flex while a 2-leg slip is selected: bump to the smallest
  // legal flex size (3). 2-leg flex is not a real product.
  const setSlipType = (t) => {
    setType(t);
    if (!sbAllowedSizes(t).includes(size)) {
      setSize(sbAllowedSizes(t)[0]);
    }
  };

  const allowedSizes = sbAllowedSizes(type);
  const be = sbBreakEven(type, size);

  const run = async () => {
    setLoadState("loading");
    setErrMsg("");
    try {
      const data = await window.cpApi.apiFetch("/api/sandbox/run", {
        method: "POST",
        body: sbBuildRequest({ leagues, minProb, size, type, kelly }),
      });
      setResults(sbMapResult(data));
      setRange("MAX");
      setLoadState("ok");
    } catch (ex) {
      // The backend returns a 400 with a human-readable `detail` when the
      // filters leave nothing to simulate; apiFetch folds that into the
      // error message. Surface it verbatim — it tells the user how to fix
      // their config (e.g. "Raise Min True Prob to at least 54.07%").
      setResults(null);
      setErrMsg(ex.message || "Simulation failed.");
      setLoadState("error");
    }
  };

  // Auto-run once on mount.
  useEffect(() => { run(); }, []);

  const series = results?.series || [];
  const filteredSeries = useMemo(
    () => sbFilterByRange(series, range),
    [series, range]
  );

  const exportCsv = () => {
    if (!results?.slipLog?.length) return;
    const head = "date,legs,hits,n_eff,stake,payout,pl";
    const rows = results.slipLog.map(s => {
      const d = s.ts ? new Date(s.ts).toISOString() : "";
      return [d, s.legs, s.hits, s.nEff, s.stake.toFixed(2), s.payout.toFixed(2), s.pl.toFixed(2)].join(",");
    });
    const blob = new Blob([[head, ...rows].join("\n")], { type: "text/csv" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "sandbox-slips.csv";
    a.click();
    URL.revokeObjectURL(a.href);
  };

  return (
    <main className="bd-page sb-page">
      {loadState === "error" && (
        <div style={{padding:"10px 14px",margin:"0 0 14px",background:"rgba(239,68,68,.10)",border:"1px solid rgba(239,68,68,.30)",borderRadius:10,fontSize:13,color:"#FCA5A5"}}>
          {errMsg}
        </div>
      )}
      {results && (
        <div className="bt-summary">
          <StatCard label="Total Slips" value={results.slips.toString()} />
          <StatCard label="Total Profit" value={(results.profit >= 0 ? "+" : "") + "$" + results.profit.toFixed(2)} tone={results.profit >= 0 ? "good" : "bad"} />
          <StatCard label="ROI" sub={sbFmtCI(results.ci.roi_pct, "%") || null} value={(results.roi >= 0 ? "+" : "") + results.roi.toFixed(1) + "%"} tone={results.roi >= 0 ? "good" : "bad"} />
          <StatCard label="Win Rate" sub={sbFmtCI(results.ci.win_rate_pct, "%") || null} value={results.winRate.toFixed(1) + "%"} />
          <StatCard label="Max Drawdown" value={"-" + results.drawdown.toFixed(1) + "%"} tone="bad" />
        </div>
      )}

      <div className="sb-layout">
        {/* Strategy Settings */}
        <aside className="an-panel sb-controls">
          <div className="an-panel-h">
            <h3>Strategy Settings</h3>
          </div>

          <div className="sb-ctrl">
            <label>Leagues</label>
            <div className="bd-chips">
              {SB_LEAGUES.map(l => (
                <button
                  key={l}
                  className={"bd-chip " + (leagues.has(l) ? "is-on" : "")}
                  onClick={() => toggleLeague(l)}
                >{l}</button>
              ))}
            </div>
          </div>

          <div className="sb-ctrl">
            <div className="sb-ctrl-h">
              <label>Min True Prob</label>
              <span className="mono sb-val">{minProb.toFixed(1)}%</span>
            </div>
            <input type="range" min="50" max="60" step="0.1" value={minProb} onChange={e => setMinProb(parseFloat(e.target.value))} />
            <div className="sb-be-hint">Break-even for this slip: <b className="mono">{(be * 100).toFixed(2)}%</b></div>
          </div>

          <div className="sb-ctrl">
            <label>Slip Size</label>
            <select value={size} onChange={e => setSize(+e.target.value)} className="sb-sel">
              {allowedSizes.map(n => <option key={n} value={n}>{n}-Leg</option>)}
            </select>
          </div>

          <div className="sb-ctrl">
            <label>Slip Type</label>
            <div className="sb-seg">
              <button className={"sb-seg-btn " + (type === "power" ? "is-on" : "")} onClick={() => setSlipType("power")}>Power</button>
              <button className={"sb-seg-btn " + (type === "flex" ? "is-on" : "")} onClick={() => setSlipType("flex")}>Flex</button>
            </div>
          </div>

          <label className="sb-toggle">
            <input type="checkbox" checked={kelly} onChange={e => setKelly(e.target.checked)} />
            <span>Use Kelly Sizing</span>
          </label>

          <button className="cp-btn cp-btn-primary sb-run" onClick={run} disabled={loadState === "loading"}>
            {loadState === "loading" ? "Running…" : "Run Simulation"}
          </button>
        </aside>

        {/* Equity chart + slip log */}
        <div className="an-panel sb-results">
          <div className="an-panel-h">
            <h3>Cumulative Performance</h3>
            <span className="an-section-sub">
              Starting bankroll <b>${results ? results.startBank.toFixed(0) : "100"}</b> · {kelly ? "quarter-Kelly sizing" : "1u flat stake"}
            </span>
          </div>

          {loadState === "loading" && !results && (
            <div style={{padding:"40px", color:"var(--text-3)", textAlign:"center"}}>Running simulation…</div>
          )}

          {results && (
            <>
              <SBEquityChart series={filteredSeries} onHover={setHover} hover={hover} />
              <div className="pnl-range">
                {["1D","1W","1M","3M","1Y","MAX"].map(r => (
                  <button key={r} className={"pnl-range-btn " + (range === r ? "is-on" : "")} onClick={() => setRange(r)}>{r}</button>
                ))}
              </div>

              <div className="sb-log">
                <div className="sb-log-h">
                  <h4>Slip Log <span className="sb-log-count">{results.slipLog.length} slips</span></h4>
                  <button className="cp-btn cp-btn-ghost cp-btn-sm" onClick={exportCsv}>Export CSV</button>
                </div>
                <div className="bd-tbl-wrap sb-log-wrap">
                  <table className="bd-tbl">
                    <thead>
                      <tr><th>Date</th><th>Legs</th><th>Hits</th><th>Stake</th><th>Payout</th><th>P/L</th></tr>
                    </thead>
                    <tbody>
                      {results.slipLog.slice(0, 60).map((s, i) => (
                        <tr key={i}>
                          <td className="mono">{s.ts ? new Date(s.ts).toLocaleDateString(undefined, { month: "numeric", day: "numeric" }) : "—"}</td>
                          <td className="mono">{s.legs}</td>
                          <td className={"mono " + (s.hits >= s.nEff ? "tone-good" : "")}>{s.hits} / {s.nEff}</td>
                          <td className="mono">${s.stake.toFixed(2)}</td>
                          <td className="mono">${s.payout.toFixed(2)}</td>
                          <td className={"mono " + (s.pl > 0 ? "tone-good" : s.pl < 0 ? "tone-bad" : "")}>
                            {s.pl > 0 ? "+" : ""}${s.pl.toFixed(2)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </main>
  );
}

function SBEquityChart({ series, onHover, hover }) {
  const ref = useRef(null);
  const [w, setW] = useState(800);
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver(es => setW(Math.max(600, es[0].contentRect.width)));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  if (!series.length) return <div ref={ref} className="pnl-chart" />;

  const H = 300, padL = 60, padR = 24, padT = 16, padB = 30;
  const denom = series.length > 1 ? series.length - 1 : 1;
  const xs = (i) => padL + (i / denom) * (w - padL - padR);
  const min = Math.min(...series.map(p => p.y));
  const max = Math.max(...series.map(p => p.y));
  const yRange = (max - min) || 1;
  const ys = (v) => padT + (1 - (v - min) / yRange) * (H - padT - padB);
  const start = series[0].y;
  const end = series[series.length - 1].y;
  const isUp = end >= start;
  const color = isUp ? "#22C55E" : "#EF4444";
  const path = series.map((p, i) => `${i === 0 ? "M" : "L"}${xs(i).toFixed(1)},${ys(p.y).toFixed(1)}`).join(" ");
  const fillPath = `${path} L${xs(series.length - 1)},${H - padB} L${padL},${H - padB} Z`;

  return (
    <div ref={ref} className="pnl-chart">
      <svg viewBox={`0 0 ${w} ${H}`} width="100%" height={H}>
        <defs>
          <linearGradient id="sb-fill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.25" />
            <stop offset="100%" stopColor={color} stopOpacity="0" />
          </linearGradient>
        </defs>
        {[0, 0.25, 0.5, 0.75, 1].map(t => {
          const v = min + yRange * (1 - t);
          const y = padT + t * (H - padT - padB);
          return (
            <g key={t}>
              <line x1={padL} x2={w - padR} y1={y} y2={y} stroke="rgba(255,255,255,.05)" />
              <text x={padL - 8} y={y + 4} fill="#7a7a8b" fontSize="11" textAnchor="end" fontFamily="JetBrains Mono">${v.toFixed(0)}</text>
            </g>
          );
        })}
        <path d={fillPath} fill="url(#sb-fill)" />
        <path d={path} fill="none" stroke={color} strokeWidth="2" />
      </svg>
    </div>
  );
}

Object.assign(window, {
  SandboxPage,
  SB_BREAK_EVEN, sbAllowedSizes, sbBreakEven, sbBuildRequest, sbFilterByRange, sbMapResult, sbFmtCI,
});
