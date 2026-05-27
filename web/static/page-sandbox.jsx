// Sandbox — strategy simulator. Mirrors #sandbox-view.
const { useState: useStateS, useMemo: useMemoS, useRef: useRefS, useEffect: useEffectS } = React;

const SB_LEAGUES = ["NBA", "WNBA", "MLB", "NHL", "NCAAB"];

function SandboxPage() {
  const [leagues, setLeagues] = useState(new Set(["NBA", "WNBA", "MLB", "NHL"]));
  const [minProb, setMinProb] = useState(54.1);
  const [size, setSize] = useState(2);
  const [type, setType] = useState("power");
  const [kelly, setKelly] = useState(true);
  const [results, setResults] = useState(null);
  const [hover, setHover] = useState(null);
  const [range, setRange] = useState("MAX");

  const toggleLeague = (l) => {
    const n = new Set(leagues);
    if (n.has(l)) n.delete(l); else n.add(l);
    setLeagues(n);
  };

  // BE = (1/payout) ^ (1/legs) — Power lookup
  const breakEvens = {
    power: { 2: 60.0, 3: 56.0, 4: 54.5, 5: 54.2, 6: 54.07 },
    flex:  { 2: 50.0, 3: 48.0, 4: 46.0, 5: 45.0, 6: 44.0 },
  };
  const be = breakEvens[type][size] || 54.07;

  const run = () => {
    // Generate deterministic-ish sample series based on inputs
    const days = 120;
    const start = new Date();
    start.setDate(start.getDate() - days);
    const series = [];
    let bankroll = 100;
    // edge proportional to (minProb - be), capped — realistic ROI scale
    const edge = Math.max(-0.04, Math.min(0.04, (minProb - be) / 100 * 0.6));
    let slipsPlayed = 0;
    let slipsHit = 0;
    let drawdown = 0;
    let peak = 100;
    const slipLog = [];

    for (let i = 0; i <= days; i++) {
      const d = new Date(start);
      d.setDate(d.getDate() + i);
      // 1-3 slips per day depending on league count
      const slipsToday = Math.min(3, Math.max(0, Math.round(leagues.size * 0.4 + (Math.random() < 0.3 ? 1 : 0))));
      for (let s = 0; s < slipsToday; s++) {
        const stake = kelly ? Math.max(0.25, bankroll * 0.012) : 1;
        const payout = type === "power" ? { 2:3, 3:6, 4:10, 5:20, 6:40 }[size] : { 2:2, 3:2.25, 4:5, 5:10, 6:25 }[size];
        const winChance = 0.5 + edge + (Math.random() - 0.5) * 0.05;
        const win = Math.random() < winChance;
        const change = win ? stake * (payout - 1) : -stake;
        bankroll += change;
        slipsPlayed++;
        if (win) slipsHit++;
        peak = Math.max(peak, bankroll);
        drawdown = Math.max(drawdown, (peak - bankroll) / peak);
        if (slipLog.length < 200) {
          slipLog.push({
            date: d.toLocaleDateString(undefined, { month: "numeric", day: "numeric" }),
            league: [...leagues][Math.floor(Math.random() * leagues.size)],
            legs: size,
            hits: win ? size : size - 1 - Math.floor(Math.random() * 2),
            stake: stake.toFixed(2),
            payout: win ? (stake * payout).toFixed(2) : "0.00",
            pl: change.toFixed(2),
          });
        }
      }
      series.push({ date: d, bankroll: parseFloat(bankroll.toFixed(2)) });
    }

    const totalPL = bankroll - 100;
    const roi = (totalPL / (slipsPlayed * (kelly ? 1.2 : 1))) * 100;
    setResults({
      series,
      slips: slipsPlayed,
      profit: totalPL,
      roi,
      winRate: slipsPlayed ? (slipsHit / slipsPlayed) * 100 : 0,
      drawdown: drawdown * 100,
      slipLog,
    });
    setRange("MAX");
  };

  // Auto-run once on mount
  useEffect(() => { run(); }, []);

  const series = results?.series || [];
  const filteredSeries = useMemo(() => {
    if (!series.length) return [];
    const n = { "1D": 1, "1W": 7, "1M": 30, "3M": 90, "1Y": 365, "MAX": series.length }[range];
    return series.slice(-Math.min(n, series.length));
  }, [series, range]);

  return (
    <main className="bd-page sb-page">
      <div style={{padding:"10px 14px",margin:"0 0 14px",background:"rgba(99,102,241,.10)",border:"1px solid rgba(99,102,241,.25)",borderRadius:10,fontSize:13,color:"var(--text-2)"}}>
        Showing simulated results. Live sandbox wiring is in progress.
      </div>
      {results && (
        <div className="bt-summary">
          <StatCard label="Total Slips" value={results.slips.toString()} />
          <StatCard label="Total Profit" value={(results.profit >= 0 ? "+" : "") + "$" + results.profit.toFixed(2)} tone={results.profit >= 0 ? "good" : "bad"} />
          <StatCard label="ROI" value={(results.roi >= 0 ? "+" : "") + results.roi.toFixed(1) + "%"} tone={results.roi >= 0 ? "good" : "bad"} />
          <StatCard label="Win Rate" value={results.winRate.toFixed(1) + "%"} />
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
            <div className="sb-be-hint">Break-even for this slip: <b className="mono">{be.toFixed(2)}%</b></div>
          </div>

          <div className="sb-ctrl">
            <label>Slip Size</label>
            <select value={size} onChange={e => setSize(+e.target.value)} className="sb-sel">
              {[2,3,4,5,6].map(n => <option key={n} value={n}>{n}-Leg</option>)}
            </select>
          </div>

          <div className="sb-ctrl">
            <label>Slip Type</label>
            <div className="sb-seg">
              <button className={"sb-seg-btn " + (type === "power" ? "is-on" : "")} onClick={() => setType("power")}>Power</button>
              <button className={"sb-seg-btn " + (type === "flex" ? "is-on" : "")} onClick={() => setType("flex")}>Flex</button>
            </div>
          </div>

          <label className="sb-toggle">
            <input type="checkbox" checked={kelly} onChange={e => setKelly(e.target.checked)} />
            <span>Use Kelly Sizing</span>
          </label>

          <button className="cp-btn cp-btn-primary sb-run" onClick={run}>Run Simulation</button>
        </aside>

        {/* Equity chart + slip log */}
        <div className="an-panel sb-results">
          <div className="an-panel-h">
            <h3>Cumulative Performance</h3>
            <span className="an-section-sub">
              Starting bankroll <b>$100</b> · {kelly ? "quarter-Kelly sizing" : "1u flat stake"}
            </span>
          </div>

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
                  <button className="cp-btn cp-btn-ghost cp-btn-sm">Export CSV</button>
                </div>
                <div className="bd-tbl-wrap sb-log-wrap">
                  <table className="bd-tbl">
                    <thead>
                      <tr><th>Date</th><th>League</th><th>Legs</th><th>Hits</th><th>Stake</th><th>Payout</th><th>P/L</th></tr>
                    </thead>
                    <tbody>
                      {results.slipLog.slice(0, 60).map((s, i) => (
                        <tr key={i}>
                          <td className="mono">{s.date}</td>
                          <td><LeaguePill league={s.league} /></td>
                          <td className="mono">{s.legs}</td>
                          <td className={"mono " + (s.hits >= s.legs ? "tone-good" : "")}>{s.hits} / {s.legs}</td>
                          <td className="mono">${s.stake}</td>
                          <td className="mono">${s.payout}</td>
                          <td className={"mono " + (parseFloat(s.pl) > 0 ? "tone-good" : parseFloat(s.pl) < 0 ? "tone-bad" : "")}>
                            {parseFloat(s.pl) > 0 ? "+" : ""}${s.pl}
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
  const xs = (i) => padL + (i / (series.length - 1)) * (w - padL - padR);
  const min = Math.min(...series.map(p => p.bankroll));
  const max = Math.max(...series.map(p => p.bankroll));
  const yRange = (max - min) || 1;
  const ys = (v) => padT + (1 - (v - min) / yRange) * (H - padT - padB);
  const start = series[0].bankroll;
  const end = series[series.length - 1].bankroll;
  const isUp = end >= start;
  const color = isUp ? "#22C55E" : "#EF4444";
  const path = series.map((p, i) => `${i === 0 ? "M" : "L"}${xs(i).toFixed(1)},${ys(p.bankroll).toFixed(1)}`).join(" ");
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

Object.assign(window, { SandboxPage });
