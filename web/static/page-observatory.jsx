// Observatory — calibration curves + per-prop multipliers + heatmap + market feed.
const { useState: useStateO, useMemo: useMemoO } = React;

// Per-prop multipliers (illustrative, mirrors obs-multiplier-table)
const MULTIPLIERS = [
  { league: "NBA",  prop: "Points",       over: 1.00, overN: 412, under: 0.97, underN: 398 },
  { league: "NBA",  prop: "Rebounds",     over: 0.94, overN: 287, under: 1.05, underN: 290 },
  { league: "NBA",  prop: "Assists",      over: 0.98, overN: 245, under: 1.02, underN: 248 },
  { league: "NBA",  prop: "Pts+Rebs",     over: 0.96, overN: 156, under: 1.03, underN: 162 },
  { league: "NBA",  prop: "P+R+A",        over: 1.01, overN: 134, under: 0.99, underN: 138 },
  { league: "WNBA", prop: "Points",       over: 1.02, overN: 178, under: 0.99, underN: 174 },
  { league: "WNBA", prop: "Rebounds",     over: 0.92, overN: 142, under: 1.07, underN: 145 },
  { league: "WNBA", prop: "Rebs+Asts",    over: 0.91, overN: 124, under: 1.08, underN: 128 },
  { league: "NHL",  prop: "Shots On Goal",over: 1.04, overN: 198, under: 0.97, underN: 202 },
  { league: "NHL",  prop: "Assists",      over: 1.06, overN: 156, under: 0.95, underN: 158 },
  { league: "NHL",  prop: "Blocked Shots",over: 1.02, overN: 87,  under: 0.99, underN: 91 },
  { league: "MLB",  prop: "Hits Allowed", over: 0.95, overN: 102, under: 1.04, underN: 108 },
  { league: "MLB",  prop: "Walks Allowed",over: 0.97, overN: 89,  under: 1.02, underN: 92 },
  { league: "MLB",  prop: "Total Bases",  over: 1.01, overN: 145, under: 1.00, underN: 148 },
];

// Heatmap data — rows are (league, prop), cols are expected-prob bands.
// Each cell is actual hit-rate. null = insufficient sample.
const HEATMAP_BANDS = ["50-55%", "55-60%", "60-65%", "65-70%", "70-75%", "75-80%"];
const HEATMAP = [
  { league: "NBA",  prop: "Points",       row: [52.1, 56.3, 61.2, 67.0, 72.5, null] },
  { league: "NBA",  prop: "Rebounds",     row: [51.5, 54.8, 58.9, 64.2, 68.0, 71.5] },
  { league: "NBA",  prop: "Assists",      row: [50.2, 56.0, 60.5, 65.5, null, null] },
  { league: "NBA",  prop: "Pts+Rebs",     row: [52.8, 55.5, 60.0, 66.5, 70.0, null] },
  { league: "WNBA", prop: "Points",       row: [51.0, 57.2, 60.5, 64.0, 71.0, null] },
  { league: "WNBA", prop: "Rebs+Asts",    row: [52.5, 58.0, 62.5, 66.2, 70.5, 75.0] },
  { league: "NHL",  prop: "Shots On Goal",row: [50.8, 56.5, 60.0, null, null, null] },
  { league: "NHL",  prop: "Assists",      row: [52.0, 57.5, 61.0, 65.5, 70.0, 74.5] },
  { league: "MLB",  prop: "Hits Allowed", row: [51.5, 55.8, 60.5, null, null, null] },
  { league: "MLB",  prop: "Walks Allowed",row: [50.5, 57.0, 61.5, null, null, null] },
];

// Market feed
const FEED = [
  { player: "Cale Makar",       prop: "Assists O0.5",       line: 0.5,  pct: 72.4, result: "hit",     actual: "2",   logged: "5m ago" },
  { player: "V. Wembanyama",    prop: "Assists U3.5",       line: 3.5,  pct: 59.9, result: "hit",     actual: "2",   logged: "12m ago" },
  { player: "Paige Bueckers",   prop: "Rebs+Asts U8.5",     line: 8.5,  pct: 60.2, result: "pending", actual: "—",   logged: "18m ago" },
  { player: "Chet Holmgren",    prop: "Rebounds U7.5",      line: 7.5,  pct: 57.6, result: "hit",     actual: "6",   logged: "22m ago" },
  { player: "Jordan Wicks",     prop: "Hits Allowed U4.5",  line: 4.5,  pct: 62.0, result: "hit",     actual: "3",   logged: "34m ago" },
  { player: "Marina Mabrey",    prop: "Rebs+Asts U7.5",     line: 7.5,  pct: 60.2, result: "miss",    actual: "9",   logged: "48m ago" },
  { player: "Devin Vassell",    prop: "Rebounds U4.5",      line: 4.5,  pct: 57.0, result: "hit",     actual: "3",   logged: "1h ago" },
  { player: "Olivia Miles",     prop: "Rebs+Asts U10.5",    line: 10.5, pct: 63.6, result: "hit",     actual: "8",   logged: "1h ago" },
  { player: "Stephon Castle",   prop: "Points U17.5",       line: 17.5, pct: 57.1, result: "push",    actual: "17",  logged: "1h ago" },
  { player: "Jalen Williams",   prop: "Points U14.5",       line: 14.5, pct: 56.7, result: "hit",     actual: "11",  logged: "2h ago" },
  { player: "Sean Burke",       prop: "Walks Allowed U1.5", line: 1.5,  pct: 58.3, result: "dnp",     actual: "—",   logged: "2h ago" },
];

function ObservatoryPage() {
  const [leagueChips, setLeagueChips] = useState(new Set(["NBA", "WNBA", "MLB", "NHL", "NCAAB"]));
  const [resultChips, setResultChips] = useState(new Set(["pending", "hit", "miss", "push", "dnp"]));

  const togChip = (set, setter) => (v) => {
    const n = new Set(set);
    if (n.has(v)) n.delete(v); else n.add(v);
    setter(n);
  };

  const feed = useMemo(() =>
    FEED.filter(f => resultChips.has(f.result)),
    [resultChips]);

  return (
    <main className="bd-page obs-page">
      <section className="an-panel">
        <div className="an-panel-h">
          <h3>Calibration Curves</h3>
          <span className="an-section-sub">Predicted vs. actual hit rate, by prop family.</span>
        </div>
        <CalibrationCurves />
        <div className="cal-legend">
          <span className="cal-legend-item"><i style={{background:"#6366F1"}} /> Points</span>
          <span className="cal-legend-item"><i style={{background:"#22C55E"}} /> Rebounds</span>
          <span className="cal-legend-item"><i style={{background:"#F59E0B"}} /> Assists</span>
          <span className="cal-legend-item"><i style={{background:"#3DA9F0"}} /> Combo (P+R, R+A)</span>
          <span className="cal-legend-item"><i style={{background:"#A855F7"}} /> Pitcher props</span>
          <span className="cal-legend-item"><i className="cal-legend-dashed" /> Perfect calibration</span>
        </div>
      </section>

      <section className="an-panel">
        <div className="an-panel-h">
          <h3>Per-Prop Calibration</h3>
          <span className="an-section-sub">Boost / nerf applied to each (league, prop, side) bucket at p=0.60. 1.00x = no adjustment. n = effective sample.</span>
        </div>
        <div className="obs-mult-wrap">
          <table className="obs-mult">
            <thead>
              <tr>
                <th>League</th><th>Prop</th>
                <th className="obs-th-c">Over</th>
                <th className="obs-th-c">Under</th>
              </tr>
            </thead>
            <tbody>
              {MULTIPLIERS.map((m, i) => (
                <tr key={i}>
                  <td><LeaguePill league={m.league} /></td>
                  <td>{m.prop}</td>
                  <td><MultCell mult={m.over} n={m.overN} /></td>
                  <td><MultCell mult={m.under} n={m.underN} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="an-panel">
        <div className="an-panel-h">
          <h3>Prop Hit Rate by Expected Probability</h3>
          <span className="an-section-sub">Rows: prop type (grouped by league). Cols: expected hit rate in 5% bands. Cells: actual recency-weighted hit rate.</span>
        </div>
        <div className="obs-heat-wrap">
          <table className="obs-heat">
            <thead>
              <tr>
                <th>Prop</th>
                {HEATMAP_BANDS.map(b => <th key={b}>{b}</th>)}
              </tr>
            </thead>
            <tbody>
              {HEATMAP.map((row, i) => (
                <tr key={i}>
                  <td className="obs-heat-prop">
                    <LeaguePill league={row.league} />
                    <span>{row.prop}</span>
                  </td>
                  {row.row.map((v, j) => {
                    const expected = 52.5 + j * 5; // band center
                    if (v == null) return <td key={j} className="obs-heat-cell is-empty">—</td>;
                    const delta = v - expected;
                    return (
                      <td key={j} className="obs-heat-cell" style={{ background: heatColor(delta) }}>
                        <span className="obs-heat-v mono">{v.toFixed(1)}%</span>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="an-panel">
        <div className="an-panel-h">
          <h3>Market Feed <span className="an-feed-count">(verified lines)</span></h3>
          <span className="an-section-sub">Global observations used to verify model accuracy.</span>
        </div>
        <div className="obs-feed-filters">
          <div className="bd-f">
            <span>Leagues</span>
            <div className="bd-chips">
              {["NBA", "WNBA", "MLB", "NHL", "NCAAB"].map(l => (
                <button
                  key={l}
                  className={"bd-chip " + (leagueChips.has(l) ? "is-on" : "")}
                  onClick={() => togChip(leagueChips, setLeagueChips)(l)}
                >{l}</button>
              ))}
            </div>
          </div>
          <div className="bd-f">
            <span>Result</span>
            <div className="bd-chips">
              {["pending", "hit", "miss", "push", "dnp"].map(r => (
                <button
                  key={r}
                  className={"bd-chip " + (resultChips.has(r) ? "is-on" : "")}
                  onClick={() => togChip(resultChips, setResultChips)(r)}
                >{r}</button>
              ))}
            </div>
          </div>
        </div>
        <div className="bd-tbl-wrap">
          <table className="bd-tbl">
            <thead>
              <tr><th>Player</th><th>Prop</th><th>Line</th><th>Target Prob</th><th>Result</th><th>Actual</th><th>Logged</th></tr>
            </thead>
            <tbody>
              {feed.map((f, i) => (
                <tr key={i}>
                  <td className="bd-player">{f.player}</td>
                  <td className="bd-muted">{f.prop}</td>
                  <td className="mono">{f.line}</td>
                  <td className="mono"><TruePct value={f.pct} /></td>
                  <td><ResultPill result={f.result} /></td>
                  <td className="mono">{f.actual}</td>
                  <td className="bd-time mono">{f.logged}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}

function MultCell({ mult, n }) {
  // mult close to 1 = neutral; >1 = boost, <1 = nerf
  const delta = mult - 1;
  let bg = "transparent", color = "var(--text)";
  if (delta > 0.02) { bg = "rgba(34,197,94,.10)"; color = "#86EFAC"; }
  else if (delta < -0.02) { bg = "rgba(239,68,68,.10)"; color = "#FCA5A5"; }
  return (
    <div className="obs-mult-cell" style={{ background: bg }}>
      <span className="mono" style={{ color, fontWeight: 700 }}>{mult.toFixed(2)}x</span>
      <span className="obs-mult-n">n={n}</span>
    </div>
  );
}

function ResultPill({ result }) {
  const map = {
    hit:     { c: "var(--green)", bg: "rgba(34,197,94,.14)",  l: "HIT" },
    miss:    { c: "#FCA5A5",      bg: "rgba(239,68,68,.14)",  l: "MISS" },
    push:    { c: "#FDE68A",      bg: "rgba(250,204,21,.18)", l: "PUSH" },
    dnp:     { c: "#9CA3AF",      bg: "rgba(156,163,175,.14)",l: "DNP" },
    pending: { c: "#93C5FD",      bg: "rgba(96,165,250,.14)", l: "PENDING" },
  };
  const m = map[result];
  return <span className="obs-pill" style={{ color: m.c, background: m.bg }}>{m.l}</span>;
}

function heatColor(delta) {
  // -8 → red, 0 → neutral, +8 → green
  const t = Math.max(-1, Math.min(1, delta / 8));
  if (t > 0) return `rgba(34,197,94,${0.05 + t * 0.20})`;
  return `rgba(239,68,68,${0.05 + Math.abs(t) * 0.18})`;
}

// Calibration curves SVG
function CalibrationCurves() {
  const W = 800, H = 380, P = 50;
  const series = [
    { color: "#6366F1", name: "Points",   pts: [[0.5,0.51],[0.55,0.555],[0.6,0.605],[0.65,0.654],[0.7,0.715],[0.75,0.748]] },
    { color: "#22C55E", name: "Rebounds", pts: [[0.5,0.515],[0.55,0.548],[0.6,0.589],[0.65,0.642],[0.7,0.68],[0.75,0.755]] },
    { color: "#F59E0B", name: "Assists",  pts: [[0.5,0.502],[0.55,0.56],[0.6,0.605],[0.65,0.66],[0.7,0.71]] },
    { color: "#3DA9F0", name: "Combo",    pts: [[0.5,0.528],[0.55,0.575],[0.6,0.625],[0.65,0.668],[0.7,0.708]] },
    { color: "#A855F7", name: "Pitcher",  pts: [[0.5,0.515],[0.55,0.558],[0.6,0.615],[0.65,0.66]] },
  ];
  const x = (v) => P + (v - 0.5) / 0.3 * (W - 2 * P);
  const y = (v) => H - P - (v - 0.5) / 0.3 * (H - 2 * P);
  const grids = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8];

  return (
    <div className="cal-curves">
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height="380">
        {/* Grid */}
        {grids.map(g => (
          <g key={g}>
            <line x1={x(g)} x2={x(g)} y1={P} y2={H - P} stroke="rgba(255,255,255,.05)" />
            <line x1={P} x2={W - P} y1={y(g)} y2={y(g)} stroke="rgba(255,255,255,.05)" />
            <text x={x(g)} y={H - P + 18} fill="#7a7a8b" fontSize="11" textAnchor="middle" fontFamily="JetBrains Mono">{(g * 100).toFixed(0)}%</text>
            <text x={P - 10} y={y(g) + 4} fill="#7a7a8b" fontSize="11" textAnchor="end" fontFamily="JetBrains Mono">{(g * 100).toFixed(0)}%</text>
          </g>
        ))}
        {/* Perfect-cal diagonal */}
        <line x1={x(0.5)} y1={y(0.5)} x2={x(0.8)} y2={y(0.8)} stroke="rgba(255,255,255,.3)" strokeDasharray="4,4" strokeWidth="1.5" />
        {/* Curves */}
        {series.map(s => {
          const d = s.pts.map((p, i) => `${i === 0 ? "M" : "L"}${x(p[0]).toFixed(1)},${y(p[1]).toFixed(1)}`).join(" ");
          return (
            <g key={s.name}>
              <path d={d} fill="none" stroke={s.color} strokeWidth="2.5" />
              {s.pts.map((p, i) => (
                <circle key={i} cx={x(p[0])} cy={y(p[1])} r="3.5" fill={s.color} stroke="#0a0a0d" strokeWidth="1.5" />
              ))}
            </g>
          );
        })}
        {/* Axis titles */}
        <text x={W / 2} y={H - 8} fill="#7a7a8b" fontSize="12" textAnchor="middle">Predicted hit rate</text>
        <text x={14} y={H / 2} fill="#7a7a8b" fontSize="12" textAnchor="middle" transform={`rotate(-90 14 ${H / 2})`}>Actual hit rate</text>
      </svg>
    </div>
  );
}

Object.assign(window, { ObservatoryPage });
