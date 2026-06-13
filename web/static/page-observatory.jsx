// Observatory — calibration curves + per-prop multipliers + heatmap + market feed.
// Wired to /api/observatory, /api/observatory/multipliers, /api/calibration/heatmap,
// /api/calibration/curves.
const { useState: useStateO, useMemo: useMemoO } = React;

function _ago(iso) {
  if (!iso) return "—";
  const t = new Date(iso).getTime();
  if (isNaN(t)) return "—";
  const dm = Date.now() - t;
  const m = Math.round(dm / 60000);
  if (m < 1) return "just now";
  if (m < 60) return m + "m ago";
  const h = Math.round(m / 60);
  if (h < 24) return h + "h ago";
  const d = Math.round(h / 24);
  return d + "d ago";
}

// Fallback mock — used only if API returns empty (e.g. fresh deploy).
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
  const [leagueChips, setLeagueChips] = useState(new Set(["NBA", "WNBA", "MLB", "NHL", "NCAAB", "SOCCER"]));
  const [resultChips, setResultChips] = useState(new Set(["pending", "hit", "miss", "push", "dnp"]));
  const [multData, setMultData] = useState(null);
  const [heatData, setHeatData] = useState(null);
  const [feedData, setFeedData] = useState(null);
  const [curvesData, setCurvesData] = useState(null);
  const [feedExpanded, setFeedExpanded] = useState(false);
  const FEED_COLLAPSED_COUNT = 10;
  const [loadErr, setLoadErr] = useState("");

  useEffect(() => {
    let cancelled = false;
    const f = window.cpApi.apiFetch;
    Promise.allSettled([
      f("/api/observatory/multipliers"),
      f("/api/calibration/heatmap"),
      f("/api/observatory"),
      f("/api/calibration/curves"),
    ]).then(([m, h, o, c]) => {
      if (cancelled) return;
      if (m.status === "fulfilled") setMultData(m.value); else setLoadErr(prev => prev || ("multipliers: " + m.reason?.message));
      if (h.status === "fulfilled") setHeatData(h.value); else setLoadErr(prev => prev || ("heatmap: " + h.reason?.message));
      if (o.status === "fulfilled") setFeedData(o.value);  else setLoadErr(prev => prev || ("observatory: " + o.reason?.message));
      if (c.status === "fulfilled") setCurvesData(c.value); else {/* curves are optional for the panel */}
    });
    return () => { cancelled = true; };
  }, []);

  const togChip = (set, setter) => (v) => {
    const n = new Set(set);
    if (n.has(v)) n.delete(v); else n.add(v);
    setter(n);
  };

  // Multipliers: map server rows to {league, prop, over, overN, under, underN}.
  // We NEVER substitute mock values — if the API is empty, the panel shows an
  // empty state. Showing fake numbers in a calibration table would be worse
  // than showing nothing.
  const multipliers = useMemo(() => {
    if (!multData) return null;          // still loading
    const anchor = multData.anchor || 0.6;
    const rows = multData.rows || [];
    return rows.map(r => ({
      league: r.league,
      prop:   r.prop,
      over:   r.over ? r.over.q / anchor : null,
      overN:  r.over ? Math.round(r.over.n_eff) : 0,
      under:  r.under ? r.under.q / anchor : null,
      underN: r.under ? Math.round(r.under.n_eff) : 0,
    }));
  }, [multData]);

  // Heatmap: render every (league, prop, side) row the API returns. No
  // collapsing — over and under are separate calibration buckets and the
  // user wants to see both. No mock fallback.
  const heatRows = useMemo(() => {
    if (!heatData) return null;           // still loading
    const buckets = heatData.buckets || [];
    const bands = buckets.length
      ? buckets.map(b => b.label)
      : ["50-55%", "55-60%", "60-65%", "65-70%", "70-75%", "75-80%"];
    const bandCenters = buckets.length
      ? buckets.map(b => ((b.lo + b.hi) / 2) * 100)
      : [52.5, 57.5, 62.5, 67.5, 72.5, 77.5];
    const rows = (heatData.rows || []).map(r => ({
      league: r.league,
      prop:   r.prop,
      side:   r.side,                     // "over" | "under"
      n_eff:  r.n_eff,
      // Each cell carries both the actual hit rate (%) and its per-cell
      // effective sample so the user can see how trustworthy each bucket
      // is, not just the row average.
      row:    (r.cells || []).map(c => c
        ? { actual: c.actual * 100, n: Math.round(c.n_eff || 0) }
        : null),
    }));
    // Sort by league → prop → side so over/under rows sit next to each other.
    rows.sort((a, b) =>
      a.league.localeCompare(b.league) ||
      a.prop.localeCompare(b.prop) ||
      a.side.localeCompare(b.side)
    );
    return { bands, bandCenters, rows };
  }, [heatData]);

  // Feed: server returns market_observatory rows directly. No mock fallback.
  const feedRows = useMemo(() => {
    if (!feedData) return null;           // still loading
    return (feedData || [])
      .filter(r => leagueChips.has(r.league))
      .filter(r => resultChips.has(r.result || "pending"))
      .map(r => ({
        player: r.player,
        league: r.league,
        prop:   `${r.prop} ${(r.side || "").toUpperCase().startsWith("O") ? "O" : "U"}${r.line}`,
        line:   r.line,
        pct:    (r.true_prob || 0) * 100,
        result: r.result || "pending",
        actual: r.stat_actual != null ? String(r.stat_actual) : "—",
        logged: _ago(r.created_at),
      }));
  }, [feedData, leagueChips, resultChips]);

  // Calibration curves: one line per LEAGUE (sport). We read ONLY the
  // per-league level of /api/calibration/curves (isotonic.leagues) — no
  // per-prop or per-(league, prop, side) fallback so the chart can never
  // silently revert to prop-family curves.
  const curveSeries = useMemo(() => {
    const iso = curvesData?.isotonic || curvesData || null;
    if (!iso) return null;
    const leagueLevels = iso.leagues || {};

    // Stable per-league colors — match LeaguePill where possible.
    const leagueColor = {
      NBA:   "#F97316",
      WNBA:  "#C084FC",
      NHL:   "#60A5FA",
      MLB:   "#34D399",
      NFL:   "#FBBF24",
      NCAAB: "#A78BFA",
      NCAAF: "#F87171",
      SOCCER: "#10B981",
    };
    const fallback = ["#818cf8", "#22C55E", "#F59E0B", "#3DA9F0", "#A855F7", "#EC4899", "#14B8A6", "#FB7185"];

    let ci = 0;
    const series = [];
    for (const [lg, lvl] of Object.entries(leagueLevels)) {
      if (!lvl || !Array.isArray(lvl.curve) || lvl.curve.length < 2) continue;
      const pts = lvl.curve
        .map(p => [Number(p[0]), Number(p[1])])
        .filter(p => !isNaN(p[0]) && !isNaN(p[1]) && p[0] >= 0.30 && p[0] <= 0.95);
      if (pts.length < 2) continue;
      series.push({
        name:  lg,
        color: leagueColor[lg] || fallback[ci++ % fallback.length],
        pts,
        n_eff: Math.round(lvl.n_eff || 0),
      });
    }
    series.sort((a, b) => b.n_eff - a.n_eff);
    return series.length ? series : null;
  }, [curvesData]);

  return (
    <main className="bd-page obs-page">
      {loadErr && (
        <div style={{padding:"10px 14px",margin:"0 0 14px",background:"rgba(239,68,68,.10)",border:"1px solid rgba(239,68,68,.25)",borderRadius:10,fontSize:13,color:"#FCA5A5"}}>
          Partial load: {loadErr}
        </div>
      )}
      <section className="an-panel">
        <div className="an-panel-h">
          <h3>Calibration Curves</h3>
          <span className="an-section-sub">Predicted vs. actual hit rate, by league. Closer to the diagonal = better calibrated.</span>
        </div>
        {curvesData == null ? (
          <ObsPlaceholder>Loading calibration state…</ObsPlaceholder>
        ) : !curveSeries ? (
          <ObsPlaceholder>No fitted calibration curves yet — once your model has enough resolved props per league, the curves appear here.</ObsPlaceholder>
        ) : (
          <>
            <CalibrationCurves series={curveSeries} />
            <div className="cal-legend">
              {curveSeries.map(s => (
                <span key={s.name} className="cal-legend-item">
                  <i style={{background:s.color}} /> {s.name} <em style={{color:"var(--text-3)",fontStyle:"normal",marginLeft:4}}>n={s.n_eff}</em>
                </span>
              ))}
              <span className="cal-legend-item"><i className="cal-legend-dashed" /> Perfect calibration</span>
            </div>
          </>
        )}
      </section>

      <section className="an-panel">
        <div className="an-panel-h">
          <h3>Per-Prop Calibration</h3>
          <span className="an-section-sub">Boost / nerf applied to each (league, prop, side) bucket at p=0.60. 1.00x = no adjustment. n = effective sample.</span>
        </div>
        {multipliers == null ? (
          <ObsPlaceholder>Loading calibration multipliers…</ObsPlaceholder>
        ) : multipliers.length === 0 ? (
          <ObsPlaceholder>No fitted multipliers yet for any (league, prop, side) bucket.</ObsPlaceholder>
        ) : (
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
                {multipliers.map((m, i) => (
                  <tr key={i}>
                    <td><LeaguePill league={m.league} /></td>
                    <td>{m.prop}</td>
                    <td>{m.over != null ? <MultCell mult={m.over} n={m.overN} /> : <span className="obs-heat-cell is-empty">—</span>}</td>
                    <td>{m.under != null ? <MultCell mult={m.under} n={m.underN} /> : <span className="obs-heat-cell is-empty">—</span>}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="an-panel">
        <div className="an-panel-h">
          <h3>Prop Hit Rate by Expected Probability</h3>
          <span className="an-section-sub">One row per (league, prop, side). Columns: expected hit rate bands. Cells: actual recency-weighted hit rate from resolved observations — greener as the hit rate climbs above 56%.</span>
        </div>
        {heatRows == null ? (
          <ObsPlaceholder>Loading heatmap…</ObsPlaceholder>
        ) : heatRows.rows.length === 0 ? (
          <ObsPlaceholder>No resolved props per bucket meet the minimum effective sample yet.</ObsPlaceholder>
        ) : (
          <div className="obs-heat-wrap">
            <table className="obs-heat">
              <thead>
                <tr>
                  <th>League · Prop · Side</th>
                  {heatRows.bands.map(b => <th key={b} className="obs-th-c">{b}</th>)}
                  <th className="obs-th-c">n</th>
                </tr>
              </thead>
              <tbody>
                {heatRows.rows.map((row, i) => (
                  <tr key={i}>
                    <td className="obs-heat-prop">
                      <LeaguePill league={row.league} />
                      <span>{row.prop}</span>
                      <span className={"bt-leg-side bt-leg-side-" + row.side}>{row.side === "over" ? "O" : "U"}</span>
                    </td>
                    {row.row.map((cell, j) => {
                      // Every cell shows n=… even when empty, so the user can
                      // see exactly which buckets have no data.
                      if (cell == null) {
                        return (
                          <td key={j} className="obs-heat-cell is-empty">
                            <span className="obs-heat-v mono">—</span>
                            <span className="obs-heat-n mono">n=0</span>
                          </td>
                        );
                      }
                      return (
                        <td key={j} className="obs-heat-cell" style={{ background: heatColor(cell.actual) }}>
                          <span className="obs-heat-v mono">{cell.actual.toFixed(1)}%</span>
                          <span className="obs-heat-n mono">n={cell.n}</span>
                        </td>
                      );
                    })}
                    <td className="obs-heat-cell mono" style={{color:"var(--text-3)"}}>{Math.round(row.n_eff)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
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
              {["NBA", "WNBA", "MLB", "NHL", "NCAAB", "SOCCER"].map(l => (
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
        {feedRows == null ? (
          <ObsPlaceholder>Loading market feed…</ObsPlaceholder>
        ) : feedRows.length === 0 ? (
          <ObsPlaceholder>No observations match the current league / result filters.</ObsPlaceholder>
        ) : (
        <>
        <div className="bd-tbl-wrap">
          <table className="bd-tbl">
            <thead>
              <tr><th>Player</th><th>League</th><th>Prop</th><th>Line</th><th>Target Prob</th><th>Result</th><th>Actual</th><th>Logged</th></tr>
            </thead>
            <tbody>
              {(feedExpanded ? feedRows : feedRows.slice(0, FEED_COLLAPSED_COUNT)).map((f, i) => (
                <tr key={i}>
                  <td className="bd-player">{f.player}</td>
                  <td><LeaguePill league={f.league} /></td>
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
        {feedRows.length > FEED_COLLAPSED_COUNT && (
          <div style={{display:"flex",justifyContent:"center",marginTop:14}}>
            <button
              className="cp-btn cp-btn-ghost cp-btn-sm"
              onClick={() => setFeedExpanded(v => !v)}
            >
              {feedExpanded
                ? `Show fewer (top ${FEED_COLLAPSED_COUNT})`
                : `Show all (${feedRows.length})`}
            </button>
          </div>
        )}
        </>
        )}
      </section>
    </main>
  );
}

function ObsPlaceholder({ children }) {
  return (
    <div style={{
      padding: "32px 18px",
      textAlign: "center",
      color: "var(--text-3)",
      fontSize: 13.5,
      lineHeight: 1.55,
      background: "rgba(255,255,255,.015)",
      border: "1px dashed var(--hair)",
      borderRadius: 10,
    }}>{children}</div>
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

function heatColor(actualPct) {
  // Color scales with the ABSOLUTE bucket hit rate, not delta-vs-expected.
  // Pivot is 56% (slightly above the Power-6 leg break-even of 54.08%):
  //   - 56% = neutral / no tint
  //   - climbs to fully-saturated green by 80%+
  //   - drops to fully-saturated red by 32% or lower
  const PIVOT = 56, GREEN_FULL = 80, RED_FULL = 32;
  if (actualPct == null || isNaN(actualPct)) return "transparent";
  if (actualPct >= PIVOT) {
    const t = Math.min(1, (actualPct - PIVOT) / (GREEN_FULL - PIVOT));
    return `rgba(34,197,94,${0.06 + t * 0.32})`;
  }
  const t = Math.min(1, (PIVOT - actualPct) / (PIVOT - RED_FULL));
  return `rgba(239,68,68,${0.06 + t * 0.30})`;
}

// Calibration curves SVG — responsive, fills its container.
// Axis is data-driven: the visible window auto-fits to the min/max of every
// curve so sparse early-season data isn't squeezed into a 30-50% corner.
function CalibrationCurves({ series }) {
  const containerRef = useRef(null);
  const [width, setWidth] = useState(960);
  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver(es => {
      const w = Math.max(560, Math.round(es[0].contentRect.width));
      setWidth(w);
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  // Compute axis range from data, clamped to a sensible window.
  let lo = 0.4, hi = 0.85;
  if (series && series.length) {
    let minV = 1, maxV = 0;
    for (const s of series) {
      for (const [a, b] of s.pts) {
        if (a < minV) minV = a;
        if (b < minV) minV = b;
        if (a > maxV) maxV = a;
        if (b > maxV) maxV = b;
      }
    }
    // Pad ±2 percentage points, snap to nearest 5%, clamp to [0.30, 0.95].
    lo = Math.max(0.30, Math.floor((minV - 0.02) * 20) / 20);
    hi = Math.min(0.95, Math.ceil((maxV + 0.02) * 20) / 20);
    if (hi - lo < 0.15) hi = Math.min(0.95, lo + 0.15);
  }

  const W = Math.max(560, width);
  const H = Math.round(Math.min(520, Math.max(360, W * 0.46)));
  const padL = 72, padR = 28, padT = 28, padB = 56;
  const x = (v) => padL + ((v - lo) / (hi - lo)) * (W - padL - padR);
  const y = (v) => H - padB - ((v - lo) / (hi - lo)) * (H - padT - padB);

  // Grid: tick every 5 percentage points across the visible window.
  const ticks = [];
  for (let v = Math.round(lo * 20) / 20; v <= hi + 1e-9; v += 0.05) {
    ticks.push(Number(v.toFixed(2)));
  }

  return (
    <div className="cal-curves" ref={containerRef}>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H} preserveAspectRatio="none"
           style={{ display: "block", fontFamily: "Inter,system-ui,sans-serif" }}>
        <defs>
          <linearGradient id="cal-plot-bg" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="#0a0a12" stopOpacity="0.0" />
            <stop offset="100%" stopColor="#0a0a12" stopOpacity="0.6" />
          </linearGradient>
        </defs>

        {/* Plot frame */}
        <rect x={padL} y={padT} width={W - padL - padR} height={H - padT - padB}
              fill="url(#cal-plot-bg)" stroke="rgba(255,255,255,.06)" strokeWidth="1" rx="6" />

        {/* Grid + tick labels */}
        {ticks.map(g => (
          <g key={g}>
            <line x1={x(g)} x2={x(g)} y1={padT} y2={H - padB}
                  stroke="rgba(255,255,255,.05)" strokeWidth="1" />
            <line x1={padL} x2={W - padR} y1={y(g)} y2={y(g)}
                  stroke="rgba(255,255,255,.05)" strokeWidth="1" />
            <text x={x(g)} y={H - padB + 20} fill="#9ca3af" fontSize="11.5"
                  fontFamily="JetBrains Mono,ui-monospace,monospace"
                  textAnchor="middle">{Math.round(g * 100)}%</text>
            <text x={padL - 12} y={y(g) + 4} fill="#9ca3af" fontSize="11.5"
                  fontFamily="JetBrains Mono,ui-monospace,monospace"
                  textAnchor="end">{Math.round(g * 100)}%</text>
          </g>
        ))}

        {/* Perfect-calibration diagonal */}
        <line x1={x(lo)} y1={y(lo)} x2={x(hi)} y2={y(hi)}
              stroke="rgba(255,255,255,.28)" strokeDasharray="6,5" strokeWidth="1.5" />
        <text x={x(hi) - 6} y={y(hi) - 8} fill="rgba(255,255,255,.45)"
              fontSize="10.5" textAnchor="end" fontStyle="italic">y = x</text>

        {/* Curves: smoothed paths + endpoint dots */}
        {series && series.map(s => {
          const pts = s.pts.slice().sort((a, b) => a[0] - b[0]);
          const d = pts.map((p, i) => `${i === 0 ? "M" : "L"}${x(p[0]).toFixed(1)},${y(p[1]).toFixed(1)}`).join(" ");
          return (
            <g key={s.name}>
              <path d={d} fill="none" stroke={s.color} strokeWidth="2.5"
                    strokeLinejoin="round" strokeLinecap="round"
                    style={{ filter: `drop-shadow(0 0 6px ${s.color}55)` }} />
              {pts.map((p, i) => (
                <circle key={i} cx={x(p[0])} cy={y(p[1])} r="3.5"
                        fill={s.color} stroke="#0a0a0d" strokeWidth="1.5" />
              ))}
            </g>
          );
        })}

        {/* Axis titles */}
        <text x={padL + (W - padL - padR) / 2} y={H - 12} fill="#cbd5e1"
              fontSize="12.5" fontWeight="600" textAnchor="middle"
              letterSpacing=".02em">Predicted hit rate</text>
        <text x={18} y={padT + (H - padT - padB) / 2} fill="#cbd5e1"
              fontSize="12.5" fontWeight="600" textAnchor="middle"
              letterSpacing=".02em"
              transform={`rotate(-90 18 ${padT + (H - padT - padB) / 2})`}>Actual hit rate</text>
      </svg>
    </div>
  );
}

Object.assign(window, { ObservatoryPage });
