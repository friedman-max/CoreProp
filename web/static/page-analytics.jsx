// Analytics — Cumulative P&L chart + Brier/CLV stats. Wired to /api/analytics.
const { useState: useStateA, useMemo: useMemoA, useRef: useRefA, useEffect: useEffectA } = React;

const RANGES = ["1D", "1W", "1M", "3M", "1Y", "MAX"];

function _windowStartMs(range, latestMs) {
  if (range === "MAX") return -Infinity;
  const day = 86400000;
  const days = { "1D": 1, "1W": 7, "1M": 30, "3M": 90, "1Y": 365 }[range] || 30;
  return latestMs - days * day;
}

function AnalyticsPage() {
  const [range, setRange] = useState("1M");
  const [hover, setHover] = useState(null);
  const [data, setData] = useState(null);
  const [loadState, setLoadState] = useState("loading");
  const [errMsg, setErrMsg] = useState("");

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const d = await window.cpApi.apiFetch("/api/analytics");
        if (cancelled) return;
        setData(d);
        setLoadState("ok");
      } catch (ex) {
        if (cancelled) return;
        setErrMsg(ex.message || "Failed to load analytics.");
        setLoadState("error");
      }
    })();
    return () => { cancelled = true; };
  }, []);

  // Full P&L series mapped from pnl_timeline (already cumulative).
  // Each point also carries `delta` = this slip's own profit (payout - stake)
  // so the chart can colour the dot by win/loss and the hover summary can
  // attribute the change to a specific resolved slip.
  const fullSeries = useMemo(() => {
    if (!data?.pnl_timeline) return [];
    return data.pnl_timeline.map(p => ({
      date:  new Date(p.timestamp),
      pnl:   p.cum_pnl,                              // running total in units
      delta: typeof p.pnl === "number" ? p.pnl : 0, // this slip's contribution
      slipId: p.slip_id || null,
    })).filter(p => !isNaN(p.date.getTime()));
  }, [data]);

  const filtered = useMemo(() => {
    if (!fullSeries.length) return [];

    // Slice to the window, then REBASE so the window opens at 0u. Viewing a
    // single timeframe in a vacuum: a +18u week reads 0 → +18, not
    // −65 → −47. Baseline = the cumulative P&L immediately *before* the
    // window's first slip (or that slip's own pre-delta value if the window
    // includes the very first slip).
    let startIdx = 0;
    if (range !== "MAX") {
      const latest = fullSeries[fullSeries.length - 1].date.getTime();
      const cutoff = _windowStartMs(range, latest);
      startIdx = fullSeries.findIndex(p => p.date.getTime() >= cutoff);
      if (startIdx < 0) return [];
    }
    const first = fullSeries[startIdx];
    const baseline = startIdx > 0
      ? fullSeries[startIdx - 1].pnl          // P&L right before the window
      : (first.pnl - (first.delta || 0));     // pre-first-slip = ~0
    return fullSeries.slice(startIdx).map(p => ({
      ...p,
      pnl: p.pnl - baseline,                  // rebased: window starts at 0u
      cumAbs: p.pnl,                          // keep absolute for reference
    }));
  }, [fullSeries, range]);

  // Window stats: filter resolved_legs by the same window and recompute.
  const windowLegs = useMemo(() => {
    if (!data?.resolved_legs) return [];
    if (range === "MAX") return data.resolved_legs;
    if (!fullSeries.length) return data.resolved_legs;
    const latest = fullSeries[fullSeries.length - 1].date.getTime();
    const cutoff = _windowStartMs(range, latest);
    return data.resolved_legs.filter(l => {
      const t = l.timestamp ? new Date(l.timestamp).getTime() : NaN;
      return !isNaN(t) && t >= cutoff;
    });
  }, [data, range, fullSeries]);

  const windowClv = useMemo(() => {
    if (!data?.clv_legs) return [];
    if (range === "MAX") return data.clv_legs;
    if (!fullSeries.length) return data.clv_legs;
    const latest = fullSeries[fullSeries.length - 1].date.getTime();
    const cutoff = _windowStartMs(range, latest);
    return data.clv_legs.filter(l => {
      const t = l.timestamp ? new Date(l.timestamp).getTime() : NaN;
      return !isNaN(t) && t >= cutoff;
    });
  }, [data, range, fullSeries]);

  // Series is already rebased to open at 0u, so the window's net P&L is just
  // the last rebased cumulative value.
  const totalPnL = filtered.length ? filtered[filtered.length - 1].pnl : 0;

  const allLegs = windowLegs;
  const brier = allLegs.length
    ? allLegs.reduce((a, l) => a + Math.pow(l.true_prob - l.outcome, 2), 0) / allLegs.length
    : 0;
  const logLoss = allLegs.length
    ? -allLegs.reduce((a, l) => {
        const p = Math.min(0.99, Math.max(0.01, l.true_prob));
        return a + (l.outcome * Math.log(p) + (1 - l.outcome) * Math.log(1 - p));
      }, 0) / allLegs.length
    : 0;
  const avgPred = allLegs.length ? (allLegs.reduce((a, l) => a + l.true_prob, 0) / allLegs.length) * 100 : 0;
  const rawHit = allLegs.length ? (allLegs.filter(l => l.outcome === 1).length / allLegs.length) * 100 : 0;
  const delta = rawHit - avgPred;

  // Reliability curve — the live calibration gauge. Bin the window's legs by
  // predicted prob (5% bins), and for each populated bin compare mean
  // predicted (x) against realized hit rate (y). On a perfectly-calibrated
  // model every point sits on the y = x diagonal; points BELOW the diagonal
  // are overconfident (predicted > realized), the exact 2x-overconfidence
  // FINDINGS §2 describes. This is what CALIBRATION_MAP_ENABLED is meant to
  // pull back onto the line — watch these dots hug the diagonal after you
  // enable and refit.
  const reliability = useMemo(() => {
    const BINW = 0.05;
    const acc = {}; // bin index → { n, hits, psum }
    for (const l of allLegs) {
      const p = l.true_prob;
      if (typeof p !== "number" || p <= 0 || p >= 1) continue;
      const idx = Math.floor(p / BINW);
      const b = acc[idx] || (acc[idx] = { n: 0, hits: 0, psum: 0 });
      b.n += 1; b.hits += l.outcome; b.psum += p;
    }
    return Object.keys(acc).map(k => {
      const b = acc[k];
      return { pred: b.psum / b.n, actual: b.hits / b.n, n: b.n };
    }).sort((a, c) => a.pred - c.pred);
  }, [allLegs]);

  // Expected Calibration Error: sample-weighted mean |predicted − realized|
  // across the populated bins. One honest number for "how far off are the
  // odds" — 0 = perfectly calibrated. Complements Brier (which conflates
  // calibration with resolution); ECE isolates the calibration gap the map
  // targets.
  const ece = allLegs.length && reliability.length
    ? reliability.reduce((a, b) => a + b.n * Math.abs(b.pred - b.actual), 0) / allLegs.length
    : 0;

  // CLV: prefer the windowed per-leg rows. But clv_legs only carries a row
  // when BOTH closing_prob and clv_pct are populated AND the slip timestamp
  // resolved server-side — so the windowed array can come back empty even
  // when the model has tracked CLV. In that case (or for MAX) fall back to
  // the backend's all-time aggregate (clv_plus_rate / avg_clv_pct /
  // n_clv_tracked) so the CLV cards always show a value when any exists.
  // clv_pct is a probability delta (closing_prob − true_prob), i.e. a
  // fraction like 0.025 → 2.5 percentage points, so it is ×100 for display.
  const haveWindowClv = windowClv.length > 0;
  const clvCount = haveWindowClv ? windowClv.length : (data?.n_clv_tracked || 0);
  // +CLV rate is over MOVED legs only (|clv| > eps), matching the backend's
  // _summarize_clv: a stale 0.0 (closing == bet-time, capture never re-fired)
  // is neither a win nor a loss against the close, so counting it in the
  // denominator dilutes the rate. Every leg is seeded clv_pct=0 at insert, so
  // including stales made the window +CLV rate chronically understated and
  // flipped its good/bad tone vs the backend fallback path. (avg_clv_pct below
  // intentionally still includes stales, matching the backend's all-in mean.)
  const windowClvMoved = haveWindowClv ? windowClv.filter(l => Math.abs(l.clv_pct) > 1e-6) : [];
  const clvPos = haveWindowClv
    ? (windowClvMoved.length ? (windowClvMoved.filter(l => l.clv_pct > 0).length / windowClvMoved.length) * 100 : 0)
    : (typeof data?.clv_plus_rate === "number" ? data.clv_plus_rate * 100 : 0);
  const clvAvg = haveWindowClv
    ? (windowClv.reduce((a, l) => a + l.clv_pct, 0) / windowClv.length) * 100
    : (typeof data?.avg_clv_pct === "number" ? data.avg_clv_pct * 100 : 0);
  // Note whether the CLV figures are window-scoped or the all-time fallback
  // so the section subtitle can be honest about it.
  const clvIsAllTime = !haveWindowClv && clvCount > 0;

  const hovered = hover ?? filtered[filtered.length - 1];

  if (loadState === "loading") {
    return <main className="bd-page an-page"><div style={{padding:"32px",color:"var(--text-3)"}}>Loading analytics…</div></main>;
  }
  if (loadState === "error") {
    return <main className="bd-page an-page"><div style={{padding:"32px",color:"#FCA5A5"}}>Error: {errMsg}</div></main>;
  }
  if (!fullSeries.length) {
    return (
      <main className="bd-page an-page">
        <div className="an-panel" style={{padding:"32px",textAlign:"center",color:"var(--text-2)"}}>
          No resolved slips yet. Once your logged slips settle, your equity curve and calibration stats appear here.
        </div>
      </main>
    );
  }

  // When the user is hovering the chart, the big number on top shows the
  // cumulative P&L (in units) AT that timestamp. When not hovering, it shows
  // the window delta (end − start). The right-side caption shows either the
  // hovered date plus that slip's individual win/loss (e.g. "+5.00u this
  // slip") or the full window range.
  const headerNumber = hover ? hover.pnl : totalPnL;
  const headerLabel  = hover
    ? hover.date.toLocaleString(undefined, { month: "short", day: "numeric", year: "numeric", hour: "numeric", minute: "2-digit" })
    : (filtered.length
        ? `${filtered[0].date.toLocaleDateString(undefined,{month:"short",day:"numeric"})} → ${filtered[filtered.length-1].date.toLocaleDateString(undefined,{month:"short",day:"numeric",year:"numeric"})}`
        : "");
  const hoverDeltaText = hover && typeof hover.delta === "number"
    ? `${hover.delta >= 0 ? "+" : ""}${hover.delta.toFixed(2)}u this slip`
    : null;

  // By this point the loading/error/empty states have already returned early
  // above, so stat cards only render with real data — but keep an explicit
  // flag so the shimmer contract with StatCard stays consistent.
  const anLoading = false;

  return (
    <main className="bd-page an-page">
      <div className="an-panel">
        <div className="pnl-header">
          <div>
            <div className="pnl-title">
              {hover ? "P&L at hover" : "Cumulative P&L"}
              <span className="pnl-title-sub">{hover ? "units at this point in time" : "Unit Stake / Slip · window delta"}</span>
            </div>
            <div className={"pnl-total " + (headerNumber >= 0 ? "tone-good" : "tone-bad")}>
              {headerNumber >= 0 ? "+" : ""}{headerNumber.toFixed(2)}u
            </div>
          </div>
          <div className="pnl-hover">
            <div>{headerLabel}</div>
            {hoverDeltaText && (
              <div style={{
                marginTop: 4,
                fontSize: 13,
                fontFamily: "JetBrains Mono,ui-monospace,monospace",
                fontWeight: 700,
                color: hover.delta > 0 ? "#22C55E" : hover.delta < 0 ? "#EF4444" : "#FBBF24",
              }}>{hoverDeltaText}</div>
            )}
          </div>
        </div>

        <PnLChart series={filtered} onHover={setHover} hover={hover} />

        <div className="pnl-range">
          {RANGES.map(r => (
            <button key={r} className={"pnl-range-btn " + (range === r ? "is-on" : "")} onClick={() => setRange(r)}>{r}</button>
          ))}
        </div>

        <div className="an-section-h">Window stats <span className="an-section-sub">stats for: {range}</span></div>
        <div className="bt-summary">
          <StatCard loading={anLoading} label="Brier Score" value={brier.toFixed(4)} tone={brier < 0.25 ? "good" : "neutral"} />
          <StatCard loading={anLoading} label="Log Loss" value={logLoss.toFixed(4)} />
          <StatCard loading={anLoading} label="Resolved Legs" value={String(allLegs.length)} />
          <StatCard loading={anLoading} label="Raw Hit Rate" value={rawHit.toFixed(1) + "%"} tone={rawHit >= 54.08 ? "good" : "bad"} />
          <StatCard loading={anLoading} label="Avg Predicted Prob" value={avgPred.toFixed(1) + "%"} />
          <StatCard loading={anLoading} label="Hit Rate Delta" value={(delta >= 0 ? "+" : "") + delta.toFixed(1) + "%"} tone={delta >= 0 ? "good" : "bad"} />
          <StatCard loading={anLoading} label="Calibration Error" sub="ECE, lower better" value={(ece * 100).toFixed(1) + "%"} tone={ece <= 0.03 ? "good" : ece <= 0.06 ? "neutral" : "bad"} />
        </div>

        <div className="an-section-h">Calibration reliability <span className="an-section-sub">predicted vs realized · dots on the diagonal = accurate · below = overconfident</span></div>
        <ReliabilityChart points={reliability} />

        <div className="an-section-h">Closing Line Value <span className="an-section-sub">if CLV+ {`>`} 50% &amp; Avg CLV% {`>`} 0, you're beating the market{clvIsAllTime ? " · all-time (no CLV tracked in this window)" : ""}</span></div>
        <div className="bt-summary">
          <StatCard loading={anLoading} label="Tracked" value={String(clvCount)} />
          <StatCard loading={anLoading} label="CLV+ Rate" value={clvCount ? clvPos.toFixed(1) + "%" : "—"} tone={clvPos >= 50 ? "good" : "bad"} />
          <StatCard loading={anLoading} label="Avg CLV%" value={clvCount ? (clvAvg >= 0 ? "+" : "") + clvAvg.toFixed(1) + "%" : "—"} tone={clvAvg >= 0 ? "good" : "bad"} />
        </div>
      </div>
    </main>
  );
}

function PnLChart({ series, onHover, hover }) {
  const ref = useRef(null);
  const [size, setSize] = useState({ w: 800, h: 320 });
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver(es => {
      const r = es[0].contentRect;
      setSize({ w: Math.max(600, r.width), h: 320 });
    });
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);

  if (!series.length) return <div ref={ref} className="pnl-chart" />;

  const padL = 56, padR = 28, padT = 18, padB = 34;
  const W = size.w, H = size.h;
  // X is positioned by real timestamp, not by index, so gaps between slips
  // map to real time on the x-axis.
  const tMin = series[0].date.getTime();
  const tMax = series[series.length - 1].date.getTime();
  const tSpan = Math.max(1, tMax - tMin);
  const xs = (i) => padL + ((series[i].date.getTime() - tMin) / tSpan) * (W - padL - padR);
  const min = Math.min(...series.map(p => p.pnl), 0);
  const max = Math.max(...series.map(p => p.pnl), 0);
  const yRange = max - min || 1;
  const ys = (v) => padT + (1 - (v - min) / yRange) * (H - padT - padB);
  const zeroY = ys(0);

  // STEP-AFTER path: bankroll stays constant between resolved slips, then
  // jumps when the next one settles. Each segment = one slip's contribution.
  //   start: (x0, ys(0))
  //   for each point: horizontal hold to that x, then vertical jump to ys(pnl)
  let pathD = `M${xs(0).toFixed(1)},${ys(0).toFixed(1)} `;
  let prevY = ys(0);
  for (let i = 0; i < series.length; i++) {
    const xi = xs(i);
    const yi = ys(series[i].pnl);
    pathD += `L${xi.toFixed(1)},${prevY.toFixed(1)} L${xi.toFixed(1)},${yi.toFixed(1)} `;
    prevY = yi;
  }
  const lastX = xs(series.length - 1);
  const fillD = `${pathD} L${lastX.toFixed(1)},${zeroY} L${xs(0).toFixed(1)},${zeroY} Z`;

  const isUp = series[series.length - 1].pnl >= 0;
  const lineColor = isUp ? "#22C55E" : "#EF4444";

  // Y-axis ticks
  const yTicks = [];
  const step = niceStep(yRange / 4);
  for (let v = Math.ceil(min / step) * step; v <= max; v += step) yTicks.push(v);

  // X-axis: 4-5 evenly-spaced date labels by time, not index.
  const xTickCount = Math.min(5, series.length);
  const xTicks = [];
  for (let i = 0; i < xTickCount; i++) {
    const t = tMin + (i / (xTickCount - 1)) * tSpan;
    const x = padL + (i / (xTickCount - 1)) * (W - padL - padR);
    xTicks.push({ x, label: new Date(t).toLocaleDateString(undefined, { month: "short", day: "numeric" }) });
  }

  // Find the slip the cursor is nearest to in time.
  const onMove = (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const x = (e.clientX - rect.left) * (W / rect.width); // normalize to viewBox
    const ratio = Math.min(1, Math.max(0, (x - padL) / (W - padL - padR)));
    const tHover = tMin + ratio * tSpan;
    let best = 0, bestDt = Infinity;
    for (let i = 0; i < series.length; i++) {
      const dt = Math.abs(series[i].date.getTime() - tHover);
      if (dt < bestDt) { bestDt = dt; best = i; }
    }
    onHover(series[best]);
  };

  const hoverIdx = hover ? series.findIndex(p => p.date.getTime() === hover.date.getTime() && p.pnl === hover.pnl) : -1;

  // Per-slip dot color: green if this slip won, red if it lost, yellow if push.
  const dotColor = (delta) => delta > 0 ? "#22C55E" : delta < 0 ? "#EF4444" : "#FBBF24";

  return (
    <div ref={ref} className="pnl-chart">
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H} onMouseMove={onMove} onMouseLeave={() => onHover(null)}>
        <defs>
          <linearGradient id="pnl-fill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor={lineColor} stopOpacity="0.24" />
            <stop offset="100%" stopColor={lineColor} stopOpacity="0" />
          </linearGradient>
        </defs>
        {/* Y grid */}
        {yTicks.map((v, i) => (
          <g key={i}>
            <line x1={padL} x2={W - padR} y1={ys(v)} y2={ys(v)} stroke="rgba(255,255,255,.06)" strokeWidth="1" />
            <text x={padL - 10} y={ys(v) + 4} fill="#9ca3af" fontSize="11.5" textAnchor="end" fontFamily="JetBrains Mono,ui-monospace,monospace">
              {v > 0 ? "+" : ""}{v.toFixed(1)}u
            </text>
          </g>
        ))}
        {/* Zero line */}
        <line x1={padL} x2={W - padR} y1={zeroY} y2={zeroY} stroke="rgba(255,255,255,.18)" strokeDasharray="3,3" />

        {/* Filled area under the step path */}
        <path d={fillD} fill="url(#pnl-fill)" />

        {/* Step-after equity curve */}
        <path d={pathD} fill="none" stroke={lineColor} strokeWidth="2.25" strokeLinejoin="miter" strokeLinecap="square" />

        {/* Per-slip dots colored by individual outcome */}
        {series.map((p, i) => (
          <circle
            key={i}
            cx={xs(i)}
            cy={ys(p.pnl)}
            r={hoverIdx === i ? 5 : 3}
            fill={dotColor(p.delta)}
            stroke="#0a0a0d"
            strokeWidth="1.25"
          />
        ))}

        {/* Hover crosshair */}
        {hoverIdx >= 0 && (
          <g>
            <line x1={xs(hoverIdx)} x2={xs(hoverIdx)} y1={padT} y2={H - padB}
                  stroke="rgba(255,255,255,.45)" strokeWidth="1.25" strokeDasharray="4,4" />
            <circle cx={xs(hoverIdx)} cy={ys(series[hoverIdx].pnl)} r="10"
                    fill={dotColor(series[hoverIdx].delta)} fillOpacity="0.22" />
            <circle cx={xs(hoverIdx)} cy={ys(series[hoverIdx].pnl)} r="5.5"
                    fill={dotColor(series[hoverIdx].delta)} stroke="#0a0a0d" strokeWidth="2" />
          </g>
        )}

        {/* X tick labels */}
        {xTicks.map((t, i) => (
          <text key={i} x={t.x} y={H - 10} fill="#9ca3af" fontSize="11.5" textAnchor="middle" fontFamily="JetBrains Mono,ui-monospace,monospace">{t.label}</text>
        ))}
      </svg>
    </div>
  );
}

function ReliabilityChart({ points }) {
  const ref = useRef(null);
  const [size, setSize] = useState({ w: 800, h: 320 });
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver(es => {
      const r = es[0].contentRect;
      setSize({ w: Math.max(600, r.width), h: 320 });
    });
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);

  const padL = 56, padR = 28, padT = 18, padB = 34;
  const W = size.w, H = size.h;
  // Fixed 0–100% axes on both sides so the diagonal is a true 45° reference
  // regardless of which prob range the window's legs fall in.
  const lo = 0, hi = 1;
  const sx = (v) => padL + ((v - lo) / (hi - lo)) * (W - padL - padR);
  const sy = (v) => padT + (1 - (v - lo) / (hi - lo)) * (H - padT - padB);

  if (!points || !points.length) {
    return (
      <div ref={ref} className="pnl-chart" style={{ display: "grid", placeItems: "center", color: "var(--text-3)", minHeight: 160 }}>
        No resolved legs in this window yet.
      </div>
    );
  }

  // Scale dot radius by sample count so thin bins read as less trustworthy.
  const maxN = Math.max(...points.map(p => p.n), 1);
  const rFor = (n) => 3 + 5 * Math.sqrt(n / maxN);

  const ticks = [0, 0.25, 0.5, 0.75, 1];

  return (
    <div ref={ref} className="pnl-chart">
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H}>
        {/* Grid + axis ticks */}
        {ticks.map((t, i) => (
          <g key={i}>
            <line x1={sx(t)} x2={sx(t)} y1={padT} y2={H - padB} stroke="rgba(255,255,255,.06)" strokeWidth="1" />
            <line x1={padL} x2={W - padR} y1={sy(t)} y2={sy(t)} stroke="rgba(255,255,255,.06)" strokeWidth="1" />
            <text x={sx(t)} y={H - 10} fill="#9ca3af" fontSize="11.5" textAnchor="middle" fontFamily="JetBrains Mono,ui-monospace,monospace">{(t * 100).toFixed(0)}%</text>
            <text x={padL - 10} y={sy(t) + 4} fill="#9ca3af" fontSize="11.5" textAnchor="end" fontFamily="JetBrains Mono,ui-monospace,monospace">{(t * 100).toFixed(0)}%</text>
          </g>
        ))}

        {/* Perfect-calibration diagonal (y = x) */}
        <line x1={sx(0)} y1={sy(0)} x2={sx(1)} y2={sy(1)} stroke="rgba(255,255,255,.35)" strokeWidth="1.5" strokeDasharray="5,4" />

        {/* Break-even reference (Power-6 leg BE ≈ 54.08%) — vertical guide */}
        <line x1={sx(0.5408)} x2={sx(0.5408)} y1={padT} y2={H - padB} stroke="rgba(96,165,250,.35)" strokeWidth="1" />

        {/* Observed reliability curve */}
        <path
          d={points.map((p, i) => `${i === 0 ? "M" : "L"}${sx(p.pred).toFixed(1)},${sy(p.actual).toFixed(1)}`).join(" ")}
          fill="none" stroke="#22C55E" strokeWidth="2" strokeOpacity="0.7"
        />
        {points.map((p, i) => {
          // Below the diagonal (actual < pred) = overconfident → red; on/above = green.
          const over = p.actual < p.pred - 0.01;
          return (
            <circle key={i} cx={sx(p.pred)} cy={sy(p.actual)} r={rFor(p.n)}
                    fill={over ? "#EF4444" : "#22C55E"} fillOpacity="0.55"
                    stroke="#0a0a0d" strokeWidth="1.25">
              <title>{`predicted ${(p.pred * 100).toFixed(1)}% → realized ${(p.actual * 100).toFixed(1)}%  (n=${p.n})`}</title>
            </circle>
          );
        })}

        {/* Axis labels */}
        <text x={(padL + W - padR) / 2} y={H - 26} fill="#6b7280" fontSize="11" textAnchor="middle" fontFamily="JetBrains Mono,ui-monospace,monospace">predicted probability</text>
        <text x={16} y={(padT + H - padB) / 2} fill="#6b7280" fontSize="11" textAnchor="middle" fontFamily="JetBrains Mono,ui-monospace,monospace" transform={`rotate(-90 16 ${(padT + H - padB) / 2})`}>realized hit rate</text>
      </svg>
    </div>
  );
}

function niceStep(raw) {
  const exp = Math.floor(Math.log10(raw));
  const frac = raw / Math.pow(10, exp);
  let nice;
  if (frac < 1.5) nice = 1;
  else if (frac < 3) nice = 2;
  else if (frac < 7) nice = 5;
  else nice = 10;
  return nice * Math.pow(10, exp);
}

Object.assign(window, { AnalyticsPage });
