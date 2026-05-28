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
    if (range === "MAX") return fullSeries;
    const latest = fullSeries[fullSeries.length - 1].date.getTime();
    const cutoff = _windowStartMs(range, latest);
    return fullSeries.filter(p => p.date.getTime() >= cutoff);
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

  const totalPnL = filtered.length ? filtered[filtered.length - 1].pnl - (filtered[0]?.pnl || 0) : 0;

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

  const clvCount = windowClv.length;
  const clvPos = clvCount ? (windowClv.filter(l => l.clv_pct > 0).length / clvCount) * 100 : 0;
  const clvAvg = clvCount ? windowClv.reduce((a, l) => a + l.clv_pct, 0) / clvCount : 0;

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
          <StatCard label="Brier Score" value={brier.toFixed(4)} tone={brier < 0.25 ? "good" : "neutral"} />
          <StatCard label="Log Loss" value={logLoss.toFixed(4)} />
          <StatCard label="Resolved Legs" value={String(allLegs.length)} />
          <StatCard label="Raw Hit Rate" value={rawHit.toFixed(1) + "%"} tone={rawHit >= 54.08 ? "good" : "bad"} />
          <StatCard label="Avg Predicted Prob" value={avgPred.toFixed(1) + "%"} />
          <StatCard label="Hit Rate Delta" value={(delta >= 0 ? "+" : "") + delta.toFixed(1) + "%"} tone={delta >= 0 ? "good" : "bad"} />
        </div>

        <div className="an-section-h">Closing Line Value <span className="an-section-sub">if CLV+ {`>`} 50% &amp; Avg CLV% {`>`} 0, you're beating the market</span></div>
        <div className="bt-summary">
          <StatCard label="Tracked" value={String(clvCount)} />
          <StatCard label="CLV+ Rate" value={clvCount ? clvPos.toFixed(1) + "%" : "—"} tone={clvPos >= 50 ? "good" : "bad"} />
          <StatCard label="Avg CLV%" value={clvCount ? (clvAvg >= 0 ? "+" : "") + clvAvg.toFixed(1) + "%" : "—"} tone={clvAvg >= 0 ? "good" : "bad"} />
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
