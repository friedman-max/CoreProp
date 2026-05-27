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
  const fullSeries = useMemo(() => {
    if (!data?.pnl_timeline) return [];
    return data.pnl_timeline.map(p => ({
      date: new Date(p.timestamp),
      pnl: p.cum_pnl,
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

  return (
    <main className="bd-page an-page">
      <div className="an-panel">
        <div className="pnl-header">
          <div>
            <div className="pnl-title">Cumulative P&amp;L <span className="pnl-title-sub">Unit Stake / Slip</span></div>
            <div className={"pnl-total " + (totalPnL >= 0 ? "tone-good" : "tone-bad")}>
              {totalPnL >= 0 ? "+" : ""}{totalPnL.toFixed(2)}u
            </div>
          </div>
          <div className="pnl-hover">{hovered ? hovered.date.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" }) : ""}</div>
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
  const [size, setSize] = useState({ w: 800, h: 280 });
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver(es => {
      const r = es[0].contentRect;
      setSize({ w: Math.max(600, r.width), h: 280 });
    });
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);

  if (!series.length) return <div ref={ref} className="pnl-chart" />;

  const padL = 50, padR = 24, padT = 16, padB = 30;
  const W = size.w, H = size.h;
  const xs = (i) => padL + (i / (series.length - 1)) * (W - padL - padR);
  const min = Math.min(...series.map(p => p.pnl), 0);
  const max = Math.max(...series.map(p => p.pnl), 0);
  const yRange = max - min || 1;
  const ys = (v) => padT + (1 - (v - min) / yRange) * (H - padT - padB);
  const zeroY = ys(0);

  const path = series.map((p, i) => `${i === 0 ? "M" : "L"}${xs(i).toFixed(1)},${ys(p.pnl).toFixed(1)}`).join(" ");
  const fillPath = `${path} L${xs(series.length - 1)},${zeroY} L${xs(0)},${zeroY} Z`;
  const isUp = series[series.length - 1].pnl >= 0;
  const color = isUp ? "#22C55E" : "#EF4444";

  // Y-axis ticks
  const yTicks = [];
  const step = niceStep(yRange / 4);
  for (let v = Math.ceil(min / step) * step; v <= max; v += step) yTicks.push(v);

  // X-axis: 4-5 evenly-spaced date labels
  const xTickCount = 5;
  const xTicks = [];
  for (let i = 0; i < xTickCount; i++) {
    const idx = Math.round((i / (xTickCount - 1)) * (series.length - 1));
    xTicks.push({ x: xs(idx), label: series[idx].date.toLocaleDateString(undefined, { month: "short", day: "numeric" }) });
  }

  const onMove = (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const ratio = (x - padL) / (W - padL - padR);
    const i = Math.round(Math.min(1, Math.max(0, ratio)) * (series.length - 1));
    onHover(series[i]);
  };

  const hoverIdx = hover ? series.findIndex(p => p.date.getTime() === hover.date.getTime()) : -1;

  return (
    <div ref={ref} className="pnl-chart">
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H} onMouseMove={onMove} onMouseLeave={() => onHover(null)}>
        <defs>
          <linearGradient id="pnl-fill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity="0.28" />
            <stop offset="100%" stopColor={color} stopOpacity="0" />
          </linearGradient>
        </defs>
        {yTicks.map((v, i) => (
          <g key={i}>
            <line x1={padL} x2={W - padR} y1={ys(v)} y2={ys(v)} stroke="rgba(255,255,255,.06)" strokeWidth="1" />
            <text x={padL - 8} y={ys(v) + 4} fill="#7a7a8b" fontSize="11" textAnchor="end" fontFamily="JetBrains Mono">
              {v > 0 ? "+" : ""}{v.toFixed(1)}u
            </text>
          </g>
        ))}
        <line x1={padL} x2={W - padR} y1={zeroY} y2={zeroY} stroke="rgba(255,255,255,.15)" strokeDasharray="3,3" />
        <path d={fillPath} fill="url(#pnl-fill)" />
        <path d={path} fill="none" stroke={color} strokeWidth="2" />
        {hoverIdx >= 0 && (
          <g>
            <line x1={xs(hoverIdx)} x2={xs(hoverIdx)} y1={padT} y2={H - padB} stroke="rgba(255,255,255,.25)" strokeDasharray="3,3" />
            <circle cx={xs(hoverIdx)} cy={ys(series[hoverIdx].pnl)} r="4" fill={color} stroke="white" strokeWidth="1.5" />
          </g>
        )}
        {xTicks.map((t, i) => (
          <text key={i} x={t.x} y={H - 8} fill="#7a7a8b" fontSize="11" textAnchor="middle" fontFamily="JetBrains Mono">{t.label}</text>
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
