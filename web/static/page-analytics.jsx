// Analytics — Cumulative P&L chart + Brier/CLV stats. Wired to /api/analytics.
const { useState: useStateA, useMemo: useMemoA, useRef: useRefA, useEffect: useEffectA } = React;

const RANGES = ["1D", "1W", "1M", "3M", "1Y", "MAX"];
const _RANGE_DAYS = { "1D": 1, "1W": 7, "1M": 30, "3M": 90, "1Y": 365 };

// Per-leg break-even for a 6-leg Power slip, used to tint Raw Hit Rate and to
// place the reliability chart's guide line. Sourced from page-backtest.jsx,
// which DERIVES it from its payout table (index.html + build.sh both load
// page-backtest before page-analytics, so the global exists). This was
// hardcoded as 54.08 — the pre-37.5x value — which tinted a 54.1%-54.6% hit
// rate green when it is EV-NEGATIVE. The fallback recomputes from 37.5x rather
// than restating a literal, so it can't silently go stale either.
// Chart gutters, shared by PnLChart and ReliabilityChart — they were declared
// identically in both and had to be edited in lockstep. Prefixed because these
// bundles are plain global scripts with no module scope, so a bare `padL` would
// be a collision risk across files.
//
// padL=56 is sized for the widest right-anchored Y-axis label, drawn at
// x={padL-10}; shrink it and the labels clip. The X tick text sits at y={H-10}
// and the axis caption at y={H-26}, so changing b without moving those detaches
// them from the axis.
const AN_CHART_PAD = { l: 56, r: 28, t: 18, b: 34 };

const AN_LEG_BE_PCT = typeof CP_LEG_BE_6_POWER_PCT === "number"
  ? CP_LEG_BE_6_POWER_PCT
  : Math.pow(1 / 37.5, 1 / 6) * 100;

function AnalyticsPage() {
  const [range, setRange] = useState("1W");
  // Custom range: two YYYY-MM-DD strings. Only consulted when range==="CUSTOM".
  // Empty until the user first opens Custom, at which point they default to the
  // full data span (first slip → today) so the chart isn't blank.
  const [custom, setCustom] = useState({ start: "", end: "" });
  const [data, setData] = useState(null);
  const [loadState, setLoadState] = useState("loading");
  const [errMsg, setErrMsg] = useState("");
  // Point under the finger/cursor while the P&L chart is being scrubbed, or null
  // when it isn't. It lives HERE rather than inside PnLChart because the
  // header's big number tracks the drag (see headerNumber below). PnLChart keeps
  // the smooth part of the gesture — the crosshair's x — local to itself and
  // only calls onScrub when the active POINT changes, so this state (and the
  // page re-render it causes) updates at most once per slip, not per frame.
  const [scrub, setScrub] = useState(null);
  // Stable identity so PnLChart's effect doesn't re-fire every render.
  const onScrub = React.useCallback((p) => setScrub(p), []);

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
  // so the chart can colour each dot green/red/yellow by win/loss/push.
  const fullSeries = useMemo(() => {
    if (!data?.pnl_timeline) return [];
    return data.pnl_timeline.map(p => ({
      date:  new Date(p.timestamp),
      pnl:   p.cum_pnl,                              // running total in units
      delta: typeof p.pnl === "number" ? p.pnl : 0, // this slip's contribution
      slipId: p.slip_id || null,
    })).filter(p => !isNaN(p.date.getTime()));
  }, [data]);

  // Window bounds shared by the chart and the stat cards. A fixed range
  // (1D…1Y) is anchored to NOW (real wall-clock), not to the last slip — so
  // "1Y" always means "the last 365 days ending today", and the axis spans the
  // whole window even when the newest bet is weeks old. MAX spans first slip →
  // now. Returns { startMs, endMs } (startMs === -Infinity for MAX's lower
  // bound so every slip is included).
  const nowMs = useMemo(() => Date.now(), [data]);
  const windowBounds = useMemo(() => {
    if (range === "CUSTOM") {
      // Parse the date inputs in LOCAL time. Start = midnight of the start day;
      // end = end-of-day of the end day so the whole end date is included.
      const s = custom.start ? new Date(custom.start + "T00:00:00").getTime() : -Infinity;
      const e = custom.end   ? new Date(custom.end   + "T23:59:59.999").getTime() : nowMs;
      // Tolerate an inverted range (end before start) by swapping.
      return e >= s ? { startMs: s, endMs: e } : { startMs: e, endMs: s };
    }
    const endMs = nowMs;
    if (range === "MAX") return { startMs: -Infinity, endMs };
    const days = _RANGE_DAYS[range] || 30;
    return { startMs: endMs - days * 86400000, endMs };
  }, [range, nowMs, custom]);

  const filtered = useMemo(() => {
    if (!fullSeries.length) return [];
    const { startMs, endMs } = windowBounds;

    // Slips that actually fall inside the window.
    const startIdx = range === "MAX"
      ? 0
      : fullSeries.findIndex(p => p.date.getTime() >= startMs);

    // Baseline = cumulative P&L right before the window opens, so the curve is
    // REBASED to start at 0u. For a fixed window that's the last slip before
    // the window (or ~0 if the window predates the first bet). For MAX it's the
    // pre-first-slip value (~0).
    let baseline, inWindow;
    if (startIdx < 0) {
      // No slip within the window (all bets are older than the window start).
      // Everything to date is the running baseline; the window shows a flat 0.
      baseline = fullSeries[fullSeries.length - 1].pnl;
      inWindow = [];
    } else {
      baseline = startIdx > 0
        ? fullSeries[startIdx - 1].pnl
        : (fullSeries[0].pnl - (fullSeries[0].delta || 0));
      // Cap the top of the window too. For the fixed presets endMs is "now" so
      // this is a no-op (no slip is in the future), but for a CUSTOM range whose
      // end is in the past it correctly drops slips that settle after the window.
      inWindow = fullSeries.slice(startIdx).filter(p => p.date.getTime() <= endMs);
    }

    const pts = inWindow.map(p => ({
      ...p,
      pnl: p.pnl - baseline,   // rebased: window opens at 0u
      cumAbs: p.pnl,           // keep absolute for reference
    }));

    // Endpoint points so the drawn axis spans the FULL window and there's a
    // visible marker at the first and last date shown. `endpoint: true` marks
    // these synthetic points — they render a neutral dot but carry no per-slip
    // delta since they aren't slips. For MAX the domain naturally starts at the
    // first slip (already a real dot), so only the trailing "now" endpoint is
    // added.
    const out = [];
    if (range !== "MAX" && isFinite(startMs)) {
      // Leading flat segment: 0u from the window start until the first real
      // bet in the window. If a bet lands exactly at/after the start this still
      // draws a flat run from window-open to that bet.
      const firstT = pts.length ? pts[0].date.getTime() : endMs;
      if (firstT > startMs) {
        out.push({ date: new Date(startMs), pnl: 0, delta: 0, slipId: null, endpoint: true, endpointKind: "start" });
      }
    }
    out.push(...pts);
    // Trailing flat segment: hold the last cumulative value out to "now" so the
    // curve doesn't stop at the most recent bet.
    const lastPnl = pts.length ? pts[pts.length - 1].pnl : 0;
    const lastT = pts.length ? pts[pts.length - 1].date.getTime() : -Infinity;
    if (endMs > lastT) {
      out.push({ date: new Date(endMs), pnl: lastPnl, delta: 0, slipId: null, endpoint: true, endpointKind: "end" });
    }
    return out;
  }, [fullSeries, range, windowBounds]);

  // Window stats: filter resolved_legs by the same window and recompute.
  const windowLegs = useMemo(() => {
    if (!data?.resolved_legs) return [];
    if (range === "MAX") return data.resolved_legs;
    const { startMs, endMs } = windowBounds;
    return data.resolved_legs.filter(l => {
      const t = l.timestamp ? new Date(l.timestamp).getTime() : NaN;
      return !isNaN(t) && t >= startMs && t <= endMs;
    });
  }, [data, range, windowBounds]);

  const windowClv = useMemo(() => {
    if (!data?.clv_legs) return [];
    if (range === "MAX") return data.clv_legs;
    const { startMs, endMs } = windowBounds;
    return data.clv_legs.filter(l => {
      const t = l.timestamp ? new Date(l.timestamp).getTime() : NaN;
      return !isNaN(t) && t >= startMs && t <= endMs;
    });
  }, [data, range, windowBounds]);

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

  // Switch to the custom range. On first open, seed the inputs with the full
  // data span (first slip → today) so the chart shows something immediately
  // rather than an empty window the user has to fill in by hand.
  const enterCustom = () => {
    setCustom(c => {
      if (c.start && c.end) return c;
      const first = fullSeries.length ? fullSeries[0].date : new Date(nowMs - 30 * 86400000);
      return { start: toISODate(first), end: toISODate(new Date(nowMs)) };
    });
    setRange("CUSTOM");
  };

  if (loadState === "loading") {
    return <main className="bd-page an-page"><div className="cp-state">Loading analytics…</div></main>;
  }
  if (loadState === "error") {
    return <main className="bd-page an-page"><div className="cp-state-err">Error: {errMsg}</div></main>;
  }
  if (!fullSeries.length) {
    return (
      <main className="bd-page an-page">
        <div className="an-panel cp-state">
          No resolved slips yet. Once your logged slips settle, your equity curve and calibration stats appear here.
        </div>
      </main>
    );
  }

  // The header's big number TRACKS the chart drag, Robinhood-style: while a
  // scrub is active it reads the cumulative P&L as of the scrubbed point (and
  // the date of the slip in effect there); on release it snaps back to the
  // window total and the window's date range. The label stays "Cumulative P&L"
  // in both states — the number's meaning never changes, only the instant it's
  // read at — and the +/- tone flips with the scrubbed sign.
  //
  // This REVERSES an earlier decision that the header must not react to
  // dragging. That decision was correct for what the chart was then: a static
  // display whose drag gesture belonged to the PAGE (touch-action:pan-y), so a
  // reacting header would have twitched at a reader who was only trying to
  // scroll. The chart now claims the gesture itself (.pnl-chart.is-scrub sets
  // touch-action:none), so a drag over it is always a deliberate read, and
  // following it is the entire point of the interaction. Note this is now the
  // one place the web chart intentionally does MORE than the iOS Performance
  // tab, whose Swift Charts view has no scrub — the old comment's "matching
  // iOS" claim no longer holds and shouldn't be restored.
  const headerNumber = scrub ? scrub.pnl : totalPnL;
  const headerLabel  = scrub
    ? scrub.date.toLocaleDateString(undefined,{month:"short",day:"numeric",year:"numeric"})
    : (filtered.length
        ? `${filtered[0].date.toLocaleDateString(undefined,{month:"short",day:"numeric"})} → ${filtered[filtered.length-1].date.toLocaleDateString(undefined,{month:"short",day:"numeric",year:"numeric"})}`
        : "");

  // By this point the loading/error/empty states have already returned early
  // above, so stat cards only render with real data — but keep an explicit
  // flag so the shimmer contract with StatCard stays consistent.
  const anLoading = false;

  return (
    <main className="bd-page an-page">
      <div className="an-panel">
        <div className="pnl-header">
          <div>
            <div className="pnl-title">Cumulative P&L</div>
            <div className={"pnl-total " + (headerNumber >= 0 ? "tone-good" : "tone-bad")}>
              {headerNumber >= 0 ? "+" : ""}{headerNumber.toFixed(2)}u
            </div>
          </div>
          <div className="pnl-hover">
            <div>{headerLabel}</div>
          </div>
        </div>

        <PnLChart series={filtered} onScrub={onScrub} />

        <div className="pnl-range">
          {RANGES.map(r => (
            <button key={r} className={"pnl-range-btn " + (range === r ? "is-on" : "")} onClick={() => setRange(r)}>{r}</button>
          ))}
          <button className={"pnl-range-btn is-custom " + (range === "CUSTOM" ? "is-on" : "")} onClick={enterCustom}>Custom</button>
        </div>

        {range === "CUSTOM" && (
          <div className="pnl-custom">
            <label className="pnl-custom-field">
              <span>From</span>
              <input type="date" className="pnl-custom-input"
                     value={custom.start} max={custom.end || undefined}
                     onChange={e => setCustom(c => ({ ...c, start: e.target.value }))} />
            </label>
            <span className="pnl-custom-arrow">→</span>
            <label className="pnl-custom-field">
              <span>To</span>
              <input type="date" className="pnl-custom-input"
                     value={custom.end} min={custom.start || undefined}
                     onChange={e => setCustom(c => ({ ...c, end: e.target.value }))} />
            </label>
          </div>
        )}

        <div className="an-section-h">Window stats <span className="an-section-sub">stats for: {range === "CUSTOM" ? `${custom.start} → ${custom.end}` : range}</span></div>
        <div className="bt-summary">
          <StatCard loading={anLoading} label="Brier Score" value={brier.toFixed(4)} tone={brier < 0.25 ? "good" : "neutral"} />
          <StatCard loading={anLoading} label="Log Loss" value={logLoss.toFixed(4)} />
          <StatCard loading={anLoading} label="Resolved Legs" value={String(allLegs.length)} />
          <StatCard loading={anLoading} label="Raw Hit Rate" value={rawHit.toFixed(1) + "%"} tone={rawHit >= AN_LEG_BE_PCT ? "good" : "bad"} />
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

// Draggable ("scrubbable") equity curve, Robinhood-style: drag on touch, hover on
// desktop, and the header's number follows (see AnalyticsPage's headerNumber).
//
// The three interaction properties that took care to get right:
//
//   * The drag never scrolls the page and never selects anything. Both are CSS,
//     not JS: `.pnl-chart.is-scrub { touch-action: none }` makes this element
//     claim the gesture outright, and the shared `.pnl-chart` already carries
//     user-select/-webkit-touch-callout:none. Doing it in CSS beats
//     preventDefault() on touchmove, which modern browsers treat as passive on
//     these listeners and would ignore.
//   * One code path for mouse, touch and pen: POINTER events, plus
//     setPointerCapture so a fast drag that leaves the chart box keeps tracking
//     instead of silently stopping. Mouse additionally scrubs on plain hover
//     (there is no "press" to wait for on a desktop chart); touch and pen only
//     while down, and lifting ends the read because a finger has no hover state.
//   * It stays smooth. The crosshair's x is LOCAL state so a frame-rate gesture
//     re-renders only this SVG, the geometry is memoized so pointermove doesn't
//     rebuild the path string, and the parent is notified only when the active
//     POINT changes (at most once per slip).
function PnLChart({ series, onScrub }) {
  const ref = useRef(null);
  const svgRef = useRef(null);
  const [size, setSize] = useState({ w: 800, h: 320 });
  // Active point index, and the crosshair x in SVG user units. Both null = idle.
  const [scrubIdx, setScrubIdx] = useState(null);
  const [cursorX, setCursorX] = useState(null);
  const draggingRef = useRef(false);
  const lastIdxRef = useRef(null);

  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver(es => {
      const r = es[0].contentRect;
      setSize({ w: Math.max(600, r.width), h: 320 });
    });
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);

  // Geometry. Memoized because every pointermove re-renders this component and
  // rebuilding the step path + tick arrays per frame is wasted work. Runs before
  // the empty-series early return below, so it must tolerate an empty series
  // (hooks cannot run conditionally).
  const geo = useMemo(() => {
    if (!series.length) return null;
    const { l: padL, r: padR, t: padT, b: padB } = AN_CHART_PAD;
    const W = size.w, H = size.h;
    // X is positioned by real timestamp, not by index, so gaps between slips
    // map to real time on the x-axis.
    const tMin = series[0].date.getTime();
    const tMax = series[series.length - 1].date.getTime();
    const tSpan = Math.max(1, tMax - tMin);
    const plotW = W - padL - padR;
    // Precomputed rather than a function of i: the hit-test walks this array on
    // every pointermove.
    const xsArr = series.map(p => padL + ((p.date.getTime() - tMin) / tSpan) * plotW);
    const min = Math.min(...series.map(p => p.pnl), 0);
    const max = Math.max(...series.map(p => p.pnl), 0);
    const yRange = max - min || 1;
    const ys = (v) => padT + (1 - (v - min) / yRange) * (H - padT - padB);
    const zeroY = ys(0);

    // STEP-AFTER path: bankroll stays constant between resolved slips, then
    // jumps when the next one settles. Each segment = one slip's contribution.
    //   start: (x0, ys(0))
    //   for each point: horizontal hold to that x, then vertical jump to ys(pnl)
    let pathD = `M${xsArr[0].toFixed(1)},${ys(0).toFixed(1)} `;
    let prevY = ys(0);
    for (let i = 0; i < series.length; i++) {
      const xi = xsArr[i];
      const yi = ys(series[i].pnl);
      pathD += `L${xi.toFixed(1)},${prevY.toFixed(1)} L${xi.toFixed(1)},${yi.toFixed(1)} `;
      prevY = yi;
    }
    const lastX = xsArr[series.length - 1];
    const fillD = `${pathD} L${lastX.toFixed(1)},${zeroY} L${xsArr[0].toFixed(1)},${zeroY} Z`;

    // Y-axis ticks
    const yTicks = [];
    const step = niceStep(yRange / 4);
    for (let v = Math.ceil(min / step) * step; v <= max; v += step) yTicks.push(v);

    // X-axis: 4-5 evenly-spaced date labels by time, not index. The count-1
    // divisor is guarded: a single-point series would otherwise emit NaN into
    // an x attribute and drop the label.
    const xTickCount = Math.min(5, series.length);
    const xTicks = [];
    for (let i = 0; i < xTickCount; i++) {
      const f = xTickCount === 1 ? 0 : i / (xTickCount - 1);
      xTicks.push({
        x: padL + f * plotW,
        label: new Date(tMin + f * tSpan).toLocaleDateString(undefined, { month: "short", day: "numeric" }),
      });
    }
    return { padL, padR, padT, padB, W, H, xsArr, ys, zeroY, pathD, fillD, yTicks, xTicks };
  }, [series, size.w, size.h]);

  // Drop any in-flight scrub when the window changes under us (range button,
  // custom dates, a reload): the old index pointed into a series that no longer
  // exists, and the crosshair would sit at a stale x.
  useEffect(() => {
    draggingRef.current = false;
    lastIdxRef.current = null;
    setScrubIdx(null);
    setCursorX(null);
    if (onScrub) onScrub(null);
  }, [series, onScrub]);

  // clientX -> SVG user units. The <svg> is width:100% over a fixed viewBox and
  // W has a 600 floor, so on any narrow viewport the on-screen box and the
  // user-space box differ by a scale factor — converting is not optional.
  const toUserX = (clientX) => {
    const el = svgRef.current;
    if (!el || !geo) return null;
    const r = el.getBoundingClientRect();
    if (!r.width) return null;
    return (clientX - r.left) * (geo.W / r.width);
  };

  // The value in effect at x on a step-after curve is the LAST point at or
  // before x — deliberately not the nearest point, because between two slips the
  // curve genuinely holds the earlier total, and "nearest" would report a total
  // that hadn't happened yet when the cursor is just left of a jump.
  const indexAtUserX = (ux) => {
    const xs = geo.xsArr;
    let found = 0;
    for (let i = 0; i < xs.length; i++) {
      if (xs[i] <= ux) found = i;
      else break;
    }
    return found;
  };

  const applyScrub = (clientX) => {
    if (!geo) return;
    const ux = toUserX(clientX);
    if (ux == null) return;
    // Clamp into the plot area so the crosshair can't wander over the axis
    // gutters while the pointer is captured outside the chart.
    const x = Math.max(geo.padL, Math.min(geo.W - geo.padR, ux));
    const i = indexAtUserX(x);
    setCursorX(x);
    setScrubIdx(i);
    if (i !== lastIdxRef.current) {
      lastIdxRef.current = i;
      if (onScrub) onScrub(series[i]);
    }
  };

  const endScrub = () => {
    draggingRef.current = false;
    lastIdxRef.current = null;
    setScrubIdx(null);
    setCursorX(null);
    if (onScrub) onScrub(null);
  };

  const onPointerDown = (e) => {
    if (e.pointerType === "mouse" && e.button !== 0) return;   // right/middle click
    draggingRef.current = true;
    // Keep receiving moves after the pointer leaves the chart box; without this a
    // fast drag off the top or side edge stops updating mid-gesture.
    try { e.currentTarget.setPointerCapture(e.pointerId); } catch (err) { /* not fatal */ }
    applyScrub(e.clientX);
  };
  const onPointerMove = (e) => {
    if (e.pointerType === "mouse" || draggingRef.current) applyScrub(e.clientX);
  };
  const onPointerUp = (e) => {
    draggingRef.current = false;
    try { e.currentTarget.releasePointerCapture(e.pointerId); } catch (err) { /* already released */ }
    // A finger has no hover state, so lifting ends the read. A mouse keeps
    // scrubbing — the cursor is still over the chart — and pointerleave ends it.
    if (e.pointerType !== "mouse") endScrub();
  };
  const onPointerLeave = () => { if (!draggingRef.current) endScrub(); };

  // Keyboard parity with the pointer read, so the values aren't hover-only.
  const onKeyDown = (e) => {
    if (!geo) return;
    const n = series.length;
    let i = scrubIdx == null ? n - 1 : scrubIdx;
    if (e.key === "ArrowLeft") i = Math.max(0, i - 1);
    else if (e.key === "ArrowRight") i = Math.min(n - 1, i + 1);
    else if (e.key === "Home") i = 0;
    else if (e.key === "End") i = n - 1;
    else if (e.key === "Escape") { endScrub(); return; }
    else return;
    e.preventDefault();   // arrows would otherwise scroll the page
    setScrubIdx(i);
    setCursorX(geo.xsArr[i]);
    if (i !== lastIdxRef.current) {
      lastIdxRef.current = i;
      if (onScrub) onScrub(series[i]);
    }
  };

  if (!series.length || !geo) return <div ref={ref} className="pnl-chart" />;

  const { padL, padR, padT, padB, W, H, ys, zeroY, pathD, fillD, yTicks, xTicks } = geo;
  // Keeps the JSX below reading as it did when x was computed from the index.
  const xs = (i) => geo.xsArr[i];

  const isUp = series[series.length - 1].pnl >= 0;
  const lineColor = isUp ? "#22C55E" : "#EF4444";

  // Per-slip dot color: green if this slip won, red if it lost, yellow if push.
  // Endpoint markers (window start / "now") are neutral so they read as axis
  // bookends, not as a slip outcome.
  const dotColor = (p) => {
    if (p && p.endpoint) return "#9CA3AF";
    const delta = typeof p === "number" ? p : (p ? p.delta : 0);
    return delta > 0 ? "#22C55E" : delta < 0 ? "#EF4444" : "#FBBF24";
  };

  return (
    <div
      ref={ref}
      className="pnl-chart is-scrub"
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={onPointerUp}
      onPointerCancel={endScrub}
      onPointerLeave={onPointerLeave}
      onKeyDown={onKeyDown}
      onBlur={endScrub}
      tabIndex={0}
      role="img"
      aria-label={`Cumulative profit and loss, ${series.length} points. Drag across the chart, or use the left and right arrow keys, to read the total at any date.`}
    >
      <svg ref={svgRef} viewBox={`0 0 ${W} ${H}`} width="100%" height={H}>
        <defs>
          <linearGradient id="pnl-fill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor={lineColor} stopOpacity="0.24" />
            <stop offset="100%" stopColor={lineColor} stopOpacity="0" />
          </linearGradient>
        </defs>
        {/* Y grid */}
        {yTicks.map((v, i) => (
          <g key={i}>
            <line x1={padL} x2={W - padR} y1={ys(v)} y2={ys(v)} stroke="var(--hair)" strokeWidth="1" />
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

        {/* Per-slip dots colored by individual outcome, plus neutral endpoint
            markers at the first date shown (window start) and the last ("now").
            Legacy hidden anchors, if any, still get no dot. */}
        {series.map((p, i) => (
          p.anchor ? null : (
            <circle
              key={i}
              cx={xs(i)}
              cy={ys(p.pnl)}
              r={p.endpoint ? 3.5 : 3}
              fill={dotColor(p)}
              stroke="#0a0a0d"
              strokeWidth="1.25"
            />
          )
        ))}

        {/* Scrub layer: crosshair + the marker riding the curve. Drawn last of
            the data marks so the marker is never hidden behind a per-slip dot.
            pointerEvents="none" keeps it out of hit-testing — it must never
            become the event target mid-drag. (Scoped to this <g>: the same
            property on .pnl-chart would be inherited by ReliabilityChart's
            <circle> marks and kill their native <title> tooltips.)
            cy is ys(active pnl), which for a step-after curve IS the curve's y at
            the cursor, so the marker sits exactly on the line at every x. */}
        {cursorX != null && scrubIdx != null && (
          <g pointerEvents="none">
            <line x1={cursorX} x2={cursorX} y1={padT} y2={H - padB}
                  stroke="rgba(255,255,255,.45)" strokeWidth="1" />
            <circle cx={cursorX} cy={ys(series[scrubIdx].pnl)} r="5.5"
                    fill={lineColor} stroke="#0a0a0d" strokeWidth="2" />
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

  const { l: padL, r: padR, t: padT, b: padB } = AN_CHART_PAD;
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
            <line x1={sx(t)} x2={sx(t)} y1={padT} y2={H - padB} stroke="var(--hair)" strokeWidth="1" />
            <line x1={padL} x2={W - padR} y1={sy(t)} y2={sy(t)} stroke="var(--hair)" strokeWidth="1" />
            <text x={sx(t)} y={H - 10} fill="#9ca3af" fontSize="11.5" textAnchor="middle" fontFamily="JetBrains Mono,ui-monospace,monospace">{(t * 100).toFixed(0)}%</text>
            <text x={padL - 10} y={sy(t) + 4} fill="#9ca3af" fontSize="11.5" textAnchor="end" fontFamily="JetBrains Mono,ui-monospace,monospace">{(t * 100).toFixed(0)}%</text>
          </g>
        ))}

        {/* Perfect-calibration diagonal (y = x) */}
        <line x1={sx(0)} y1={sy(0)} x2={sx(1)} y2={sy(1)} stroke="rgba(255,255,255,.35)" strokeWidth="1.5" strokeDasharray="5,4" />

        {/* Break-even reference (Power-6 leg BE) — vertical guide */}
        <line x1={sx(AN_LEG_BE_PCT / 100)} x2={sx(AN_LEG_BE_PCT / 100)} y1={padT} y2={H - padB} stroke="rgba(96,165,250,.35)" strokeWidth="1" />

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

// Local-time YYYY-MM-DD for <input type="date"> values (avoids the UTC shift
// that toISOString() would introduce near midnight).
function toISODate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
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
