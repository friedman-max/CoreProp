// Backtest — slip-level history matching web/static/index.html `#backtest-view`.
const { useState: useStateBT, useMemo: useMemoBT } = React;

// Sample slip history — mirrors backtest_slips shape
const BT_SLIPS = [
  { id: "s001", ts: "5/26 9:42 PM", type: "Power", legs: 6, league: "MIXED", stake: 5, payout: 200,  result: "hit",     hits: 6, pushes: 0, dnps: 0,
    bets: [
      { player: "Cale Makar",   prop: "Assists O0.5",   pct: 72.4, result: "hit", actual: "2" },
      { player: "Wembanyama",   prop: "Assists U3.5",   pct: 59.9, result: "hit", actual: "2" },
      { player: "Bueckers",     prop: "Rebs+Asts U8.5", pct: 60.2, result: "hit", actual: "7" },
      { player: "Holmgren",     prop: "Rebounds U7.5",  pct: 57.6, result: "hit", actual: "6" },
      { player: "Wicks",        prop: "Hits Allow U4.5",pct: 62.0, result: "hit", actual: "3" },
      { player: "Vassell",      prop: "Rebounds U4.5",  pct: 57.0, result: "hit", actual: "3" },
    ] },
  { id: "s002", ts: "5/26 8:18 PM", type: "Power", legs: 5, league: "NBA", stake: 5, payout: 0, result: "miss", hits: 4, pushes: 0, dnps: 0,
    bets: [
      { player: "Stephon Castle", prop: "Points U17.5", pct: 57.1, result: "hit",  actual: "13" },
      { player: "J. Williams",    prop: "Points U14.5", pct: 56.7, result: "hit",  actual: "11" },
      { player: "Cason Wallace",  prop: "Rebs U3.5",    pct: 62.0, result: "hit",  actual: "2" },
      { player: "Dort",           prop: "Rebs U2.5",    pct: 59.2, result: "hit",  actual: "2" },
      { player: "McCain",         prop: "Rebs U2.5",    pct: 57.6, result: "miss", actual: "5" },
    ] },
  { id: "s003", ts: "5/26 7:55 PM", type: "Flex", legs: 4, league: "WNBA", stake: 5, payout: 25, result: "hit", hits: 4, pushes: 0, dnps: 0,
    bets: [
      { player: "C. Williams",  prop: "Rebs+Asts U9.5", pct: 66.4, result: "hit", actual: "7" },
      { player: "Olivia Miles", prop: "Rebs+Asts U10.5",pct: 63.6, result: "hit", actual: "8" },
      { player: "Mabrey",       prop: "Rebs+Asts U7.5", pct: 60.2, result: "hit", actual: "6" },
      { player: "N. Cloud",     prop: "Rebs U3.5",      pct: 56.9, result: "hit", actual: "2" },
    ] },
  { id: "s004", ts: "5/26 6:40 PM", type: "Power", legs: 3, league: "MLB", stake: 5, payout: 30, result: "hit", hits: 3, pushes: 0, dnps: 0,
    bets: [
      { player: "Jordan Wicks",  prop: "Hits Allow U4.5",  pct: 62.0, result: "hit", actual: "3" },
      { player: "Sean Burke",    prop: "Walks Allow U1.5", pct: 58.3, result: "hit", actual: "1" },
      { player: "Jordan Wicks",  prop: "Walks Allow U1.5", pct: 57.8, result: "hit", actual: "1" },
    ] },
  { id: "s005", ts: "5/26 12:11 AM", type: "Power", legs: 6, league: "MIXED", stake: 5, payout: 0, result: "miss", hits: 5, pushes: 0, dnps: 0,
    bets: [
      { player: "Slavin",         prop: "Blk Shots O1.5", pct: 58.0, result: "hit",  actual: "4" },
      { player: "Champagnie",     prop: "P+R U14.5",      pct: 57.0, result: "hit",  actual: "9" },
      { player: "G. Jaquez",      prop: "Points U11.5",   pct: 57.7, result: "hit",  actual: "8" },
      { player: "N. Coffey",      prop: "Rebs U5.5",      pct: 57.8, result: "hit",  actual: "4" },
      { player: "Bedard",         prop: "SOG O3.5",       pct: 56.1, result: "hit",  actual: "5" },
      { player: "Pete Alonso",    prop: "TB O1.5",        pct: 55.9, result: "miss", actual: "1" },
    ] },
  { id: "s006", ts: "5/25 10:30 PM", type: "Power", legs: 4, league: "NBA", stake: 5, payout: 50, result: "hit", hits: 4, pushes: 0, dnps: 0,
    bets: [
      { player: "Wemby",     prop: "Pts U22.5",  pct: 58.1, result: "hit", actual: "18" },
      { player: "Holmgren",  prop: "Rebs U7.5",  pct: 59.0, result: "hit", actual: "5" },
      { player: "Vassell",   prop: "Pts U16.5",  pct: 56.2, result: "hit", actual: "12" },
      { player: "Wallace",   prop: "Asts O3.5",  pct: 55.5, result: "hit", actual: "5" },
    ] },
  { id: "s007", ts: "5/25 9:00 PM", type: "Flex", legs: 5, league: "WNBA", stake: 5, payout: 10, result: "hit", hits: 4, pushes: 0, dnps: 1,
    bets: [
      { player: "Bueckers",   prop: "R+A U8.5",   pct: 61.2, result: "hit", actual: "6" },
      { player: "Mabrey",     prop: "R+A U7.5",   pct: 59.0, result: "hit", actual: "4" },
      { player: "Coffey",     prop: "Rebs U5.5",  pct: 58.0, result: "hit", actual: "3" },
      { player: "Cloud",      prop: "Rebs U3.5",  pct: 56.9, result: "hit", actual: "1" },
      { player: "Diggins",    prop: "Pts U18.5",  pct: 57.0, result: "dnp", actual: "—" },
    ] },
  { id: "s008", ts: "5/25 7:15 PM", type: "Power", legs: 3, league: "MLB", stake: 5, payout: 0, result: "miss", hits: 2, pushes: 0, dnps: 0,
    bets: [
      { player: "Riley Greene", prop: "Hits O0.5",  pct: 55.5, result: "miss", actual: "0" },
      { player: "Pete Alonso",  prop: "TB O1.5",    pct: 56.0, result: "hit",  actual: "2" },
      { player: "S. Burke",     prop: "K's O3.5",   pct: 57.0, result: "hit",  actual: "5" },
    ] },
  { id: "s009", ts: "5/25 5:00 PM", type: "Power", legs: 6, league: "MIXED", stake: 5, payout: 200, result: "hit", hits: 6, pushes: 0, dnps: 0,
    bets: [
      { player: "Bedard",   prop: "SOG O3.5",  pct: 56.1, result: "hit", actual: "4" },
      { player: "Makar",    prop: "Asts O0.5", pct: 71.0, result: "hit", actual: "1" },
      { player: "Wemby",    prop: "Pts U22.5", pct: 57.8, result: "hit", actual: "20" },
      { player: "Holmgren", prop: "Rebs U7.5", pct: 58.4, result: "hit", actual: "6" },
      { player: "Mabrey",   prop: "R+A U7.5",  pct: 59.0, result: "hit", actual: "5" },
      { player: "Wicks",    prop: "Hits A U4.5",pct: 60.0, result: "hit", actual: "3" },
    ] },
  { id: "s010", ts: "5/27 7:00 PM", type: "Power", legs: 6, league: "MIXED", stake: 5, payout: null, result: "pending", hits: 0, pushes: 0, dnps: 0,
    bets: [
      { player: "Bueckers",  prop: "R+A U8.5", pct: 60.2, result: "pending", actual: "—" },
      { player: "Miles",     prop: "R+A U10.5",pct: 63.6, result: "pending", actual: "—" },
      { player: "Mabrey",    prop: "R+A U7.5", pct: 60.2, result: "pending", actual: "—" },
      { player: "Bedard",    prop: "SOG O3.5", pct: 56.1, result: "pending", actual: "—" },
      { player: "Alonso",    prop: "TB O1.5",  pct: 55.9, result: "pending", actual: "—" },
      { player: "Greene",    prop: "Hits O0.5",pct: 55.5, result: "pending", actual: "—" },
    ] },
];

// Mirror engine/constants.py exactly. Used to recompute slip payout from
// leg outcomes when the API's `payout` or `completed` flags are missing
// (e.g. older slips whose legs use "won"/"lost" instead of "hit"/"miss").
const BT_POWER_PAYOUTS = { 2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 40.0 };
const BT_FLEX_PAYOUTS = {
  3: { 2: 1.0, 3: 3.0 },
  4: { 3: 1.5, 4: 6.0 },
  5: { 3: 0.4, 4: 2.0, 5: 10.0 },
  6: { 4: 0.4, 5: 2.0, 6: 25.0 },
};

// Slip-level expected value % using the per-leg true probabilities.
// EV per unit stake = sum_outcomes( P(outcome) * payout(outcome) ) − 1.
// For Power: P(all hit) × table_payout. For Flex: sum over k hits using
// the Poisson-binomial expansion of the leg-prob vector.
function btSlipEvPct(slipTypeRaw, legs) {
  const probs = (legs || []).map(l => Math.max(0, Math.min(1, (l.pct || 0) / 100)));
  const n = probs.length;
  if (n < 2) return null;
  const slipType = String(slipTypeRaw || "power").toLowerCase();

  if (slipType === "power") {
    const pAll = probs.reduce((a, p) => a * p, 1);
    const pay = BT_POWER_PAYOUTS[n];
    if (!pay) return null;
    return (pAll * pay - 1) * 100;
  }

  // Flex: distribution over exact hit counts via Poisson-binomial.
  // dist[k] = P(exactly k hits out of n).
  let dist = [1];
  for (const p of probs) {
    const next = new Array(dist.length + 1).fill(0);
    for (let k = 0; k < dist.length; k++) {
      next[k]     += dist[k] * (1 - p);
      next[k + 1] += dist[k] * p;
    }
    dist = next;
  }
  if (n === 2) {
    // Flex 2-leg = Power 2-leg.
    return (dist[2] * BT_POWER_PAYOUTS[2] - 1) * 100;
  }
  const table = BT_FLEX_PAYOUTS[n];
  if (!table) return null;
  let expected = 0;
  for (let k = 0; k < dist.length; k++) {
    const pay = table[k] || 0;
    expected += dist[k] * pay;
  }
  return (expected - 1) * 100;
}

// Normalise the various result wordings the legs table may carry.
function btNormLegResult(r) {
  const s = String(r || "").toLowerCase();
  if (s === "hit" || s === "won" || s === "win" || s === "1") return "hit";
  if (s === "miss" || s === "lost" || s === "loss" || s === "0") return "miss";
  if (s === "push") return "push";
  if (s === "dnp") return "dnp";
  return "pending";
}

// Compute (resolved?, payout, result, hits_eff, n_eff) for a slip from its
// legs, independent of whatever the backend filled in. Returns:
//   resolved: every leg has a non-"pending" status
//   payout:   units returned for a 1-unit stake (0 = total loss)
//   result:   "pending" | "hit" | "miss" | "push"
function btComputeSlipOutcome(s) {
  const legs = (s.legs || []).map(l => btNormLegResult(l.result));
  if (!legs.length) return { resolved: false, payout: null, result: "pending", hitsEff: 0, nEff: 0 };
  if (legs.some(r => r === "pending")) return { resolved: false, payout: null, result: "pending", hitsEff: 0, nEff: 0 };

  // Pushes and DNPs are excluded from the n_eff denominator — exactly as
  // engine/web/app.py does. n_eff = legs that actually resolved hit/miss.
  const effective = legs.filter(r => r !== "push" && r !== "dnp");
  const nEff = effective.length;
  const hitsEff = effective.filter(r => r === "hit").length;
  const slipType = String(s.slip_type || s.type || "power").toLowerCase();

  let payout;
  if (nEff < 2) {
    payout = (nEff === 0 || (nEff === 1 && hitsEff === 1)) ? 1.0 : 0.0;
  } else if (slipType === "power") {
    payout = (hitsEff === nEff) ? (BT_POWER_PAYOUTS[nEff] || 0) : 0;
  } else if (nEff === 2) {
    // Flex 2-leg = Power 2-leg.
    payout = (hitsEff === 2) ? BT_POWER_PAYOUTS[2] : 0;
  } else {
    payout = ((BT_FLEX_PAYOUTS[nEff] || {})[hitsEff]) || 0;
  }

  let result;
  if (nEff === 0) result = "push";
  else if (payout > 1) result = "hit";
  else if (payout === 1) result = "push";
  else result = "miss";

  return { resolved: true, payout, result, hitsEff, nEff };
}

function btResultFromSlip(s) {
  return btComputeSlipOutcome(s).result;
}

function btMapSlip(s) {
  const legs = (s.legs || []).map(l => {
    const sideShort = (l.side || "").toUpperCase().startsWith("O") ? "O" : "U";
    const line = l.line ?? l.pp_line ?? "";
    const propName = l.prop_type || l.prop || l.stat_type || "";
    const actual = (l.stat_actual !== null && l.stat_actual !== undefined && l.stat_actual !== "")
      ? String(l.stat_actual)
      : (l.actual_value !== null && l.actual_value !== undefined && l.actual_value !== "" ? String(l.actual_value) : null);
    return {
      player: l.player || l.player_name || "—",
      league: l.league || "",
      propName,
      side: sideShort,
      line,
      prop: [propName, sideShort, line].filter(Boolean).join(" ").trim(),
      pct: l.true_prob != null ? l.true_prob * 100 : 0,
      // Normalise to hit/miss/push/dnp/pending so summary stats and the
      // leg-actual pill render consistently regardless of how the row was
      // written ("won"/"lost"/"1"/"0" all map to hit/miss).
      result: btNormLegResult(l.result),
      actual,
    };
  });
  const computed = btComputeSlipOutcome(s);
  const result = computed.result;
  // Trust our own computation: it works even when the backend left
  // `payout` null or `completed` false because the legs use "won"/"lost"
  // wording.
  const payoutUnits = computed.resolved ? computed.payout : null;
  const ts = s.timestamp ? new Date(s.timestamp).toLocaleString([], { month: "numeric", day: "numeric", hour: "numeric", minute: "2-digit" }) : "—";
  const tsRaw = s.timestamp ? new Date(s.timestamp).getTime() : 0;
  // Pick a representative league (most common across legs) — or MIXED.
  const leagueCounts = {};
  for (const l of (s.legs || [])) { const k = l.league || "MIXED"; leagueCounts[k] = (leagueCounts[k] || 0) + 1; }
  const leagueKeys = Object.keys(leagueCounts);
  const league = leagueKeys.length === 1 ? leagueKeys[0] : "MIXED";
  return {
    id: s.id || s.slip_id,
    ts,
    tsRaw,
    type: ((s.slip_type || "power").charAt(0).toUpperCase() + (s.slip_type || "power").slice(1)),
    legs: s.n_legs || legs.length,
    league,
    stake: 1,                   // 1-unit canonical stake everywhere
    payout: payoutUnits,        // null until resolved; in units (incl. stake)
    result,
    hits: s.hits || 0,
    bets: legs,
  };
}

function BacktestPage() {
  const [resultFilter, setResultFilter] = useState("");
  const [leagueFilter, setLeagueFilter] = useState("");
  const [page, setPage] = useState(1);
  const [slips, setSlips] = useState([]);
  const [loadState, setLoadState] = useState("loading");
  const [errMsg, setErrMsg] = useState("");
  const PER = 50;

  React.useEffect(() => {
    let cancelled = false;
    const URL = "/api/backtest/slips";
    const apply = (data) => {
      if (cancelled || !data) return;
      setSlips((data.slips || []).map(btMapSlip));
      setLoadState("ok");
    };
    // Seed instantly from the SWR cache if available, then subscribe so a
    // slip logged from the +EV tab (which revalidates this URL) shows up
    // here without a manual refresh.
    const cached = window.cpApi.getCached && window.cpApi.getCached(URL);
    if (cached) apply(cached);
    const unsub = window.cpApi.subscribeCache
      ? window.cpApi.subscribeCache(URL, apply)
      : () => {};
    (async () => {
      try {
        const data = window.cpApi.cachedFetch
          ? await window.cpApi.cachedFetch(URL)
          : await window.cpApi.apiFetch(URL);
        apply(data);
      } catch (ex) {
        if (cancelled) return;
        setErrMsg(ex.message || "Failed to load slips.");
        if (!cached) setLoadState("error");
      }
    })();
    // Light poll so resolved results / newly-logged slips refresh over time.
    const id = setInterval(() => {
      if (window.cpApi.cachedFetch) window.cpApi.cachedFetch(URL).then(apply).catch(() => {});
    }, 60000);
    return () => { cancelled = true; clearInterval(id); unsub(); };
  }, []);

  const filtered = useMemo(() =>
    slips.filter(s =>
      (!resultFilter || s.result === resultFilter) &&
      (!leagueFilter || s.league === leagueFilter || (s.league === "MIXED" && leagueFilter === ""))
    ), [slips, resultFilter, leagueFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PER));
  const slipsView = filtered.slice((page - 1) * PER, page * PER);

  // Summary stats — derived from the slips returned by /api/backtest/slips.
  const done = slips.filter(s => s.result !== "pending");
  const allLegs = slips.flatMap(s => s.bets || []);
  const doneLegs = allLegs.filter(l => l.result !== "pending");
  const legHits = doneLegs.filter(l => l.result === "hit").length;
  const slipHits = done.filter(s => s.result === "hit").length;

  // PrizePicks payout tables (must mirror engine/constants.py).
  // For Power, payout when all hit. For Flex, payout when all hit.
  const POWER_PAY = { 2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 40.0 };
  const FLEX_MAX_PAY = { 3: 3.0, 4: 6.0, 5: 10.0, 6: 25.0 };

  // Per-slip break-even hit rate. For each slip a "win" means payout > stake,
  // and the slip needs the corresponding payout multiple to break even on an
  // expected-value basis. We use the max payout per (type, legs) as the
  // reference, which is conservative for Flex (treating it as Power-equivalent)
  // and exact for Power.
  const _slipBE = (s) => {
    const t = (s.type || "").toLowerCase();
    const n = s.legs || (s.bets || []).length;
    const tbl = t === "flex" ? FLEX_MAX_PAY : POWER_PAY;
    const pay = tbl[n];
    return pay && pay > 1 ? 1 / pay : null;
  };

  // Weighted slip-mix BE: average BE across the actual resolved slips. The
  // user wanted "16.67% for 3 legs"-style thresholds — this gives that exact
  // number for an all-Power-3 dataset and the right blended threshold for
  // mixed leg counts.
  let _beSum = 0, _beCount = 0;
  for (const s of done) {
    const be = _slipBE(s);
    if (be != null) { _beSum += be; _beCount += 1; }
  }
  const slipBeWeighted = _beCount > 0 ? (_beSum / _beCount) * 100 : null;

  // Real ROI: 1-unit stake per resolved slip (matches the backend's
  // pnl_timeline math in engine/calibration.evaluate_analytics). Profit per
  // slip = payout - 1 (stake), ROI = total profit / total stake.
  const totalStakeUnits = done.length;
  const totalPayoutUnits = done.reduce((a, s) => a + (typeof s.payout === "number" ? s.payout : 0), 0);
  const totalProfitUnits = totalPayoutUnits - totalStakeUnits;

  const slipsDone = done.length;
  const slipsTotal = slips.length;
  const legsDone = doneLegs.length;
  const legsTotal = allLegs.length;
  const slipHitRate = slipsDone ? (slipHits / slipsDone) * 100 : 0;
  const legHitRate = legsDone ? (legHits / legsDone) * 100 : 0;
  const expLegHitRate = doneLegs.length ? doneLegs.reduce((a, l) => a + (l.pct || 0), 0) / doneLegs.length : 0;
  const roi = totalStakeUnits > 0 ? (totalProfitUnits / totalStakeUnits) * 100 : 0;

  const fmt = (v, d = 1, suf = "%") => v.toFixed(d) + suf;

  return (
    <main className="bd-page bt-page">
      <FiltersBar count={filtered.length} label="slips" page={page} totalPages={totalPages} onPage={setPage}>
        <label className="bd-f"><span>Result</span>
          <select value={resultFilter} onChange={e => { setResultFilter(e.target.value); setPage(1); }}>
            <option value="">All</option><option value="pending">Pending</option>
            <option value="hit">Hit</option><option value="miss">Miss</option>
            <option value="push">Push</option><option value="dnp">DNP</option>
          </select>
        </label>
        <label className="bd-f"><span>League</span>
          <select value={leagueFilter} onChange={e => { setLeagueFilter(e.target.value); setPage(1); }}>
            <option value="">All</option>{LEAGUE_ORDER.map(l => <option key={l}>{l}</option>)}
          </select>
        </label>
      </FiltersBar>

      {/* Summary cards.
       * Tone rule (per user): a hit-rate card is green only when it is at or
       * above the break-even threshold for the actual slip mix; red below.
       *
       *   - Slip Hit Rate BE: weighted average of 1/payout across resolved
       *     slips. For a pure Power-3 dataset this collapses to 16.67%; for
       *     Power 4 it's 10%, Power 6 it's 2.5%, and so on. Mixed slip logs
       *     get the correct blended threshold.
       *   - Leg Hit Rate BE = 54.08% (geometric BE for Power 6 — the
       *     standard reference point for individual-leg quality).
       *   - ROI: positive ⇒ green, negative ⇒ red. Computed as
       *     (sum_payouts - n_resolved) / n_resolved using 1-unit stake per
       *     slip, matching the backend's pnl_timeline math. */}
      <div className="bt-summary">
        <StatCard label="Slips (Done / Total)" sub="Recent 300" value={`${slipsDone} / ${slipsTotal}`} />
        <StatCard
          label="Slip Hit Rate"
          sub={slipBeWeighted != null ? `BE ${slipBeWeighted.toFixed(2)}%` : "BE —"}
          value={fmt(slipHitRate)}
          tone={slipsDone === 0 || slipBeWeighted == null ? "neutral" : (slipHitRate >= slipBeWeighted ? "good" : "bad")}
        />
        <StatCard label="Legs (Done / Total)" sub="Recent 300" value={`${legsDone} / ${legsTotal}`} />
        <StatCard label="Leg Hit Rate" sub="BE 54.08%" value={fmt(legHitRate)} tone={legsDone === 0 ? "neutral" : (legHitRate >= 54.08 ? "good" : "bad")} />
        <StatCard label="Exp. Leg Hit Rate" value={fmt(expLegHitRate)} />
        <StatCard
          label="Actual ROI"
          sub={done.length > 0 ? `${totalProfitUnits >= 0 ? "+" : ""}${totalProfitUnits.toFixed(2)}u / ${totalStakeUnits}u` : null}
          value={(roi >= 0 ? "+" : "") + fmt(roi)}
          tone={done.length === 0 ? "neutral" : (roi >= 0 ? "good" : "bad")}
        />
      </div>

      {loadState === "loading" && <div style={{padding:"20px", color:"var(--text-3)"}}>Loading slips…</div>}
      {loadState === "error" && <div style={{padding:"20px", color:"#FCA5A5"}}>Error: {errMsg}</div>}
      {loadState === "ok" && slipsView.length === 0 && (
        <div style={{padding:"32px", color:"var(--text-3)", textAlign:"center"}}>No logged slips yet. Save a slip from the +EV Bets tab to start tracking your backtest.</div>
      )}

      {/* Slip grid */}
      <div className="bt-slips-grid">
        {slipsView.map(s => <SlipCard key={s.id} slip={s} />)}
      </div>
    </main>
  );
}

function StatCard({ label, sub, value, tone }) {
  return (
    <div className={"bt-card tone-" + (tone || "neutral")}>
      <div className="bt-card-label">
        {label}
        {sub && <span className="bt-card-sub">({sub})</span>}
      </div>
      <div className="bt-card-value">{value}</div>
    </div>
  );
}

function SlipCard({ slip }) {
  // Status drives the entire card border + badge color:
  //   pending → yellow, hit → green, miss/loss → red, push/dnp → muted yellow
  const resultLabel = {
    hit:     { t: "WIN",     cls: "is-win" },
    miss:    { t: "LOSS",    cls: "is-loss" },
    push:    { t: "PUSH",    cls: "is-push" },
    dnp:     { t: "DNP",     cls: "is-push" },
    pending: { t: "PENDING", cls: "is-pending" },
  }[slip.result];
  const [placeState, setPlaceState] = React.useState("idle");

  // Slip-level +EV%: expected return per 1u stake, in %.
  const evPct = React.useMemo(
    () => btSlipEvPct(slip.type, slip.bets),
    [slip.type, slip.bets]
  );

  // Place-on-PrizePicks. Two-step flow mirroring the old vanilla-JS UI:
  //   1. POST the slip's legs to /api/pending-slip → backend queues it.
  //   2. Open https://app.prizepicks.com/ in a new tab → the CoreProp
  //      Chrome extension's content script picks up the queued slip,
  //      builds it on PrizePicks via DOM automation, and clears the queue.
  const placeOnPP = async () => {
    if (placeState === "sending") return;
    setPlaceState("sending");
    try {
      const legs = (slip.bets || []).map(b => ({
        player: b.player,
        league: b.league,
        prop:   b.propName || b.prop,
        line:   b.line,
        side:   b.side === "O" ? "over" : "under",
      }));
      if (!legs.length) throw new Error("slip has no legs");
      await window.cpApi.apiFetch("/api/pending-slip", {
        method: "POST",
        body: {
          legs,
          slip_type: slip.type || "Power",
          n_legs:    legs.length,
        },
      });
      // Open PrizePicks so the extension's content script fires. Same
      // user-gesture chain as the click guarantees the popup isn't blocked.
      window.open("https://app.prizepicks.com/", "_blank", "noopener,noreferrer");
      setPlaceState("queued");
      setTimeout(() => setPlaceState(s => s === "queued" ? "idle" : s), 4000);
    } catch (ex) {
      console.error("place failed:", ex);
      setPlaceState("error");
      setTimeout(() => setPlaceState(s => s === "error" ? "idle" : s), 3000);
    }
  };

  return (
    <article className={"bt-slip bt-slip-compact " + resultLabel.cls}>
      <header className="bt-slip-hd">
        <div className="bt-slip-hd-l">
          <span className="bt-slip-type">{slip.type} · {slip.legs}L</span>
          <span className="bt-slip-ts" title="When this slip was logged to your backtest">Logged {slip.ts}</span>
        </div>
        <div className="bt-slip-hd-r">
          {evPct != null && (
            <span
              className={"bt-slip-ev " + (evPct >= 0 ? "is-pos" : "is-neg")}
              title="Expected value per 1u stake using each leg's true probability"
            >
              {evPct >= 0 ? "+" : ""}{evPct.toFixed(1)}% EV
            </span>
          )}
          <span className={"bt-slip-badge " + resultLabel.cls}>{resultLabel.t}</span>
        </div>
      </header>

      <ul className="bt-slip-legs">
        {slip.bets.map((b, i) => {
          // Decide what to display in the actual-value pill:
          //   - pending → "—" (no result yet)
          //   - dnp     → "DNP"
          //   - push    → "P"
          //   - hit/miss → actual stat number when present, else fall back to
          //     a glyph so the box never renders empty.
          let actualDisplay;
          if (b.result === "pending") actualDisplay = "—";
          else if (b.result === "dnp") actualDisplay = "DNP";
          else if (b.result === "push") actualDisplay = "P";
          else if (b.actual != null && b.actual !== "" && b.actual !== "—") actualDisplay = b.actual;
          else actualDisplay = b.result === "hit" ? "✓" : "✕";
          return (
            <li key={i} className={"bt-slip-leg leg-" + b.result}>
              <div className="bt-leg-body">
                <div className="bt-leg-name">{b.player}</div>
                <div className="bt-leg-prop">
                  {b.league && <span className="bt-leg-league">{b.league}</span>}
                  <span className="bt-leg-prop-name">{b.propName}</span>
                  <span className={"bt-leg-side bt-leg-side-" + (b.side === "O" ? "over" : "under")}>{b.side}{b.line}</span>
                </div>
              </div>
              <span className="bt-leg-pct mono" title="Modeled win probability">{(b.pct || 0).toFixed(1)}%</span>
              <span className={"bt-leg-actual mono leg-" + b.result}>{actualDisplay}</span>
            </li>
          );
        })}
      </ul>

      <button
        type="button"
        className={"bt-slip-place bt-slip-place-" + placeState}
        onClick={placeOnPP}
        disabled={placeState === "sending"}
        title="Send this slip's legs to the PrizePicks Chrome extension"
      >
        {placeState === "sending" ? "Sending to extension…"
          : placeState === "queued" ? "Queued for extension ✓"
          : placeState === "error" ? "Retry"
          : "Place on PrizePicks"}
      </button>
    </article>
  );
}

Object.assign(window, { BacktestPage, BT_SLIPS });
