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

function btResultFromSlip(s) {
  if (!s.completed) return "pending";
  const legs = s.legs || [];
  const eff = legs.filter(l => l.result !== "push" && l.result !== "dnp");
  if (eff.length === 0) return "push";
  return (s.payout || 0) > (s.stake || 0) ? "hit" : "miss";
}

function btMapSlip(s) {
  const legs = (s.legs || []).map(l => ({
    player: l.player_name || l.player || "—",
    prop: [l.prop_type || l.stat_type || "", (l.side || "").toUpperCase().startsWith("O") ? "O" : "U", l.line ?? l.pp_line ?? ""].join(" ").trim(),
    pct: l.true_prob != null ? l.true_prob * 100 : 0,
    result: l.result || "pending",
    actual: l.actual_value != null ? String(l.actual_value) : (l.result === "pending" ? "—" : "—"),
  }));
  const result = btResultFromSlip(s);
  const ts = s.timestamp ? new Date(s.timestamp).toLocaleString([], { month: "numeric", day: "numeric", hour: "numeric", minute: "2-digit" }) : "—";
  // Pick a representative league (most common across legs) — or MIXED.
  const leagueCounts = {};
  for (const l of (s.legs || [])) { const k = l.league || "MIXED"; leagueCounts[k] = (leagueCounts[k] || 0) + 1; }
  const leagueKeys = Object.keys(leagueCounts);
  const league = leagueKeys.length === 1 ? leagueKeys[0] : "MIXED";
  return {
    id: s.id || s.slip_id,
    ts,
    type: ((s.slip_type || "power").charAt(0).toUpperCase() + (s.slip_type || "power").slice(1)),
    legs: s.n_legs || legs.length,
    league,
    stake: s.stake || 0,
    payout: s.payout,
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
  const PER = 8;

  React.useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await window.cpApi.apiFetch("/api/backtest/slips");
        if (cancelled) return;
        setSlips((data.slips || []).map(btMapSlip));
        setLoadState("ok");
      } catch (ex) {
        if (cancelled) return;
        setErrMsg(ex.message || "Failed to load slips.");
        setLoadState("error");
      }
    })();
    return () => { cancelled = true; };
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
  const totalStake = done.reduce((a, s) => a + (s.stake || 0), 0);
  const totalPayout = done.reduce((a, s) => a + (s.payout || 0), 0);

  const slipsDone = done.length;
  const slipsTotal = slips.length;
  const legsDone = doneLegs.length;
  const legsTotal = allLegs.length;
  const slipHitRate = slipsDone ? (slipHits / slipsDone) * 100 : 0;
  const legHitRate = legsDone ? (legHits / legsDone) * 100 : 0;
  const expLegHitRate = doneLegs.length ? doneLegs.reduce((a, l) => a + (l.pct || 0), 0) / doneLegs.length : 0;
  const roi = totalStake > 0 ? ((totalPayout - totalStake) / totalStake) * 100 : 0;

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

      {/* Summary cards */}
      <div className="bt-summary">
        <StatCard label="Slips (Done / Total)" sub="Recent 300" value={`${slipsDone} / ${slipsTotal}`} />
        <StatCard label="Slip Hit Rate" sub="Recent 300" value={fmt(slipHitRate)} tone="good" />
        <StatCard label="Legs (Done / Total)" sub="Recent 300" value={`${legsDone} / ${legsTotal}`} />
        <StatCard label="Leg Hit Rate" sub="BE 54.08%" value={fmt(legHitRate)} tone="good" />
        <StatCard label="Exp. Leg Hit Rate" value={fmt(expLegHitRate)} />
        <StatCard label="Actual ROI" value={"+" + fmt(roi)} tone="good" />
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
  const resultLabel = {
    hit: { t: "WIN",     cls: "is-win" },
    miss: { t: "LOSS",   cls: "is-loss" },
    push: { t: "PUSH",   cls: "is-push" },
    dnp: { t: "DNP",     cls: "is-push" },
    pending: { t: "PENDING", cls: "is-pending" },
  }[slip.result];
  const pl = slip.result === "pending" ? null : (slip.payout || 0) - slip.stake;
  return (
    <article className={"bt-slip " + resultLabel.cls}>
      <header className="bt-slip-hd">
        <div className="bt-slip-hd-l">
          <span className="bt-slip-type">{slip.type} · {slip.legs} Leg</span>
          <span className="bt-slip-ts">{slip.ts}</span>
        </div>
        <span className={"bt-slip-badge " + resultLabel.cls}>{resultLabel.t}</span>
      </header>

      <ul className="bt-slip-legs">
        {slip.bets.map((b, i) => (
          <li key={i} className={"bt-slip-leg leg-" + b.result}>
            <span className="bt-leg-i mono">{i + 1}</span>
            <div className="bt-leg-body">
              <div className="bt-leg-name">{b.player}</div>
              <div className="bt-leg-prop">{b.prop}</div>
            </div>
            <span className="bt-leg-pct mono">{b.pct.toFixed(1)}%</span>
            <span className={"bt-leg-actual mono leg-" + b.result}>{b.actual}</span>
          </li>
        ))}
      </ul>

      <footer className="bt-slip-foot">
        <div className="bt-slip-foot-c">
          <span className="bt-slip-foot-k">Stake</span>
          <span className="bt-slip-foot-v mono">${slip.stake}</span>
        </div>
        <div className="bt-slip-foot-c">
          <span className="bt-slip-foot-k">Payout</span>
          <span className="bt-slip-foot-v mono">{slip.payout == null ? "—" : "$" + slip.payout}</span>
        </div>
        <div className="bt-slip-foot-c">
          <span className="bt-slip-foot-k">P/L</span>
          <span className={"bt-slip-foot-v mono " + (pl > 0 ? "tone-good" : pl < 0 ? "tone-bad" : "")}>
            {pl == null ? "—" : (pl >= 0 ? "+" : "") + "$" + pl}
          </span>
        </div>
      </footer>
    </article>
  );
}

Object.assign(window, { BacktestPage, BT_SLIPS });
