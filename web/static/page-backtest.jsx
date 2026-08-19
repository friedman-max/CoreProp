// Backtest — slip-level history matching web/static/index.html `#backtest-view`.
const { useState: useStateBT, useMemo: useMemoBT } = React;

// Live data only — slips come from /api/backtest/slips. No sample fallback.

// Mirror engine/constants.py exactly. Used to recompute slip payout from
// leg outcomes when the API's `payout` or `completed` flags are missing
// (e.g. older slips whose legs use "won"/"lost" instead of "hit"/"miss").
// KEEP IN SYNC with engine/constants.py: Power-6 pays 37.5x (not the old 40x).
const BT_POWER_PAYOUTS = { 2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 37.5 };

// Per-leg break-even for a 6-leg Power slip — the standard reference point for
// individual-leg quality. DERIVED from the payout table, never hardcoded: when
// PrizePicks cut 6-Power from 40x to 37.5x this moved 54.07% -> 54.66%, and the
// hardcoded copies here and in page-analytics.jsx went stale, tinting a
// 54.1%-54.6% hit rate GREEN when it is actually EV-NEGATIVE. Mirrors
// engine/constants.py BREAK_EVEN[("6","power")]; enforced by
// tests/engine_tests/test_payout_table_mirror.py.
const CP_LEG_BE_6_POWER_PCT = Math.pow(1 / BT_POWER_PAYOUTS[6], 1 / 6) * 100;
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
    // Game start time for this leg, formatted short (e.g. "7/16 7:05 PM"). The
    // column is game_start (see legs table); older rows without it show nothing.
    const gs = l.game_start || l.start_time || null;
    const gsDate = gs ? new Date(gs) : null;
    // Keep the raw epoch too: placement must be blocked once a game has
    // started, and the formatted string can't be compared.
    const gameStartMs = gsDate && !isNaN(gsDate.getTime()) ? gsDate.getTime() : null;
    const gameTime = gameStartMs
      ? gsDate.toLocaleString([], { month: "numeric", day: "numeric", hour: "numeric", minute: "2-digit" })
      : "";
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
      gameTime,
      gameStartMs,
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

// Page-level auto-place status. Lives here, not only in the avatar menu:
// this is the page where placement actually happens, and a control buried in a
// small dropdown on a phone is a control nobody finds. Fetched once per page
// rather than per slip card — there can be dozens of cards.
function AutoPlaceStrip() {
  const [st, setSt] = useState(null);
  const load = React.useCallback(() => {
    if (!window.cpApi || !window.cpApi.isLoggedIn()) return;
    window.cpApi.apiFetch("/api/auto-place/status").then(setSt).catch(() => setSt(null));
  }, []);
  useEffect(() => {
    load();
    // Re-read whenever the panel saves, so this strip never contradicts it.
    window.addEventListener("cp:auto-place-changed", load);
    return () => window.removeEventListener("cp:auto-place-changed", load);
  }, [load]);

  if (!st) return null;
  const open = () => window.dispatchEvent(new CustomEvent("cp:open-auto-place"));
  return (
    <div className={"bt-ap-strip " + (st.armed ? "is-armed" : "")}>
      <span className="bt-ap-dot" aria-hidden="true" />
      <span className="bt-ap-txt">
        {st.armed
          ? <>Auto-place <b>armed</b> · {st.mode} · ${st.stake}/slip
              {st.spent_today != null && <> · ${Number(st.spent_today).toFixed(2)} of ${Number(st.daily_cap).toFixed(2)} today</>}</>
          : <>Auto-place is <b>off</b>{st.blocked_reason && st.mode !== "off" ? ` — ${st.blocked_reason}` : ""}</>}
      </span>
      <button type="button" className="bt-ap-btn" onClick={open}>
        {st.armed ? "Change" : "Set up"}
      </button>
    </div>
  );
}

function BacktestPage() {
  const [resultFilter, setResultFilter] = useState("");
  const [leagueFilter, setLeagueFilter] = useState("");
  const [page, setPage] = useState(1);
  const [slips, setSlips] = useState([]);
  const [loadState, setLoadState] = useState("loading");
  const [errMsg, setErrMsg] = useState("");
  // Slip queued for deletion — drives the confirmation modal. null = closed.
  const [pendingDelete, setPendingDelete] = useState(null);
  const [deleting, setDeleting] = useState(false);
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

  // Confirmed delete of a slip from the user's own backtest history.
  // Optimistically drop it from local state (every summary stat is derived
  // from `slips`, so they recompute immediately), then DELETE server-side.
  // On failure, roll back so the UI doesn't lie about what's stored.
  const confirmDelete = React.useCallback(async () => {
    const slip = pendingDelete;
    if (!slip || !slip.id) { setPendingDelete(null); return; }
    const slipId = slip.id;
    const prev = slips;
    setDeleting(true);
    setSlips(cur => cur.filter(s => s.id !== slipId));
    try {
      await window.cpApi.apiFetch(`/api/backtest/slip/${encodeURIComponent(slipId)}`, { method: "DELETE" });
      // Keep the SWR cache honest so the next poll/tab-switch doesn't restore
      // the deleted slip from a stale cached payload.
      if (window.cpApi.getCached) {
        const cachedNow = window.cpApi.getCached("/api/backtest/slips");
        if (cachedNow && Array.isArray(cachedNow.slips)) {
          cachedNow.slips = cachedNow.slips.filter(s => (s.id || s.slip_id) !== slipId);
        }
      }
      setPendingDelete(null);
    } catch (ex) {
      // Roll back on failure and surface the error in the modal.
      setSlips(prev);
      setPendingDelete(p => p ? { ...p, error: (ex.message || String(ex)) } : p);
    } finally {
      setDeleting(false);
    }
  }, [pendingDelete, slips]);

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
  // Only hit/miss legs count toward the hit rate. push and dnp are excluded
  // (they can never be "hit", so counting them only deflates the rate) —
  // matching the Analytics tab's Raw Hit Rate and the backend's _RESOLVED set.
  const doneLegs = allLegs.filter(l => l.result === "hit" || l.result === "miss");
  const legHits = doneLegs.filter(l => l.result === "hit").length;
  const slipHits = done.filter(s => s.result === "hit").length;

  // PrizePicks payout tables (must mirror engine/constants.py; Power-6 = 37.5x).
  // For Power, payout when all hit. For Flex, payout when all hit.
  const POWER_PAY = { 2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 37.5 };
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

  // Show shimmer instead of empty-derived zeros until the first fetch lands
  // (loading OR error, when we have nothing real yet). A genuine empty
  // dataset (loadState "ok" with 0 slips) still shows truthful zeros.
  const statsLoading = loadState !== "ok" && slips.length === 0;

  return (
    <main className="bd-page bt-page">
      <FiltersBar count={filtered.length} label="slips" page={page} totalPages={totalPages} onPage={setPage}>
        <label className="bd-f"><span>Result</span>
          <select value={resultFilter} onChange={e => { setResultFilter(e.target.value); setPage(1); }}>
            <option value="">All</option><option value="pending">Pending</option>
            <option value="hit">Hit</option><option value="miss">Miss</option>
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
       *   - Leg Hit Rate BE = CP_LEG_BE_6_POWER_PCT (geometric BE for Power 6
       *     — the standard reference point for individual-leg quality).
       *     Derived from the payout table so a payout change can't strand it.
       *   - ROI: positive ⇒ green, negative ⇒ red. Computed as
       *     (sum_payouts - n_resolved) / n_resolved using 1-unit stake per
       *     slip, matching the backend's pnl_timeline math. */}
      <AutoPlaceStrip />
      <div className="bt-summary">
        <StatCard loading={statsLoading} label="Slips (Done / Total)" sub="Recent 300" value={`${slipsDone} / ${slipsTotal}`} />
        <StatCard
          loading={statsLoading}
          label="Slip Hit Rate"
          sub={slipBeWeighted != null ? `BE ${slipBeWeighted.toFixed(2)}%` : "BE —"}
          value={fmt(slipHitRate)}
          tone={slipsDone === 0 || slipBeWeighted == null ? "neutral" : (slipHitRate >= slipBeWeighted ? "good" : "bad")}
        />
        <StatCard loading={statsLoading} label="Legs (Done / Total)" sub="Recent 300" value={`${legsDone} / ${legsTotal}`} />
        <StatCard loading={statsLoading} label="Leg Hit Rate" sub={`BE ${CP_LEG_BE_6_POWER_PCT.toFixed(2)}%`} value={fmt(legHitRate)} tone={legsDone === 0 ? "neutral" : (legHitRate >= CP_LEG_BE_6_POWER_PCT ? "good" : "bad")} />
        <StatCard loading={statsLoading} label="Exp. Leg Hit Rate" value={fmt(expLegHitRate)} />
        <StatCard
          loading={statsLoading}
          label="Actual ROI"
          sub={done.length > 0 ? `${totalProfitUnits >= 0 ? "+" : ""}${totalProfitUnits.toFixed(2)}u / ${totalStakeUnits}u` : null}
          value={(roi >= 0 ? "+" : "") + fmt(roi)}
          tone={done.length === 0 ? "neutral" : (roi >= 0 ? "good" : "bad")}
        />
      </div>

      {loadState === "loading" && <div style={{padding:"20px", color:"var(--text-3)"}}>Loading slips…</div>}
      {loadState === "error" && <div style={{padding:"20px", color:"#FCA5A5"}}>Error: {errMsg}</div>}
      {loadState === "ok" && slipsView.length === 0 && (
        <div style={{padding:"32px", color:"var(--text-3)", textAlign:"center"}}>
          {slips.length === 0
            ? "No logged slips yet. Save a slip from the +EV Bets tab to start tracking your backtest."
            : "No slips match your filters."}
        </div>
      )}

      {/* Slip grid */}
      <div className="bt-slips-grid">
        {slipsView.map(s => <SlipCard key={s.id} slip={s} onDelete={setPendingDelete} />)}
      </div>

      {pendingDelete && (
        <DeleteSlipModal
          slip={pendingDelete}
          busy={deleting}
          error={pendingDelete.error}
          onConfirm={confirmDelete}
          onCancel={() => { if (!deleting) setPendingDelete(null); }}
        />
      )}
    </main>
  );
}

// Confirmation modal for deleting a backtest slip. Styled to match the app's
// modal look (cp-modal) rather than a raw window.confirm().
function DeleteSlipModal({ slip, busy, error, onConfirm, onCancel }) {
  React.useEffect(() => {
    const onKey = (e) => {
      if (e.key === "Escape" && !busy) onCancel();
      if (e.key === "Enter" && !busy) onConfirm();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [busy, onConfirm, onCancel]);

  const legCount = slip.legs || (slip.bets || []).length;
  return (
    <div className="cp-modal-back" onMouseDown={(e) => { if (e.target === e.currentTarget && !busy) onCancel(); }}>
      <div className="cp-modal bt-del-modal" role="alertdialog" aria-modal="true" aria-labelledby="bt-del-title">
        <div className="bt-del-icon">🗑</div>
        <h3 id="bt-del-title" className="bt-del-title">Delete this slip?</h3>
        <p className="bt-del-body">
          <b>{slip.type} · {legCount}L</b> logged {slip.ts}. This permanently removes it
          from your backtest and updates your stats. This can’t be undone.
        </p>
        {error && <div className="bt-del-error">Couldn’t delete: {error}</div>}
        <div className="bt-del-actions">
          <button type="button" className="cp-btn cp-btn-ghost" onClick={onCancel} disabled={busy}>Cancel</button>
          <button type="button" className="cp-btn bt-del-confirm" onClick={onConfirm} disabled={busy}>
            {busy ? "Deleting…" : "Delete slip"}
          </button>
        </div>
      </div>
    </div>
  );
}

function StatCard({ label, sub, value, tone, loading }) {
  // While the first fetch is in flight, render a shimmer instead of a value
  // derived from an empty dataset (which would read as a real "0.0%" / "0/0").
  return (
    <div className={"bt-card tone-" + (loading ? "neutral" : (tone || "neutral"))}>
      <div className="bt-card-label">
        {label}
        {!loading && sub && <span className="bt-card-sub">({sub})</span>}
      </div>
      <div className="bt-card-value">
        {loading ? <span className="cp-skel" /> : value}
      </div>
    </div>
  );
}

/**
 * Is the CoreProp browser extension installed?
 *
 * Returns null while unknown, then true/false. The extension's beacon content
 * script sets `data-coreprop-ext` on <html> and answers a postMessage ping;
 * both are checked because a content script runs at document_idle, which can
 * land after React has already mounted and read the attribute.
 *
 * Without this the Backtest page had no way to know, so it reported
 * "Queued for extension ✓" to every user — including the overwhelming majority
 * who have no extension and for whom the flow is: click, blank tab, silence.
 */
function btUseExtensionPresent() {
  const [present, setPresent] = React.useState(
    () => (document.documentElement.dataset.corepropExt ? true : null)
  );

  // Empty dep list on purpose. Keying the effect on `present` would re-arm the
  // whole probe every time it resolved to false, i.e. a 1.2s polling loop for
  // the entire session on any machine without the extension.
  React.useEffect(() => {
    // Only a positive latches: the beacon is a content script and can land
    // after our timeout on a slow load, so a negative must stay revisable or
    // an installed extension would be reported missing for the whole session.
    let found = false;
    const settle = (v) => {
      if (found) return;
      found = !!v;
      setPresent(v);
    };

    const onMsg = (ev) => {
      if (ev.source !== window) return;
      const d = ev.data;
      if (d && d.source === "coreprop-extension" && (d.type === "pong" || d.type === "ready")) settle(true);
    };
    window.addEventListener("message", onMsg);
    window.postMessage({ source: "coreprop-page", type: "ping" }, window.location.origin);

    const iv = setInterval(() => {
      if (document.documentElement.dataset.corepropExt) settle(true);
    }, 400);
    // Answer "no" early so the install CTA appears promptly, but keep polling.
    const to = setTimeout(() => settle(!!document.documentElement.dataset.corepropExt), 1200);

    return () => {
      window.removeEventListener("message", onMsg);
      clearInterval(iv);
      clearTimeout(to);
    };
  }, []);

  return present;
}

function SlipCard({ slip, onDelete }) {
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
  const [placeErr, setPlaceErr] = React.useState(null);
  const extPresent = btUseExtensionPresent();

  // Placement gating. A settled slip's games are over, and a leg whose game has
  // tipped off can't be taken any more — but the extension matches on
  // player+prop+line with no date awareness, so a stale leg would happily stage
  // a bet on TONIGHT's game at the same number, priced by a model run days ago.
  const placeLegs = slip.bets || [];
  const isSettled = slip.result !== "pending";
  const hasStarted = placeLegs.some(b => b.gameStartMs != null && b.gameStartMs <= Date.now());
  const canPlace = !isSettled && placeLegs.length >= 2 && !hasStarted;

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
    setPlaceErr(null);
    // Open the tab synchronously, before any await: Chrome's transient user
    // activation expires (~5s) while the POST runs, so a window.open after
    // the await gets blocked. No "noopener" here since a null handle would
    // be indistinguishable from a blocked popup, and we redirect it below.
    const ppTab = window.open("about:blank", "_blank");
    const badLeg = (msg) => {
      const err = new Error(msg);
      err.badLeg = true;
      return err;
    };
    try {
      const legs = (slip.bets || []).map(b => {
        // Don't default a blank side to "under": that would place the wrong bet.
        if (!b.side || !String(b.side).trim()) throw badLeg("Leg missing over/under side.");
        if (!Number.isFinite(parseFloat(b.line))) throw badLeg("Leg has an invalid line.");
        return {
          player: b.player,
          league: b.league,
          // propName is PrizePicks' own stat_type verbatim (web/app.py sets
          // prop_type=m.pp.stat_type), which is what lets the extension match
          // the card's stat label directly instead of guessing.
          prop:   b.propName || b.prop,
          line:   b.line,
          side:   b.side === "O" ? "over" : "under",
          game_start: b.gameStartMs != null ? new Date(b.gameStartMs).toISOString() : null,
        };
      });
      if (!legs.length) throw badLeg("Slip has no legs.");
      const res = await window.cpApi.apiFetch("/api/pending-slip", {
        method: "POST",
        body: {
          legs,
          slip_type: slip.type || "Power",
          n_legs:    legs.length,
          // Request unattended submission. The server HONORS this only for a
          // user who armed LIVE auto-place (with consent, within their daily
          // cap) and picks/clamps the stake itself — for everyone else it is
          // ignored and the extension just stages the slip as before.
          auto_submit: true,
        },
      });
      // Pass the single-use token so the extension fetches back exactly this
      // slip (not whatever another user queued most recently). It goes in the
      // FRAGMENT, not the query: a query string is transmitted to PrizePicks'
      // own servers and lands in their access logs, and this token is the sole
      // credential for reading and deleting the slip. Fragments are never sent.
      const ppUrl = res && res.token
        ? "https://app.prizepicks.com/#cp_slip=" + encodeURIComponent(res.token)
        : "https://app.prizepicks.com/";
      if (!ppTab) {
        // Blocked popup: don't claim success, hand the user the link instead.
        setPlaceErr({ msg: "Popup blocked. Click to open PrizePicks:", url: ppUrl });
        setPlaceState("error");
        setTimeout(() => setPlaceState(s => s === "error" ? "idle" : s), 3000);
        return;
      }
      ppTab.location.href = ppUrl;
      setPlaceState("queued");
      setTimeout(() => setPlaceState(s => s === "queued" ? "idle" : s), 6000);
    } catch (ex) {
      console.error("place failed:", ex);
      if (ppTab) ppTab.close();
      // "error" must stay CLICKABLE. It previously fell under
      // disabled={placeState !== "idle"}, and the 401 branch returned without
      // scheduling a reset — leaving a greyed-out button labelled "Retry" that
      // could never be clicked, recoverable only by reloading the page.
      setPlaceState("error");
      if (ex && ex.status === 401) {
        setPlaceErr({ msg: "Session expired — sign in again, then retry." });
        return;
      }
      setPlaceErr({ msg: ex && ex.badLeg ? ex.message : "Couldn't reach CoreProp. Try again." });
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
          {onDelete && (
            <button
              type="button"
              className="bt-slip-del"
              title="Delete this slip from your backtest"
              aria-label="Delete slip"
              onClick={() => onDelete(slip)}
            >✕</button>
          )}
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
                <div className="bt-leg-name">
                  <span className="bt-leg-player">{b.player}</span>
                  {b.gameTime && <span className="bt-leg-time" title="Game start time">{b.gameTime}</span>}
                </div>
                <div className="bt-leg-prop">
                  {b.league && <span className="bt-leg-league">{b.league}</span>}
                  <span className="bt-leg-prop-name">{b.propName}</span>
                  <span className={"bt-leg-side bt-leg-side-" + (b.side === "O" ? "over" : "under")}>{b.side === "O" ? "▲" : "▼"}{b.line}</span>
                </div>
              </div>
              <span className="bt-leg-pct mono" title="Modeled win probability">{(b.pct || 0).toFixed(1)}%</span>
              <span className={"bt-leg-actual mono leg-" + b.result}>{actualDisplay}</span>
            </li>
          );
        })}
      </ul>

      {/* A settled slip's games are over — no button at all. This also clears
          the call-to-action off the majority of cards, where it was noise. */}
      {!isSettled && (
        hasStarted ? (
          <button type="button" className="bt-slip-place" disabled
            title="One or more games have already started">Game already started</button>
        ) : extPresent === false ? (
          <a className="bt-slip-place bt-slip-place-install" href="/extension" target="_blank" rel="noopener"
             title="One-click placement needs the CoreProp browser extension">
            Get the extension to place this
          </a>
        ) : (
          <button
            type="button"
            className={"bt-slip-place bt-slip-place-" + placeState}
            onClick={placeOnPP}
            disabled={placeState === "sending" || placeState === "queued" || !canPlace}
            title="Open PrizePicks and stage this slip's legs"
          >
            {placeState === "sending" ? "Sending…"
              : placeState === "queued" ? `Opened PrizePicks — staging ${placeLegs.length} legs…`
              : placeState === "error" ? "Retry"
              : "Place on PrizePicks"}
          </button>
        )
      )}
      {placeState === "queued" && (
        <div className="bt-place-note">
          Watch the CoreProp panel in the PrizePicks tab — it reports each leg.
          {slip.type && <> Set the entry to <b>{slip.type}</b> before you submit.</>}
        </div>
      )}
      {placeErr && (
        <div className="bt-place-error">
          {placeErr.msg}
          {placeErr.url && (
            <> <a href={placeErr.url} target="_blank" rel="noopener">Open PrizePicks</a></>
          )}
        </div>
      )}
    </article>
  );
}

Object.assign(window, { BacktestPage });
