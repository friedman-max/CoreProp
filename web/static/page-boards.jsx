// Combined Lines, PrizePicks Lines, Sportsbooks — all table-driven boards.
// Mirrors structure from web/static/index.html.

const { useState: useStateB, useMemo: useMemoB } = React;

// Convert true probability → American odds
function probToAmerican(p) {
  if (p >= 0.5) return Math.round(-100 * p / (1 - p));
  return Math.round(100 * (1 - p) / p);
}

const LEAGUE_ORDER = ["NBA", "WNBA", "NCAAB", "MLB", "NHL"];

// True odds derived from true probability (fallback when server doesn't include it)
function withTrueOdds(row) {
  if (row.trueOdds != null) return row;
  if (row.truePct == null) return row;
  return { ...row, trueOdds: probToAmerican(row.truePct / 100) };
}

function fmtGameTimeB(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleDateString([], { month: "numeric", day: "numeric" }) +
    " " + d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
}

function bestOdds(row) {
  const offers = [["FD", row.fd], ["DK", row.dk], ["PIN", row.pin], ["NV", row.nv]].filter(([_, o]) => o != null);
  if (!offers.length) return [null, null];
  // higher (less negative or more positive) odds = better for bettor
  return offers.reduce((a, b) => (a[1] > b[1] ? a : b));
}

// Reusable filter bar
function FiltersBar({ children, count, label = "lines", page, totalPages, onPage }) {
  return (
    <div className="bd-filters">
      {children}
      <span className="bd-badge">{count} {label}</span>
      <div className="bd-pag">
        <button className="bd-pag-btn" disabled={page <= 1} onClick={() => onPage(page - 1)}>‹</button>
        <span className="bd-pag-num mono">{page} / {totalPages}</span>
        <button className="bd-pag-btn" disabled={page >= totalPages} onClick={() => onPage(page + 1)}>›</button>
      </div>
    </div>
  );
}

function OddsCell({ value, best }) {
  if (value == null) return <span className="bd-odds-empty">—</span>;
  const cls = "bd-odds " + (best ? "is-best" : "") + " " + (value > 0 ? "is-plus" : "");
  return <span className={cls}>{value > 0 ? "+" + value : value}</span>;
}

// Full-width table-body row for the loading / error / no-results states, so a
// board never renders as a bare header over a blank panel (indistinguishable
// from a data outage). `state` comes from useBoardLines: loading | ok | error.
function BoardEmptyRow({ cols, state, error, empty, onClear }) {
  let msg;
  if (state === "loading") msg = "Loading…";
  else if (state === "error") msg = "Couldn't load lines" + (error ? ": " + error : ".");
  else msg = empty || "No lines match your filters.";
  return (
    <tr>
      <td className="bd-empty" colSpan={cols}>
        {msg}
        {state === "ok" && onClear && <button className="bd-clear" style={{ marginLeft: 10 }} onClick={onClear}>Clear filters</button>}
      </td>
    </tr>
  );
}

// ───────── Combined Lines ─────────
function CombinedLinesPage() {
  const [league, setLeague] = useState("");
  const [propQ, setPropQ] = useState("");
  const [player, setPlayer] = useState("");
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState({ col: "truePct", dir: "desc" });
  const PER = 25;

  const { rows: data, state: loadState, error } = window.cpApi.useBoardLines("/api/matched", "matches");

  const filtered = useMemo(() => {
    let r = data.filter(x =>
      (!league || x.league === league) &&
      (!propQ || (x.prop || "").toLowerCase().includes(propQ.toLowerCase())) &&
      (!player || (x.player || "").toLowerCase().includes(player.toLowerCase()))
    ).map(withTrueOdds);
    r.sort((a, b) => {
      const A = a[sort.col], B = b[sort.col];
      if (A == null && B == null) return 0;
      if (A == null) return 1;
      if (B == null) return -1;
      if (typeof A === "string") return sort.dir === "asc" ? A.localeCompare(B) : B.localeCompare(A);
      return sort.dir === "asc" ? A - B : B - A;
    });
    return r;
  }, [data, league, propQ, player, sort]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PER));
  const rows = filtered.slice((page - 1) * PER, page * PER);

  const sortBy = (col) => setSort(s => s.col === col ? { col, dir: s.dir === "asc" ? "desc" : "asc" } : { col, dir: "desc" });
  const sortClass = (col) => "sortable " + (sort.col === col ? "is-active is-" + sort.dir : "");

  return (
    <main className="bd-page">
      <FiltersBar count={filtered.length} page={page} totalPages={totalPages} onPage={setPage}>
        <label className="bd-f"><span>League</span>
          <select value={league} onChange={e => { setLeague(e.target.value); setPage(1); }}>
            <option value="">All</option>
            {LEAGUE_ORDER.map(l => <option key={l}>{l}</option>)}
          </select>
        </label>
        <label className="bd-f bd-f-grow"><span>Prop type</span>
          <input value={propQ} onChange={e => { setPropQ(e.target.value); setPage(1); }} placeholder="e.g. Points" />
        </label>
        <label className="bd-f bd-f-grow"><span>Player</span>
          <input value={player} onChange={e => { setPlayer(e.target.value); setPage(1); }} placeholder="e.g. LeBron" />
        </label>
        <button className="bd-clear" onClick={() => { setLeague(""); setPropQ(""); setPlayer(""); }}>Clear</button>
      </FiltersBar>

      <div className="bd-tbl-wrap">
        <table className="bd-tbl">
          <thead>
            <tr>
              <th className={sortClass("player")} onClick={() => sortBy("player")}>Player</th>
              <th className={sortClass("league")} onClick={() => sortBy("league")}>League</th>
              <th className={sortClass("prop")} onClick={() => sortBy("prop")}>Prop Type</th>
              <th className={sortClass("line")} onClick={() => sortBy("line")}>Line</th>
              <th className={sortClass("side")} onClick={() => sortBy("side")}>Side</th>
              <th className={sortClass("trueOdds")} onClick={() => sortBy("trueOdds")}>True Odds</th>
              <th>Best</th>
              <th>FD</th>
              <th>DK</th>
              <th>PIN</th>
              <th>NV</th>
              <th className={sortClass("startTime")} onClick={() => sortBy("startTime")}>Game Time</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => {
              const [bestBook] = bestOdds(r);
              return (
                <tr key={i}>
                  <td className="bd-player">{r.player}</td>
                  <td><LeaguePill league={r.league} /></td>
                  <td className="bd-muted">{r.prop}</td>
                  <td className="mono">{r.line}</td>
                  <td className={"bd-side " + (r.side === "OVER" ? "is-over" : "is-under")}>{r.side}</td>
                  <td className="mono bd-true-odds">{r.trueOdds > 0 ? "+" + r.trueOdds : r.trueOdds}</td>
                  <td><OddsCell value={r[bestBook?.toLowerCase()]} best /></td>
                  <td><OddsCell value={r.fd} best={bestBook === "FD"} /></td>
                  <td><OddsCell value={r.dk} best={bestBook === "DK"} /></td>
                  <td><OddsCell value={r.pin} best={bestBook === "PIN"} /></td>
                  <td><OddsCell value={r.nv} best={bestBook === "NV"} /></td>
                  <td className="bd-time mono">{fmtGameTimeB(r.startTime)}</td>
                </tr>
              );
            })}
            {rows.length === 0 && (
              <BoardEmptyRow cols={12} state={loadState} error={error}
                onClear={() => { setLeague(""); setPropQ(""); setPlayer(""); setPage(1); }} />
            )}
          </tbody>
        </table>
      </div>
    </main>
  );
}

// ───────── PrizePicks Lines ─────────
function PrizePicksPage() {
  const [league, setLeague] = useState("");
  const [propQ, setPropQ] = useState("");
  const [player, setPlayer] = useState("");
  const [page, setPage] = useState(1);
  const PER = 30;
  const { rows: data, state: loadState, error } = window.cpApi.useBoardLines("/api/prizepicks", "lines");

  const filtered = useMemo(() =>
    data.filter(x =>
      (!league || x.league === league) &&
      (!propQ || (x.prop || "").toLowerCase().includes(propQ.toLowerCase())) &&
      (!player || (x.player || "").toLowerCase().includes(player.toLowerCase()))
    ), [data, league, propQ, player]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PER));
  const rows = filtered.slice((page - 1) * PER, page * PER);

  return (
    <main className="bd-page">
      <FiltersBar count={filtered.length} page={page} totalPages={totalPages} onPage={setPage}>
        <label className="bd-f"><span>League</span>
          <select value={league} onChange={e => setLeague(e.target.value)}>
            <option value="">All</option>
            {LEAGUE_ORDER.map(l => <option key={l}>{l}</option>)}
          </select>
        </label>
        <label className="bd-f bd-f-grow"><span>Prop type</span>
          <input value={propQ} onChange={e => setPropQ(e.target.value)} placeholder="e.g. Points" />
        </label>
        <label className="bd-f bd-f-grow"><span>Player</span>
          <input value={player} onChange={e => setPlayer(e.target.value)} placeholder="e.g. LeBron" />
        </label>
        <button className="bd-clear" onClick={() => { setLeague(""); setPropQ(""); setPlayer(""); }}>Clear</button>
      </FiltersBar>

      <div className="bd-tbl-wrap">
        <table className="bd-tbl">
          <thead>
            <tr>
              <th>Player</th><th>League</th><th>Prop Type</th><th>Line</th><th>Side</th><th>Game Time</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i}>
                <td className="bd-player">{r.player}</td>
                <td><LeaguePill league={r.league} /></td>
                <td className="bd-muted">{r.prop}</td>
                <td className="mono">{r.line}</td>
                <td className={"bd-side " + (r.side === "OVER" ? "is-over" : "is-under")}>{r.side}</td>
                <td className="bd-time mono">{fmtGameTimeB(r.startTime)}</td>
              </tr>
            ))}
            {rows.length === 0 && (
              <BoardEmptyRow cols={6} state={loadState} error={error}
                empty="No PrizePicks lines right now."
                onClear={() => { setLeague(""); setPropQ(""); setPlayer(""); }} />
            )}
          </tbody>
        </table>
      </div>
    </main>
  );
}

// ───────── Sportsbooks (per-book view) ─────────
function SportsbooksPage() {
  const [book, setBook] = useState("fd");
  const [league, setLeague] = useState("");
  const [propQ, setPropQ] = useState("");
  const [player, setPlayer] = useState("");
  const [page, setPage] = useState(1);
  const PER = 25;
  const bookEndpoint = { fd: "/api/fanduel", dk: "/api/draftkings", pin: "/api/pinnacle" }[book];
  const { rows: data, state: loadState, error } = window.cpApi.useBoardLines(bookEndpoint, "lines");

  // Single-book endpoints (/api/fanduel etc.) return each row with the
  // book's own odds in `bookOdds` (from line_odds) — there are no
  // fd/dk/pin columns to filter on. Keep any row that has a book price.
  const filtered = useMemo(() =>
    data
      .filter(x => x.bookOdds != null)
      .filter(x =>
        (!league || x.league === league) &&
        (!propQ || (x.prop || "").toLowerCase().includes(propQ.toLowerCase())) &&
        (!player || (x.player || "").toLowerCase().includes(player.toLowerCase()))
      ),
    [data, league, propQ, player]
  );

  const totalPages = Math.max(1, Math.ceil(filtered.length / PER));
  const rows = filtered.slice((page - 1) * PER, page * PER);

  const bookLabels = { fd: "FanDuel", dk: "DraftKings", pin: "Pinnacle" };

  return (
    <main className="bd-page">
      <FiltersBar count={filtered.length} page={page} totalPages={totalPages} onPage={setPage}>
        <label className="bd-f"><span>Book</span>
          <select value={book} onChange={e => { setBook(e.target.value); setPage(1); }}>
            <option value="fd">FanDuel</option>
            <option value="dk">DraftKings</option>
            <option value="pin">Pinnacle</option>
          </select>
        </label>
        <label className="bd-f"><span>League</span>
          <select value={league} onChange={e => setLeague(e.target.value)}>
            <option value="">All</option>
            {LEAGUE_ORDER.map(l => <option key={l}>{l}</option>)}
          </select>
        </label>
        <label className="bd-f bd-f-grow"><span>Prop type</span>
          <input value={propQ} onChange={e => setPropQ(e.target.value)} placeholder="e.g. Points" />
        </label>
        <label className="bd-f bd-f-grow"><span>Player</span>
          <input value={player} onChange={e => setPlayer(e.target.value)} placeholder="e.g. LeBron" />
        </label>
        <button className="bd-clear" onClick={() => { setLeague(""); setPropQ(""); setPlayer(""); }}>Clear</button>
      </FiltersBar>

      <div className="bd-tbl-wrap">
        <table className="bd-tbl">
          <thead>
            <tr>
              <th>Player</th><th>League</th><th>Prop Type</th><th>Line</th><th>Side</th><th>True Odds</th><th>{bookLabels[book]} Odds</th><th>Edge</th><th>Game Time</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => {
              const bookOdds = r.bookOdds;
              // +EV when the book pays more than our true (devigged) price.
              const edge = (r.trueOdds != null && bookOdds != null) && bookOdds > r.trueOdds;
              return (
                <tr key={i} className={edge ? "bd-edge-row" : ""}>
                  <td className="bd-player">{r.player}</td>
                  <td><LeaguePill league={r.league} /></td>
                  <td className="bd-muted">{r.prop}</td>
                  <td className="mono">{r.line}</td>
                  <td className={"bd-side " + (r.side === "OVER" ? "is-over" : "is-under")}>{r.side}</td>
                  <td className="mono bd-true-odds">{r.trueOdds != null ? (r.trueOdds > 0 ? "+" + r.trueOdds : r.trueOdds) : "—"}</td>
                  <td><OddsCell value={bookOdds} best={edge} /></td>
                  <td className={"bd-edge-cell " + (edge ? "is-edge" : "")}>{edge ? "+EV" : "—"}</td>
                  <td className="bd-time mono">{fmtGameTimeB(r.startTime)}</td>
                </tr>
              );
            })}
            {rows.length === 0 && (
              <BoardEmptyRow cols={9} state={loadState} error={error}
                empty={"No " + bookLabels[book] + " lines available right now."}
                onClear={() => { setLeague(""); setPropQ(""); setPlayer(""); }} />
            )}
          </tbody>
        </table>
      </div>
    </main>
  );
}

Object.assign(window, { CombinedLinesPage, PrizePicksPage, SportsbooksPage });
