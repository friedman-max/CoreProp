// Combined Lines, PrizePicks Lines, Sportsbooks — all table-driven boards.
// Mirrors structure from web/static/index.html.

const { useState: useStateB, useMemo: useMemoB } = React;

// Convert true probability → American odds
function probToAmerican(p) {
  if (p >= 0.5) return Math.round(-100 * p / (1 - p));
  return Math.round(100 * (1 - p) / p);
}

const LEAGUE_ORDER = ["NBA", "WNBA", "NCAAB", "MLB", "NHL"];

// Extended dataset used across all 3 boards. Real codebase shape:
// player_name, league, stat_type, pp_line, side, true_prob, fd_line/fd_odds, dk_line/dk_odds, pin_line/pin_odds, start_time
const BOARD_LINES = [
  { player: "Cale Makar",        league: "NHL",   prop: "Assists",       line: 0.5,  side: "OVER",  truePct: 72.4, fd: -138, dk: -145, pin: -135, time: "5/26 9:08 PM" },
  { player: "Courtney Williams", league: "WNBA",  prop: "Rebs+Asts",     line: 9.5,  side: "UNDER", truePct: 66.4, fd: -140, dk:  null, pin:  null, time: "5/27 9:00 PM" },
  { player: "Olivia Miles",      league: "WNBA",  prop: "Rebs+Asts",     line: 10.5, side: "UNDER", truePct: 63.6, fd: -136, dk:  null, pin:  null, time: "5/27 9:00 PM" },
  { player: "Cason Wallace",     league: "NBA",   prop: "Rebounds",      line: 3.5,  side: "UNDER", truePct: 62.0, fd: -132, dk: -122, pin: -133, time: "5/26 8:40 PM" },
  { player: "Jordan Wicks",      league: "MLB",   prop: "Hits Allowed",  line: 4.5,  side: "UNDER", truePct: 62.0, fd:  null, dk: -118, pin: -134, time: "5/26 6:40 PM" },
  { player: "Marina Mabrey",     league: "WNBA",  prop: "Rebs+Asts",     line: 7.5,  side: "UNDER", truePct: 60.2, fd: -130, dk:  null, pin:  null, time: "5/27 8:00 PM" },
  { player: "Paige Bueckers",    league: "WNBA",  prop: "Rebs+Asts",     line: 8.5,  side: "UNDER", truePct: 60.2, fd: -130, dk:  null, pin:  null, time: "5/28 8:00 PM" },
  { player: "Victor Wembanyama", league: "NBA",   prop: "Assists",       line: 3.5,  side: "UNDER", truePct: 59.9, fd: -134, dk: -123, pin: -141, time: "5/26 8:40 PM" },
  { player: "Luguentz Dort",     league: "NBA",   prop: "Rebounds",      line: 2.5,  side: "UNDER", truePct: 59.2, fd: -122, dk: -112, pin: -112, time: "5/26 8:40 PM" },
  { player: "Sean Burke",        league: "MLB",   prop: "Walks Allowed", line: 1.5,  side: "UNDER", truePct: 58.3, fd:  null, dk: -120, pin:  null, time: "5/26 7:40 PM" },
  { player: "Jaccob Slavin",     league: "NHL",   prop: "Blocked Shots", line: 1.5,  side: "OVER",  truePct: 58.0, fd: -190, dk:  null, pin:  null, time: "5/27 8:08 PM" },
  { player: "Jordan Wicks",      league: "MLB",   prop: "Walks Allowed", line: 1.5,  side: "UNDER", truePct: 57.8, fd:  null, dk: -119, pin:  null, time: "5/26 6:40 PM" },
  { player: "Nia Coffey",        league: "WNBA",  prop: "Rebounds",      line: 5.5,  side: "UNDER", truePct: 57.8, fd: -125, dk: -122, pin:  null, time: "5/27 9:00 PM" },
  { player: "Gabriela Jaquez",   league: "WNBA",  prop: "Points",        line: 11.5, side: "UNDER", truePct: 57.7, fd: -128, dk: -124, pin:  null, time: "5/27 8:00 PM" },
  { player: "Chet Holmgren",     league: "NBA",   prop: "Rebounds",      line: 7.5,  side: "UNDER", truePct: 57.6, fd: -114, dk: -115, pin: -112, time: "5/26 8:40 PM" },
  { player: "Jared McCain",      league: "NBA",   prop: "Rebounds",      line: 2.5,  side: "UNDER", truePct: 57.6, fd: -114, dk: -115, pin: -112, time: "5/26 8:40 PM" },
  { player: "Stephon Castle",    league: "NBA",   prop: "Points",        line: 17.5, side: "UNDER", truePct: 57.1, fd:  null, dk: -116, pin: -129, time: "5/26 8:40 PM" },
  { player: "Julian Champagnie", league: "NBA",   prop: "Pts+Rebs",      line: 14.5, side: "UNDER", truePct: 57.0, fd: -130, dk: -112, pin:  null, time: "5/26 8:40 PM" },
  { player: "Devin Vassell",     league: "NBA",   prop: "Rebounds",      line: 4.5,  side: "UNDER", truePct: 57.0, fd: -125, dk:  101, pin: -126, time: "5/26 8:40 PM" },
  { player: "Natasha Cloud",     league: "WNBA",  prop: "Rebounds",      line: 3.5,  side: "UNDER", truePct: 56.9, fd: -130, dk: -112, pin:  null, time: "5/27 8:00 PM" },
  { player: "Jalen Williams",    league: "NBA",   prop: "Points",        line: 14.5, side: "UNDER", truePct: 56.7, fd: -146, dk: -122, pin: -122, time: "5/26 8:40 PM" },
  { player: "RJ Barrett",        league: "NBA",   prop: "Assists",       line: 4.5,  side: "OVER",  truePct: 56.4, fd: -125, dk: -120, pin: -118, time: "5/26 7:30 PM" },
  { player: "Connor Bedard",     league: "NHL",   prop: "Shots On Goal", line: 3.5,  side: "OVER",  truePct: 56.1, fd: -132, dk: -125, pin: -130, time: "5/27 8:00 PM" },
  { player: "Pete Alonso",       league: "MLB",   prop: "Total Bases",   line: 1.5,  side: "OVER",  truePct: 55.9, fd: -118, dk: -115, pin: -120, time: "5/27 7:10 PM" },
  { player: "Riley Greene",      league: "MLB",   prop: "Hits",          line: 0.5,  side: "OVER",  truePct: 55.5, fd: -210, dk: -205, pin: -215, time: "5/27 7:10 PM" },
  { player: "Tyler O'Neil",      league: "NCAAB", prop: "Points",        line: 13.5, side: "UNDER", truePct: 55.2, fd: -120, dk: -118, pin:  null, time: "5/27 9:00 PM" },
];

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
  const offers = [["FD", row.fd], ["DK", row.dk], ["PIN", row.pin]].filter(([_, o]) => o != null);
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
              <th className={sortClass("time")} onClick={() => sortBy("time")}>Game Time</th>
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
                  <td className="bd-time mono">{fmtGameTimeB(r.startTime)}</td>
                </tr>
              );
            })}
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
  const { rows: data } = window.cpApi.useBoardLines("/api/prizepicks", "lines");

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
                <td className="bd-time mono">{r.time}</td>
              </tr>
            ))}
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
  const { rows: data } = window.cpApi.useBoardLines(bookEndpoint, "lines");

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

      <div className="bd-book-hd">
        <div className="bd-book-hd-l">
          <span className={"bd-book-chip bd-book-chip-" + book.toUpperCase()}>{book.toUpperCase()}</span>
          <span>{bookLabels[book]} lines</span>
        </div>
        <span className="bd-book-hd-r">Edge highlighted when book overpays vs. our true odds</span>
      </div>

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
          </tbody>
        </table>
      </div>
    </main>
  );
}

Object.assign(window, { CombinedLinesPage, PrizePicksPage, SportsbooksPage });
