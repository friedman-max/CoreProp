// Landing page for signed-out visitors.
//
// GROUND RULE FOR THIS FILE: every number shown here is either (a) read from
// the server at runtime via /api/public/coverage, (b) derived from
// engine/constants.py's payout table by the same formula the app uses, or (c)
// absent. The previous version claimed "2,847 bets scanned today", "4,200+
// sharps", a "58.4% backtested hit rate", "+11.7% ROI", "14,823 +EV bets
// surfaced this month", "+1,287 more edges available now", 16 fabricated
// "Yesterday's Winners" with fake box-score results dated to real yesterday,
// and three invented customer testimonials. None had a source. Two were
// falsifiable and false: the board refreshes on _state["interval_min"]
// (5 min default), not "every 30 seconds", and NFL was advertised while
// config.ACTIVE_LEAGUES has never included it.
//
// If you want a performance stat on this page, wire it to settled
// market_observatory rows first. Do not type a number in.

// The sample line in the hero panel. Illustrative prices, not a live quote —
// the panel says so in four words rather than a sentence of hedging.
const LP_EXAMPLE = {
  player: "Cale Makar",
  league: "NHL",
  prop: "Assists",
  line: 0.5,
  side: "OVER",
  // Both sides of one market at two books. Two-sided is the point: the vig only
  // becomes visible when you add the two prices' implied probabilities and get
  // more than 100%. The old panel showed one side at two books, which meant the
  // "strips the vig" claim had nothing on screen backing it up.
  books: [
    { book: "FanDuel",    over: -138, under: +114 },
    { book: "DraftKings", over: -145, under: +120 },
  ],
};

// Every figure the panel prints, computed from LP_EXAMPLE.books so the panel can
// never drift from its own arithmetic. Mirrors engine/devig.py: American ->
// implied, then the multiplicative devig (each side divided by the book's
// overround), then the cross-book mean.
const LP_EXAMPLE_MATH = (() => {
  const implied = (american) =>
    american < 0 ? -american / (-american + 100) : 100 / (american + 100);
  const rows = LP_EXAMPLE.books.map(({ book, over, under }) => {
    const io = implied(over), iu = implied(under);
    const total = io + iu;                 // > 1; the excess IS the vig
    return {
      book, over, under,
      overPct:  io * 100,                  // what the posted OVER price implies
      underPct: iu * 100,                  // ...and the UNDER
      totalPct: total * 100,               // > 100 by the book's margin
      vigPct:   (total - 1) * 100,
      fairPct:  (io / total) * 100,         // margin removed, sides normalized
    };
  });
  const mean = (xs) => xs.reduce((a, b) => a + b, 0) / xs.length;
  return {
    rows,
    fairPct: mean(rows.map((r) => r.fairPct)),
    vigPct:  mean(rows.map((r) => r.vigPct)),
  };
})();

// Per-leg break-even for a 6-leg Power slip, DERIVED from the payout table the
// way ev-page.jsx does rather than hardcoded. The previous version printed
// "BE 54.07%", the stale pre-37.5x figure that
// tests/engine_tests/test_payout_table_mirror.py bans in the app tabs for
// exactly this reason: a hit rate between 54.07% and the real 54.66% would read
// as profitable while being EV-negative.
const LP_POWER_6_PAYOUT = 37.5;   // == engine/constants.py POWER_PAYOUTS[6]
const LP_POWER_6_BE_PCT = Math.pow(1 / LP_POWER_6_PAYOUT, 1 / 6) * 100;

// useCoverage() and fmtRefresh() live in components.jsx — the pricing page uses
// them too, and that file loads first (see build.sh FILES order).

function Landing({ onLogin, onStart }) {
  const cov = useCoverage();
  return (
    <main className="lp">
      <Hero onLogin={onLogin} onStart={onStart} cov={cov} />
      <Coverage cov={cov} />
      <HowItWorks cov={cov} />
      <Method />
      <Limits />
      <FinalCTA onStart={onStart} onLogin={onLogin} cov={cov} />
      <Footer cov={cov} />
    </main>
  );
}

// ───────── Hero ─────────
function Hero({ onLogin, onStart, cov }) {
  const sources = cov ? `${cov.books.length} ${cov.books_noun}` : null;
  return (
    <section className="lp-hero">
      <div className="lp-hero-inner">
        <div className="lp-hero-l">
          <h1 className="lp-h1">
            PrizePicks lines, priced against the sportsbooks.
          </h1>
          <p className="lp-sub">
            The books set a price on the same player props PrizePicks offers, and
            that price carries their margin. CoreProp takes it out and shows you
            what {sources || "the market"} actually think each line is worth.
          </p>
          <div className="lp-cta-row">
            <button className="cp-btn cp-btn-primary cp-btn-lg" onClick={onStart}>
              See pricing
            </button>
            <button className="cp-btn cp-btn-ghost cp-btn-lg" onClick={onLogin}>
              Log in
            </button>
          </div>
        </div>
        <div className="lp-hero-r">
          <ExampleCard />
        </div>
      </div>
    </section>
  );
}

// The hero panel: one line, and where the book's margin hides.
//
// Replaces a 3-column table of book / price / implied %, which asked the reader
// to do the comparison in their head. The rebuild shows the actual textbook
// demonstration of vig: stack both sides of each market as one bar, and it runs
// PAST the 100% marker. The overhang is the margin — visible without reading a
// figure, and the reason the posted price isn't the real probability.
function ExampleCard() {
  const p = LP_EXAMPLE;
  const m = LP_EXAMPLE_MATH;
  // The 100% gridline sits at a fixed fraction of the track so the overhang has
  // room to show. Scaled off the widest total, not hardcoded, so a wider market
  // can't overflow its bar.
  const scale = Math.max(...m.rows.map((r) => r.totalPct)) * 1.02;
  const hundredAt = (100 / scale) * 100;
  return (
    <figure className="lp-ex">
      <header className="lp-ex-hd">
        <div className="lp-ex-line">
          <LeaguePill league={p.league} />
          <span className="lp-ex-player">{p.player}</span>
        </div>
        <div className="lp-ex-bet">
          <span className="lp-ex-side">{p.side}</span>
          <span className="lp-ex-num mono">{p.line}</span>
          <span className="lp-ex-prop">{p.prop}</span>
        </div>
      </header>

      <div className="lp-ex-bars">
        <div className="lp-ex-scale">
          <span className="lp-ex-100" style={{ left: `${hundredAt}%` }}>100%</span>
        </div>
        {m.rows.map((r) => (
          <div key={r.book} className="lp-ex-bar">
            <div className="lp-ex-bar-top">
              <span className="lp-ex-book">{r.book}</span>
              <span className="lp-ex-price mono">
                {r.over > 0 ? `+${r.over}` : r.over} / {r.under > 0 ? `+${r.under}` : r.under}
              </span>
            </div>
            <div className="lp-ex-track">
              <span className="lp-ex-over"  style={{ width: `${(r.overPct / scale) * 100}%` }} />
              <span className="lp-ex-under" style={{ width: `${(r.underPct / scale) * 100}%` }} />
              <span className="lp-ex-rule" style={{ left: `${hundredAt}%` }} />
            </div>
            <div className="lp-ex-bar-foot">
              <span className="mono">{r.totalPct.toFixed(1)}%</span> priced
              <span className="lp-ex-excess mono">+{r.vigPct.toFixed(1)}%</span>
            </div>
          </div>
        ))}
      </div>

      <div className="lp-ex-key">
        <span className="lp-ex-key-item"><i className="lp-ex-sw lp-ex-sw-over" /> Over</span>
        <span className="lp-ex-key-item"><i className="lp-ex-sw lp-ex-sw-under" /> Under</span>
        <span className="lp-ex-key-note">Both sides sum past 100%. The excess is the house margin.</span>
      </div>

      <div className="lp-ex-out">
        <div className="lp-ex-out-l">
          <span className="lp-ex-out-k">Margin removed</span>
          <span className="lp-ex-out-v mono">{m.fairPct.toFixed(1)}%</span>
        </div>
        <div className="lp-ex-out-r">
          <span className="lp-ex-out-k">Break-even, 6-leg Power</span>
          <span className="lp-ex-out-be mono">{LP_POWER_6_BE_PCT.toFixed(2)}%</span>
        </div>
      </div>

      <figcaption className="lp-ex-cap">Sample prices</figcaption>
    </figure>
  );
}

// ───────── Coverage ─────────
// Replaces the old four-cell "stats strip", which held three invented totals
// and one league list that included NFL. Every cell here comes from
// /api/public/coverage, and the section renders a quiet placeholder until it
// arrives instead of flashing a number.
function Coverage({ cov }) {
  const refresh = cov ? fmtRefresh(cov.refresh_minutes) : null;
  const cells = [
    { k: "Lines from", v: cov ? cov.prop_source : "—" },
    { k: "Priced against", v: cov ? `${cov.books.length} ${cov.books_noun}` : "—",
      sub: cov ? cov.books.join(" · ") : "" },
    { k: "Leagues", v: cov ? String(cov.leagues.length) : "—",
      sub: cov ? cov.leagues.join(" · ") : "" },
    { k: "Updated every", v: refresh || "—",
      sub: cov ? "every source in one pass" : "" },
  ];
  return (
    <section className="lp-cov" aria-label="Coverage">
      <dl className="lp-cov-grid">
        {cells.map((c) => (
          <div key={c.k} className="lp-cov-cell">
            <dt className="lp-cov-k">{c.k}</dt>
            <dd className="lp-cov-v">{c.v}</dd>
            {c.sub && <dd className="lp-cov-sub">{c.sub}</dd>}
          </div>
        ))}
      </dl>
    </section>
  );
}

// ───────── How it works ─────────
function HowItWorks({ cov }) {
  const refresh = cov ? fmtRefresh(cov.refresh_minutes) : null;
  const steps = [
    {
      n: 1,
      t: "Match the market",
      b: `Every PrizePicks line is paired with the same market at each book we cover, including half-step equivalents. Refreshed ${
        refresh ? `every ${refresh}` : "on a fixed cycle"
      }.`,
    },
    {
      n: 2,
      t: "Remove the margin",
      b: "Both sides of each book's price are devigged using Shin's method, then averaged across books into one fair probability per line.",
    },
    {
      n: 3,
      t: "Rank against break-even",
      b: `Legs are sorted by how far that probability clears the break-even for the slip you're building: ${LP_POWER_6_BE_PCT.toFixed(
        2,
      )}% per leg on a 6-leg Power.`,
    },
  ];
  return (
    <section className="lp-how" id="how">
      <div className="lp-section-hd">
        <h2 className="lp-h2">How it works</h2>
      </div>
      <ol className="lp-steps">
        {steps.map((s) => (
          <li key={s.n} className="lp-step">
            <span className="lp-step-n mono">{s.n}</span>
            <h3 className="lp-step-t">{s.t}</h3>
            <p className="lp-step-b">{s.b}</p>
          </li>
        ))}
      </ol>
    </section>
  );
}

// ───────── What's in the product ─────────
// Only tabs and behaviours that exist in the shipped app. The old version
// advertised a "player-specific model trained on five seasons of play-by-play
// data" (no such model exists — the engine devigs market prices), "line
// movement + alerts" and "get pinged the moment a line moves" (there is no
// alerting subsystem anywhere in the codebase), and "one click to the book"
// (the app links to PrizePicks, not to sportsbooks).
function Method() {
  const items = [
    {
      t: "The board",
      b: "Every matched line, ranked by fair probability, with each book's price on the row so you can see where they disagree.",
    },
    {
      t: "Slip builder",
      b: "Power and Flex priced off the published PrizePicks payout tables, with per-leg edge and a flag when two legs come from the same game.",
    },
    {
      t: "Backtest",
      b: "Slips you log settle against final box scores. Hit rate and ROI by league and prop type, on your bets rather than ours.",
    },
    {
      t: "Closing-line value",
      b: "Each logged leg's entry price is compared to where the market closed, so you can tell a good bet from a lucky one.",
    },
  ];
  return (
    <section className="lp-method" id="product">
      <div className="lp-section-hd">
        <h2 className="lp-h2">In the app</h2>
      </div>
      <div className="lp-method-grid">
        {items.map((it) => (
          <article key={it.t} className="lp-method-card">
            <h3 className="lp-method-t">{it.t}</h3>
            <p className="lp-method-b">{it.b}</p>
          </article>
        ))}
      </div>
    </section>
  );
}

// ───────── Limits ─────────
// A tool that sells probability estimates and publishes no track record should
// say so where a buyer will read it. Two bullets, not three: the original also
// explained at length that we'd rather show nothing than a number we can't
// stand behind, which is the kind of line that undercuts itself. State the
// limit and move on.
function Limits() {
  return (
    <section className="lp-limits" aria-labelledby="limits-h">
      <div className="lp-limits-inner">
        <h2 className="lp-h2" id="limits-h">Worth knowing</h2>
        <ul className="lp-limits-list">
          <li>
            <b>This reads the market, it doesn't predict games.</b> There are no
            player projections behind these numbers, so CoreProp is only ever as
            sharp as the books it prices against.
          </li>
          <li>
            <b>We don't publish a hit rate.</b> Not enough logged bets have
            settled to quote one. The Backtest and Analytics tabs measure your
            results instead, including when there's no edge to find.
          </li>
        </ul>
      </div>
    </section>
  );
}

// ───────── Final CTA ─────────
function FinalCTA({ onStart, onLogin, cov }) {
  const days = cov?.trial_days;
  return (
    <section className="lp-cta">
      <div className="lp-cta-card">
        <h2 className="lp-h2">Start with the board</h2>
        <p className="lp-h2-sub">
          {days
            ? `One plan, ${days} days free, cancel any time.`
            : "One plan, cancel any time."}
        </p>
        <div className="lp-cta-row lp-center">
          <button className="cp-btn cp-btn-primary cp-btn-lg" onClick={onStart}>
            See pricing
          </button>
          <button className="cp-btn cp-btn-ghost cp-btn-lg" onClick={onLogin}>
            Log in
          </button>
        </div>
      </div>
    </section>
  );
}

// ───────── Footer ─────────
// The old footer rendered ten <a> elements with no href — unclickable text that
// looked like navigation, and a keyboard trap of ten focus stops that did
// nothing. Sections that exist are real in-page anchors; the rest are gone
// until there is something to link to.
function Footer({ cov }) {
  const year = new Date().getFullYear();
  return (
    <footer className="lp-foot">
      <div className="lp-foot-top">
        <div className="lp-foot-brand">
          <Logo size={22} animated={false} />
          <span className="lp-foot-tag">Prop pricing, without the vig.</span>
        </div>
        <nav className="lp-foot-nav" aria-label="Page sections">
          <a href="#how">How it works</a>
          <a href="#product">In the app</a>
          <a href="#limits-h">Worth knowing</a>
        </nav>
      </div>
      <div className="lp-foot-base">
        <span>© {year} CoreProp</span>
        {cov && (
          <span className="lp-foot-cov mono">
            {cov.prop_source} vs {cov.books.join(", ")} · {cov.leagues.join(" ")}
          </span>
        )}
      </div>
      {/* One legal line, not three. The old version also opened with "CoreProp
        * is an analytics tool and does not accept wagers", which the pricing
        * page's FAQ and its own footer each repeated. */}
      <p className="lp-foot-disc">
        CoreProp is an analytics tool and does not accept wagers. 21+ where
        applicable. If you or someone you know has a gambling problem, call
        1-800-GAMBLER.
      </p>
    </footer>
  );
}

Object.assign(window, { Landing });
