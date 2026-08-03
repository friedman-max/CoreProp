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

// Placeholder line used by the worked example in the "How the number is built"
// panel. Explicitly labelled as an example in the UI — it is not a live quote.
const LP_EXAMPLE = {
  player: "Cale Makar",
  league: "NHL",
  prop: "Assists",
  line: 0.5,
  side: "OVER",
  // Two book prices for one side of one market, and the no-vig probability the
  // engine derives from them. Chosen to be arithmetically checkable by a reader:
  // see LP_EXAMPLE_MATH below, which recomputes every figure from these odds.
  books: [["FD", -138], ["DK", -145]],
};

// The example's arithmetic, computed rather than asserted, so the panel can
// never drift from the numbers it prints. Mirrors engine/devig.py:
// american_to_implied + a two-book average of the implied probabilities.
const LP_EXAMPLE_MATH = (() => {
  const implied = (american) =>
    american < 0 ? -american / (-american + 100) : 100 / (american + 100);
  const probs = LP_EXAMPLE.books.map(([, odds]) => implied(odds));
  const avg = probs.reduce((a, b) => a + b, 0) / probs.length;
  return {
    perBook: LP_EXAMPLE.books.map(([bk, odds], i) => ({
      book: bk,
      odds,
      pct: probs[i] * 100,
    })),
    avgPct: avg * 100,
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
      <FinalCTA onStart={onStart} onLogin={onLogin} />
      <Footer cov={cov} />
    </main>
  );
}

// ───────── Hero ─────────
function Hero({ onLogin, onStart, cov }) {
  const books = cov ? cov.books.join(", ") : null;
  return (
    <section className="lp-hero">
      <div className="lp-hero-inner">
        <div className="lp-hero-l">
          <h1 className="lp-h1">
            Every PrizePicks line, priced against the sportsbooks.
          </h1>
          <p className="lp-sub">
            CoreProp pulls the same player props from{" "}
            {books ? <b>{books}</b> : "the major sportsbooks"}, strips the
            vig out of each price, and shows you the no-vig probability behind
            every PrizePicks line, next to the payout you'd need to break even.
          </p>
          <div className="lp-cta-row">
            <button className="cp-btn cp-btn-primary cp-btn-lg" onClick={onStart}>
              See pricing
            </button>
            <button className="cp-btn cp-btn-ghost cp-btn-lg" onClick={onLogin}>
              Log in
            </button>
          </div>
          <p className="lp-hero-note">
            No performance claims on this page. The math is shown in full below
            so you can check it before you pay for anything.
          </p>
        </div>
        <div className="lp-hero-r">
          <ExampleCard />
        </div>
      </div>
    </section>
  );
}

// The worked example: one line, two book prices, and the arithmetic that turns
// them into a probability. Every figure is computed from LP_EXAMPLE.books.
function ExampleCard() {
  const p = LP_EXAMPLE;
  const m = LP_EXAMPLE_MATH;
  return (
    <figure className="lp-example">
      <figcaption className="lp-example-cap">
        Worked example: illustrative prices, not a live quote
      </figcaption>
      <div className="lp-example-hd">
        <LeaguePill league={p.league} />
        <span className="lp-example-player">{p.player}</span>
      </div>
      <div className="lp-example-bet">
        <span className="lp-example-side">{p.side}</span>
        <span className="lp-example-line mono">{p.line}</span>
        <span className="lp-example-prop">{p.prop}</span>
      </div>

      <table className="lp-example-tbl">
        <thead>
          <tr>
            <th scope="col">Book</th>
            <th scope="col">Price</th>
            <th scope="col">Implied</th>
          </tr>
        </thead>
        <tbody>
          {m.perBook.map((b) => (
            <tr key={b.book}>
              <th scope="row"><span className="cp-book lp-book">{b.book}</span></th>
              <td className="mono">{b.odds > 0 ? `+${b.odds}` : b.odds}</td>
              <td className="mono">{b.pct.toFixed(1)}%</td>
            </tr>
          ))}
        </tbody>
        <tfoot>
          <tr>
            <th scope="row">No-vig consensus</th>
            <td />
            <td className="mono lp-example-out">{m.avgPct.toFixed(1)}%</td>
          </tr>
        </tfoot>
      </table>

      <p className="lp-example-foot">
        A 6-leg Power slip needs every leg above{" "}
        <b className="mono">{LP_POWER_6_BE_PCT.toFixed(2)}%</b> to break even at{" "}
        {LP_POWER_6_PAYOUT}×. That threshold, not a hit-rate claim, is what
        the board ranks against.
      </p>
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
    { k: "Priced against", v: cov ? `${cov.books.length} books` : "—",
      sub: cov ? cov.books.join(" · ") : "" },
    { k: "Leagues", v: cov ? String(cov.leagues.length) : "—",
      sub: cov ? cov.leagues.join(" · ") : "" },
    { k: "Board refresh", v: refresh || "—",
      sub: cov ? "every scrape cycle" : "" },
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
      t: "We scrape both sides",
      b: `Every PrizePicks line, and the same market at each sportsbook we cover. Re-scraped ${
        refresh ? `every ${refresh}` : "on a fixed cycle"
      }.`,
    },
    {
      n: 2,
      t: "We strip the vig",
      b: "Each book's two-sided price is devigged (Shin, 1993) into a fair probability, then averaged across books into one no-vig consensus.",
    },
    {
      n: 3,
      t: "You compare against break-even",
      b: `Each leg's consensus probability sits next to the per-leg break-even for the slip you're building: ${LP_POWER_6_BE_PCT.toFixed(
        2,
      )}% for a 6-leg Power.`,
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
      t: "No-vig consensus, not book odds",
      b: "The number you sort on is the market's fair probability with the house margin removed, averaged across every book that posts the market.",
    },
    {
      t: "Multi-book comparison",
      b: "One row per line, each book's price beside it, so you can see where the books disagree and how wide the market is.",
    },
    {
      t: "Slip builder with real break-even",
      b: "Power and Flex slips priced off the published PrizePicks payout tables, including the correlation between legs from the same game.",
    },
    {
      t: "Backtest and closing-line value",
      b: "Slips you log are settled against final box scores, and each leg's entry probability is compared to the closing market, so you see hit rate and CLV on your own bets, not ours.",
    },
  ];
  return (
    <section className="lp-method" id="product">
      <div className="lp-section-hd">
        <h2 className="lp-h2">What you get</h2>
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
// New section. A tool that sells probability estimates and shows no track
// record has to say so plainly; burying it makes the rest of the page read as
// sales copy. This is also the honest replacement for the testimonials block.
function Limits() {
  return (
    <section className="lp-limits" aria-labelledby="limits-h">
      <div className="lp-limits-inner">
        <h2 className="lp-h2" id="limits-h">What this isn't</h2>
        <ul className="lp-limits-list">
          <li>
            <b>Not a prediction model.</b> CoreProp reads prices the market has
            already set. It has no player projections of its own, so it can only
            be as sharp as the books it reads.
          </li>
          <li>
            <b>No published track record.</b> We don't advertise a hit rate or
            an ROI, because we haven't settled enough of our own logged bets to
            quote one honestly. The Backtest and Analytics tabs measure{" "}
            <em>your</em> results, and will tell you if the edge isn't there.
          </li>
          <li>
            <b>An edge is not a guarantee.</b> A leg above break-even is a
            positive expectation over many bets, and says nothing about any
            single slip.
          </li>
        </ul>
      </div>
    </section>
  );
}

// ───────── Final CTA ─────────
function FinalCTA({ onStart, onLogin }) {
  return (
    <section className="lp-cta">
      <div className="lp-cta-card">
        <h2 className="lp-h2">See the board</h2>
        <p className="lp-h2-sub">
          One plan, cancel any time. Pricing and trial terms are on the next
          page before you enter a card.
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
          <span className="lp-foot-tag">Sharper props, less guesswork.</span>
        </div>
        <nav className="lp-foot-nav" aria-label="Page sections">
          <a href="#how">How it works</a>
          <a href="#product">What you get</a>
          <a href="#limits-h">What this isn't</a>
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
      <p className="lp-foot-disc">
        CoreProp is an analytics tool and does not accept wagers. 21+ where
        applicable; eligibility depends on your jurisdiction. If you or someone
        you know has a gambling problem, call 1-800-GAMBLER.
      </p>
    </footer>
  );
}

Object.assign(window, { Landing });
