// Pricing / checkout-style sell page. Single tier: $50/mo.
const { useState: useStateP } = React;

// cpApi surfaces a failed fetch as `HTTP <code>: <body>` (see apiFetch), so a
// billing error would otherwise render to a paying user as raw red JSON like
// `HTTP 400: {"detail":"No billing account yet."}`. Pull out the server's
// `detail` sentence when present, otherwise strip the `HTTP nnn:` prefix.
function friendlyBillingError(msg) {
  if (!msg) return "Something went wrong. Please try again.";
  const m = /HTTP\s+\d+:\s*(\{[\s\S]*\})/.exec(msg);
  if (m) {
    try {
      const d = JSON.parse(m[1]).detail;
      if (d) return typeof d === "string" ? d : "Something went wrong. Please try again.";
    } catch (_) { /* not JSON — fall through */ }
  }
  return msg.replace(/^HTTP\s+\d+:\s*/, "") || "Something went wrong. Please try again.";
}

function PricingPage({ onStart, onBack, loggedIn, locked, comped }) {
  const cov = useCoverage();                         // shared with landing.jsx
  const [billing, setBilling] = useState("monthly"); // monthly | yearly
  const [cta, setCta] = useState("idle");            // idle | loading | error
  const [ctaErr, setCtaErr] = useState("");
  const [portalBusy, setPortalBusy] = useState(false);
  const monthly = 50;
  // Flat $500/yr (matches the Stripe price actually wired up in
  // STRIPE_PRICE_YEARLY) — not derived from a 15%-off formula, so it can
  // never drift out of sync with what Stripe actually charges.
  const yearly = 500;
  const yearlyPerMonth = (yearly / 12).toFixed(2);
  // Derived from the two prices above (600 - 500 = 16.67%), not a literal —
  // a hardcoded "Save 15%" would have quietly gone stale the moment the
  // yearly price stopped being monthly*12*0.85.
  const yearlySavePct = Math.round((1 - yearly / (monthly * 12)) * 100);
  // BILLING_TRIAL_DAYS, served by /api/public/coverage. Falls back to 7 (the
  // env default) for the pre-fetch render so the copy never reads "First
  // undefined days free".
  const trialDays = cov?.trial_days ?? 7;

  // Only tabs and behaviours that exist in the shipped app. Removed:
  //   - "Line movement + alerts / get pinged the moment a line moves" — there
  //     is no alerting subsystem in the codebase at all.
  //   - "click straight through to bet" — the app deep-links to PrizePicks,
  //     not to sportsbooks.
  //   - "NFL, NCAA, and more. New markets added weekly." — config.ACTIVE_LEAGUES
  //     is NBA/WNBA/MLB/NHL/NCAAB, and nothing adds markets on a weekly cadence.
  //   - "Lines update every 30 seconds" — the scheduler runs on
  //     _state["interval_min"], 5 minutes by default. The real cadence is read
  //     from /api/public/coverage below rather than asserted here.
  const benefits = [
    { t: "Fair probability on every line",  d: "Both sides of each book's price devigged and averaged, so you sort on what the market really thinks instead of the posted odds." },
    { t: "Every book on the row",           d: "PrizePicks lines beside each sportsbook's price for the same market. Where they disagree is where to look." },
    { t: "Power and Flex slip builder",     d: "Break-even from the published payout tables, per-leg edge, and a flag when two legs share a game." },
    { t: "Backtest",                        d: "Slips you log settle against final box scores, with hit rate and ROI by league and prop type." },
    { t: "Closing-line value",              d: "Each leg's entry price against where the market closed, so you can tell a good bet from a lucky one." },
    { t: "Sportsbook screen",               d: "Every league, market, and book in one table. Filter by league, prop type, and minimum probability." },
    { t: "Calibration analytics",           d: "Brier score, log loss, and calibration error on your resolved legs, so you can check whether the probabilities hold up." },
    { t: "Auto-logging",                    d: "Qualifying slips log themselves each cycle, so the backtest fills without manual entry." },
  ];

  // CTA click: if signed in, start Stripe Checkout for the selected plan.
  // If signed out, hand back to the app's auth flow (onStart) so they log in
  // first — after auth they land here and can subscribe.
  const onCta = async () => {
    if (cta === "loading") return;
    if (!window.cpApi || !window.cpApi.isLoggedIn()) {
      onStart && onStart();
      return;
    }
    setCta("loading"); setCtaErr("");
    try {
      await window.cpApi.startCheckout(billing); // redirects to Stripe
    } catch (e) {
      // Billing not configured yet (503) or a Stripe error — fall back to the
      // original behavior so the button still does something useful.
      console.warn("checkout failed:", e.message);
      if (/not configured|503/i.test(e.message || "")) {
        // Fall back to onStart, but reset the button — otherwise it stays frozen
        // on "Redirecting to checkout…" forever (disabled={cta === "loading"}).
        setCta("idle");
        onStart && onStart();
      } else {
        setCtaErr(e.message || "Couldn't start checkout.");
        setCta("error");
      }
    }
  };

  return (
    <main className="pp">
      <div className="pp-back">
        <button className="cp-btn cp-btn-ghost cp-btn-sm" onClick={onBack}>← Back to home</button>
      </div>

      <header className="pp-hd">
        {/* "Fast track your profits" promised a financial outcome the product
          * cannot deliver and we publish no evidence for. Trial length comes
          * from the server (BILLING_TRIAL_DAYS) — it used to be hardcoded as
          * "7" in six places on this page. */}
        <h1 className="pp-h1">
          One plan, full access. <br />
          <span className="pp-h1-g">First {trialDays} days free.</span>
        </h1>
        <p className="pp-h1-sub">Cancel before day {trialDays} and you're not charged.</p>
        <div className="pp-toggle">
          <button
            className={"pp-tg-btn " + (billing === "monthly" ? "is-on" : "")}
            onClick={() => setBilling("monthly")}
          >Monthly</button>
          <button
            className={"pp-tg-btn " + (billing === "yearly" ? "is-on" : "")}
            onClick={() => setBilling("yearly")}
          >
            Yearly <span className="pp-save">Save {yearlySavePct}%</span>
          </button>
        </div>
      </header>

      <section className="pp-card-wrap">
        <article className="pp-card">
          <div className="pp-card-glow" />
          <header className="pp-card-hd">
            <div className="pp-card-tag">
              <span className="pp-card-dot" />
              CoreProp All Access
            </div>
            <div className="pp-card-trial">{trialDays} day free trial</div>
          </header>

          {/* Lead with the amount that gets charged.
            *
            * This block used to render the price divided by 30 — a $50/mo plan
            * shown as a giant "$1.67", with the real figure in 12px grey
            * underneath, and the annual plan as "$1.42" with a struck-through
            * "$1.67" beside it. Nobody is billed $1.67 and no comparison shopper
            * is helped by it; framing a subscription as a daily fraction is a
            * minimization tactic, and it's the sort of thing that makes a real
            * tool look like a funnel. The per-month equivalent on the annual
            * plan is a genuine comparison and stays. */}
          <div className="pp-price">
            {billing === "monthly" ? (
              <>
                <div className="pp-price-row">
                  <span className="pp-price-cur">$</span>
                  <span className="pp-price-d">{monthly}</span>
                  <span className="pp-price-per">/mo</span>
                </div>
                <div className="pp-price-sub">Billed monthly.</div>
              </>
            ) : (
              <>
                <div className="pp-price-row">
                  <span className="pp-price-cur">$</span>
                  <span className="pp-price-d">{yearly}</span>
                  <span className="pp-price-per">/yr</span>
                </div>
                <div className="pp-price-sub">
                  Billed annually. Works out to <b>${yearlyPerMonth}/mo</b>, versus ${monthly} month to month.
                </div>
              </>
            )}
          </div>

          {/* Books and leagues from the server, not a hardcoded trio plus
            * "+ all major leagues" (which was never true — see the league list
            * in config.ACTIVE_LEAGUES). */}
          <div className="pp-books-strip">
            {(cov ? cov.books : ["FanDuel", "DraftKings", "Pinnacle"]).map(b => (
              <span key={b} className="pp-book-logo">{b}</span>
            ))}
            {/* Labelled. Without the "across", the league list rendered as a
              * bare fragment hanging off the book chips. */}
            {cov && <span className="pp-books-plus">across {cov.leagues.join(", ")}</span>}
          </div>

          <button className="cp-btn pp-cta" onClick={onCta} disabled={cta === "loading"}>
            {cta === "loading" ? "Redirecting to checkout…" : `Start ${trialDays} days free`}
          </button>
          {ctaErr && <div className="pp-cta-sub is-err" role="alert">{friendlyBillingError(ctaErr)}</div>}
          {/* The word "cancel" appeared six times on this page. It's in the
            * header and the FAQ; this line just needs to state the price. */}
          <div className="pp-cta-sub">
            Then ${billing === "monthly" ? monthly + "/mo" : yearly + "/yr"}.
          </div>
          {loggedIn && !comped && (
            <button
              type="button"
              className="cp-link cp-center pp-portal-btn"
              disabled={portalBusy}
              onClick={async () => {
                if (portalBusy) return;
                setPortalBusy(true);
                try { await window.cpApi.openBillingPortal(); }
                catch (e) { setCtaErr(e.message || "Couldn't open billing portal."); setPortalBusy(false); }
              }}
            >{portalBusy ? "Opening…" : "Manage subscription"}</button>
          )}

          <ul className="pp-benefits">
            {benefits.map((b, i) => (
              <li key={i} className="pp-benefit">
                <span className="pp-check">
                  <svg width="13" height="13" viewBox="0 0 13 13" fill="none">
                    <path d="M2.5 6.5l2.5 2.5 5.5-5.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
                </span>
                <div className="pp-b-body">
                  <div className="pp-b-t">{b.t}</div>
                  <div className="pp-b-d">{b.d}</div>
                </div>
              </li>
            ))}
          </ul>

          <footer className="pp-card-foot">
            <div className="pp-trust">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none"><path d="M6 10V8a6 6 0 1 1 12 0v2" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/><rect x="4" y="10" width="16" height="11" rx="2" stroke="currentColor" strokeWidth="2"/></svg>
              <span>Secure checkout</span>
            </div>
            <div className="pp-pay">
              <span className="pp-pay-chip">VISA</span>
              <span className="pp-pay-chip">MC</span>
              <span className="pp-pay-chip">AMEX</span>
              <span className="pp-pay-chip pp-pay-apple">Pay</span>
            </div>
          </footer>
        </article>
      </section>

      {/* Coverage facts read from the running server (/api/public/coverage).
        * This strip previously carried four invented figures — "4,200+ sharps",
        * "14,823 +EV bets surfaced this month", "58.4% backtested hit rate" and
        * "30 sec line refresh" — none of which came from anywhere, and the last
        * of which contradicted the actual scheduler interval. Renders nothing
        * until the fetch resolves rather than showing placeholders. */}
      {cov && (
        <section className="pp-foot-trust">
          <div className="pp-ft-item">
            <b>{cov.prop_source}</b>
            <em>line source</em>
          </div>
          <div className="pp-ft-item">
            <b>{cov.books.length} {cov.books_noun}</b>
            <em>{cov.books.join(" · ")}</em>
          </div>
          <div className="pp-ft-item">
            <b>{cov.leagues.length} leagues</b>
            <em>{cov.leagues.join(" · ")}</em>
          </div>
          <div className="pp-ft-item">
            <b>{cov.refresh_minutes} min</b>
            <em>between updates</em>
          </div>
        </section>
      )}

      <section className="pp-faq">
        <h3 className="pp-faq-h">Common questions</h3>
        <div className="pp-faq-grid">
          <details>
            <summary>What happens when the trial ends?</summary>
            <p>You're billed ${monthly}/mo, or ${yearly}/yr on the annual plan. Cancel before day {trialDays} and you pay nothing.</p>
          </details>
          <details>
            <summary>Which sportsbooks do you cover?</summary>
            <p>
              {cov
                ? `${cov.books.join(", ")}. Lines come from ${cov.prop_source}, across ${cov.leagues.join(", ")}.`
                : "FanDuel, DraftKings, and Pinnacle. Lines come from PrizePicks."}
            </p>
          </details>
          <details>
            <summary>How do I cancel?</summary>
            <p>Through the Stripe billing portal, linked from this page once you're signed in. Cancelling stops future charges; the current period isn't prorated.</p>
          </details>
          <details>
            <summary>How often do lines refresh?</summary>
            <p>
              {cov
                ? `Every ${cov.refresh_minutes} minutes. One cycle pulls every book, so all prices on the board share a timestamp.`
                : "On a fixed cycle. One pass pulls every book, so all prices on the board share a timestamp."}
            </p>
          </details>
          <details>
            <summary>Do you publish a hit rate?</summary>
            <p>No. Not enough logged bets have settled to quote one. The Backtest and Analytics tabs measure your own results, including when there's no edge to find.</p>
          </details>
          <details>
            <summary>Does the slip builder handle Power and Flex?</summary>
            <p>Both, priced off the published PrizePicks payout tables, with break-even per leg and a flag when two legs share a game.</p>
          </details>
          <details>
            <summary>Is this legal where I live?</summary>
            {/* The old answer promised "we'll only show books you can actually
              * use" — there is no geolocation or state-eligibility filtering
              * anywhere in the codebase, so the board shows every scraped book
              * regardless of where the visitor is. */}
            <p>CoreProp is an analytics tool and doesn't accept wagers. The board isn't filtered by location, so whether you can act on a line depends on what's available where you are.</p>
          </details>
        </div>
      </section>

      <div className="pp-respo">21+ where applicable. If you or someone you know has a gambling problem, call 1-800-GAMBLER.</div>
    </main>
  );
}

Object.assign(window, { PricingPage });
