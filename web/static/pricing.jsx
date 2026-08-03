// Pricing / checkout-style sell page. Single tier: $50/mo.
const { useState: useStateP } = React;

function PricingPage({ onStart, onBack, loggedIn, locked }) {
  const cov = useCoverage();                         // shared with landing.jsx
  const [billing, setBilling] = useState("monthly"); // monthly | yearly
  const [cta, setCta] = useState("idle");            // idle | loading | error
  const [ctaErr, setCtaErr] = useState("");
  const [portalBusy, setPortalBusy] = useState(false);
  const monthly = 50;
  const yearly = Math.round(monthly * 12 * 0.85); // 15% off
  const yearlyPerMonth = (yearly / 12).toFixed(2);
  const dailyMonthly = (monthly / 30).toFixed(2);
  const dailyYearly = (yearly / 365).toFixed(2);

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
    { t: "No-vig consensus on every line",  d: "Each book's two-sided price is devigged and averaged into one fair probability, so you sort on the market's real number rather than the posted odds." },
    { t: "Multi-book comparison",           d: "PrizePicks lines beside each sportsbook's price for the same market, so you can see where the books disagree and how wide the market is." },
    { t: "Slip Builder, Power and Flex",    d: "Break-even computed from the published PrizePicks payout tables, per-leg edge, and correlation flags for legs from the same game." },
    { t: "Backtester on your own slips",    d: "Slips you log are settled against final box scores. Hit rate and ROI broken out by league and prop type." },
    { t: "Closing-line value tracking",     d: "Every logged leg's entry probability is compared against the closing market, so you can see whether you beat the line, not just whether you won." },
    { t: "Sportsbook screen",               d: "Every league, market, and book in one table. Filter by league, prop type, and minimum probability." },
    { t: "Calibration analytics",           d: "Brier score, log loss, and calibration error on your resolved legs: the numbers that tell you if the probabilities are honest." },
    { t: "Auto-logging",                    d: "Opt in and qualifying slips are logged for you each refresh cycle, so the backtest fills without manual entry." },
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
        <button className="cp-link" onClick={onBack}>← Back to home</button>
      </div>

      <header className="pp-hd">
        {/* "Fast track your profits" promised a financial outcome the product
          * cannot deliver and we publish no evidence for. */}
        <h1 className="pp-h1">
          One plan, full access. <br />
          <span className="pp-h1-g">First 7 days free.</span>
        </h1>
        <p className="pp-h1-sub">Card required to start the trial. Cancel any time before day 7 and you're not charged.</p>
        <div className="pp-toggle">
          <button
            className={"pp-tg-btn " + (billing === "monthly" ? "is-on" : "")}
            onClick={() => setBilling("monthly")}
          >Monthly</button>
          <button
            className={"pp-tg-btn " + (billing === "yearly" ? "is-on" : "")}
            onClick={() => setBilling("yearly")}
          >
            Yearly <span className="pp-save">Save 15%</span>
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
            <div className="pp-card-7day">7 day free trial</div>
          </header>

          <div className="pp-price">
            {billing === "monthly" ? (
              <>
                <div className="pp-price-row">
                  <span className="pp-price-cur">$</span>
                  <span className="pp-price-d">{dailyMonthly.split(".")[0]}</span>
                  <span className="pp-price-c">.{dailyMonthly.split(".")[1]}</span>
                </div>
                <div className="pp-price-sub">per day, billed monthly at <b>${monthly}</b></div>
              </>
            ) : (
              <>
                <div className="pp-price-row">
                  <span className="pp-price-cur">$</span>
                  <span className="pp-price-d">{dailyYearly.split(".")[0]}</span>
                  <span className="pp-price-c">.{dailyYearly.split(".")[1]}</span>
                  <span className="pp-price-strike">${(monthly / 30).toFixed(2)}</span>
                </div>
                <div className="pp-price-sub">
                  per day, billed yearly at <b>${yearly}</b>
                  <span className="pp-price-eff">{`(${yearlyPerMonth}/mo equivalent)`}</span>
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
            {cov && <span className="pp-books-plus">{cov.leagues.join(" · ")}</span>}
          </div>

          <button className="cp-btn pp-cta" onClick={onCta} disabled={cta === "loading"}>
            {cta === "loading" ? "Redirecting to checkout…" : "Try 7 days free"}
          </button>
          {ctaErr && <div className="pp-cta-sub" style={{color:"#FCA5A5"}}>{ctaErr}</div>}
          <div className="pp-cta-sub">
            Then ${billing === "monthly" ? monthly + "/mo" : yearly + "/yr"}. Cancel anytime in one click.
          </div>
          {loggedIn && (
            <button
              type="button"
              className="cp-link cp-center"
              style={{ marginTop: 4 }}
              disabled={portalBusy}
              onClick={async () => {
                if (portalBusy) return;
                setPortalBusy(true);
                try { await window.cpApi.openBillingPortal(); }
                catch (e) { setCtaErr(e.message || "Couldn't open billing portal."); setPortalBusy(false); }
              }}
            >{portalBusy ? "Opening…" : "Manage / cancel subscription"}</button>
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
            <b>{cov.books.length} books</b>
            <em>{cov.books.join(" · ")}</em>
          </div>
          <div className="pp-ft-item">
            <b>{cov.leagues.length} leagues</b>
            <em>{cov.leagues.join(" · ")}</em>
          </div>
          <div className="pp-ft-item">
            <b>{cov.refresh_minutes} min</b>
            <em>board refresh</em>
          </div>
        </section>
      )}

      <section className="pp-faq">
        <h3 className="pp-faq-h">Common questions</h3>
        <div className="pp-faq-grid">
          <details>
            <summary>What happens after 7 days?</summary>
            <p>You'll be billed ${monthly}/mo. Cancel any time before day 7 and you pay nothing.</p>
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
            <summary>Can I cancel anytime?</summary>
            <p>Yes, through the Stripe billing portal linked from this page. Cancelling stops future charges; the current period isn't prorated.</p>
          </details>
          <details>
            <summary>How often do lines refresh?</summary>
            <p>
              {cov
                ? `Every ${cov.refresh_minutes} minutes. One scrape cycle pulls every book, so all prices on the board share a timestamp.`
                : "On a fixed cycle. One scrape pulls every book, so all prices on the board share a timestamp."}
            </p>
          </details>
          <details>
            <summary>Do you publish a hit rate or ROI?</summary>
            <p>No. We haven't settled enough logged bets to quote one honestly, and we'd rather show you nothing than a number we can't stand behind. The Backtest and Analytics tabs measure your own results, including when the edge isn't there.</p>
          </details>
          <details>
            <summary>Do you support PrizePicks style slips?</summary>
            <p>Yes. The Slip Builder includes Power and Flex math with auto break even and correlation flags.</p>
          </details>
          <details>
            <summary>Is this legal where I live?</summary>
            {/* The old answer promised "we'll only show books you can actually
              * use" — there is no geolocation or state-eligibility filtering
              * anywhere in the codebase, so the board shows every scraped book
              * regardless of where the visitor is. */}
            <p>CoreProp is an analytics tool and doesn't accept wagers. We don't filter the board by your location, so whether you can act on a line depends on which books and contests are available in your jurisdiction.</p>
          </details>
        </div>
      </section>

      <div className="pp-respo">CoreProp is an analytics tool and does not accept wagers. 21+ where applicable; eligibility depends on your jurisdiction. If you or someone you know has a gambling problem, call 1-800-GAMBLER.</div>
    </main>
  );
}

Object.assign(window, { PricingPage });
