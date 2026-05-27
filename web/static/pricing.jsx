// Pricing / checkout-style sell page. Single tier: $50/mo.
const { useState: useStateP } = React;

function PricingPage({ onStart, onBack }) {
  const [billing, setBilling] = useState("monthly"); // monthly | yearly
  const monthly = 50;
  const yearly = Math.round(monthly * 12 * 0.85); // 15% off
  const yearlyPerMonth = (yearly / 12).toFixed(2);
  const dailyMonthly = (monthly / 30).toFixed(2);
  const dailyYearly = (yearly / 365).toFixed(2);

  const benefits = [
    { icon: "edge",   t: "Positive EV engine",            d: "Live True % on every prop, every book. Sorted by edge so the best plays float to the top." },
    { icon: "books",  t: "FanDuel, DraftKings, Pinnacle", d: "Real time multi-book comparison. Auto flag the soft line and click straight through to bet." },
    { icon: "slip",   t: "Slip Builder + Power Plays",    d: "PrizePicks style slips with break even math, leg-by-leg edge, and auto correlation flags." },
    { icon: "back",   t: "Full backtester",               d: "Replay the model on any window. ROI, hit rate, and CLV broken out by sport and prop type." },
    { icon: "alert",  t: "Line movement + alerts",        d: "Track every steam move. Get pinged the moment a line moves into +EV territory." },
    { icon: "scan",   t: "Sportsbook screen",             d: "Scan every league, market, and book in one view. Filter by league, prop type, and min True %." },
    { icon: "league", t: "All major leagues",             d: "NBA, NFL, MLB, NHL, WNBA, NCAA, soccer, tennis, and more. New markets added weekly." },
    { icon: "refresh",t: "Auto refresh + LOGGED tags",    d: "Lines update every 30 seconds. Tag bets you have placed and we track CLV automatically." },
  ];

  return (
    <main className="pp">
      <div className="pp-bg-orb pp-bg-orb-1" />
      <div className="pp-bg-orb pp-bg-orb-2" />

      <div className="pp-back">
        <button className="cp-link" onClick={onBack}>← Back to home</button>
      </div>

      <header className="pp-hd">
        <h1 className="pp-h1">
          Fast track your profits. <br />
          <span className="pp-h1-g">Get 7 days on us.</span>
        </h1>
        <p className="pp-h1-sub">One plan. Full access. Cancel anytime.</p>
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

          <div className="pp-books-strip">
            {["FD","DK","PIN"].map(b => (
              <span key={b} className={"pp-book-logo pp-book-" + b}>{b}</span>
            ))}
            <span className="pp-books-plus">+ all major leagues</span>
          </div>

          <button className="cp-btn pp-cta" onClick={onStart}>
            Try 7 days free
          </button>
          <div className="pp-cta-sub">Then ${monthly}/mo. Cancel anytime in one click.</div>

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

      <section className="pp-foot-trust">
        <div className="pp-ft-item">
          <b>4,200+ sharps</b>
          <em>building edge daily</em>
        </div>
        <div className="pp-ft-item">
          <b>14,823</b>
          <em>+EV bets surfaced this month</em>
        </div>
        <div className="pp-ft-item">
          <b>58.4%</b>
          <em>backtested hit rate</em>
        </div>
        <div className="pp-ft-item">
          <b>30 sec</b>
          <em>line refresh</em>
        </div>
      </section>

      <section className="pp-faq">
        <h3 className="pp-faq-h">Common questions</h3>
        <div className="pp-faq-grid">
          <details>
            <summary>What happens after 7 days?</summary>
            <p>You'll be billed ${monthly}/mo. Cancel any time before day 7 and you pay nothing.</p>
          </details>
          <details>
            <summary>Which sportsbooks do you cover?</summary>
            <p>FanDuel, DraftKings, and Pinnacle, plus PrizePicks style slip math across all major leagues.</p>
          </details>
          <details>
            <summary>Can I cancel anytime?</summary>
            <p>Yes. One click in your account, no questions, no retention calls.</p>
          </details>
          <details>
            <summary>How often do lines refresh?</summary>
            <p>Every 30 seconds across all books, so you're always seeing the live True %.</p>
          </details>
          <details>
            <summary>Do you support PrizePicks style slips?</summary>
            <p>Yes. The Slip Builder includes Power and Flex math with auto break even and correlation flags.</p>
          </details>
          <details>
            <summary>Is this legal where I live?</summary>
            <p>CoreProp is an analytics tool. Sportsbook availability depends on your state. We'll only show books you can actually use.</p>
          </details>
        </div>
      </section>

      <div className="pp-respo">18+. If you or someone you know has a gambling problem, call 1-800-GAMBLER.</div>
    </main>
  );
}

Object.assign(window, { PricingPage });
