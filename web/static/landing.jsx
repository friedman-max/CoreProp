// Landing page — hero, features, live demo, social proof, CTA
const { useState: useStateL, useEffect: useEffectL, useRef: useRefL, useMemo: useMemoL } = React;

function Landing({ onLogin, onStart, onTab }) {
  return (
    <main className="lp">
      <Hero onLogin={onLogin} onStart={onStart} />
      <Ticker />
      <Stats />
      <YesterdaysWinners onStart={onStart} />
      <Features />
      <HowItWorks />
      <DemoPreview onStart={onStart} />
      <Testimonials />
      <FinalCTA onStart={onStart} />
      <Footer />
    </main>
  );
}

// ───────── Hero ─────────
function Hero({ onLogin, onStart }) {
  return (
    <section className="lp-hero">
      <div className="lp-hero-bg">
        <div className="lp-orb lp-orb-1" />
        <div className="lp-orb lp-orb-2" />
        <div className="lp-grid-bg" />
      </div>
      <div className="lp-hero-inner">
        <div className="lp-hero-l">
          <span className="lp-eyebrow">
            <span className="lp-pulse" /> Live edge engine · {new Intl.NumberFormat().format(2847)} bets scanned today
          </span>
          <h1 className="lp-h1">
            Find the <em className="lp-grad">+EV bets</em> sportsbooks <br />
            don't want you to see.
          </h1>
          <p className="lp-sub">
            CoreProp pulls every prop line from FanDuel, DraftKings, and Pinnacle, models the true probability,
            and surfaces the spots where the math is on your side.
          </p>
          <div className="lp-cta-row">
            <button className="cp-btn cp-btn-primary cp-btn-lg" onClick={onStart}>
              Start now!
            </button>
            <button className="cp-btn cp-btn-ghost cp-btn-lg" onClick={onLogin}>
              Log In
            </button>
          </div>
          <div className="lp-trust">
            <div className="lp-trust-avs">
              {["#6366F1","#22C55E","#F97316","#EF4444"].map((c,i) => <span key={i} className="lp-av" style={{background: c}} />)}
            </div>
            <span><b>4,200+</b> sharps building edge with CoreProp</span>
          </div>
        </div>
        <div className="lp-hero-r">
          <HeroCard />
        </div>
      </div>
    </section>
  );
}

function HeroCard() {
  const [idx, setIdx] = useState(0);
  const plays = TICKER_PLAYS;
  useEffect(() => {
    const id = setInterval(() => setIdx(i => (i + 1) % plays.length), 2200);
    return () => clearInterval(id);
  }, []);
  const p = plays[idx];
  return (
    <div className="lp-card-stack">
      <div className="lp-card lp-card-glow">
        <div className="lp-card-hd">
          <span className="lp-card-dot" /> +EV ALERT
          <span className="lp-card-time">just now</span>
        </div>
        <div className="lp-card-body">
          <div className="lp-card-player">{p.player}</div>
          <div className="lp-card-prop">{p.prop}</div>
          <div className="lp-card-meter">
            <div className="lp-meter-track">
              <div className="lp-meter-fill" style={{ width: `${p.pct}%` }} />
            </div>
            <div className="lp-meter-labels">
              <span>True %</span>
              <span className="lp-meter-val"><AnimatedNumber value={p.pct} decimals={1} suffix="%" /></span>
            </div>
          </div>
          <div className="lp-card-books">
            {p.books.map(([bk, od]) => <BookBadge key={bk} book={bk} odds={od} />)}
          </div>
        </div>
      </div>
      <div className="lp-mini-card lp-mini-1">
        <div className="lp-mini-row"><span className="lp-mini-dot is-up" /> Backtest hit rate</div>
        <div className="lp-mini-big">58.4%</div>
        <div className="lp-mini-sub">last 90 days · 1,284 bets</div>
      </div>
      <div className="lp-mini-card lp-mini-2">
        <div className="lp-mini-row"><span className="lp-mini-dot is-up" /> ROI</div>
        <div className="lp-mini-big">+11.7%</div>
        <div className="lp-mini-sub">on -110 priced legs</div>
      </div>
    </div>
  );
}

// ───────── Live ticker strip ─────────
function Ticker() {
  const items = [...TICKER_PLAYS, ...TICKER_PLAYS];
  return (
    <section className="lp-ticker">
      <div className="lp-ticker-label">
        <span className="lp-pulse" /> LIVE EDGES
      </div>
      <div className="lp-ticker-track">
        <div className="lp-ticker-roll">
          {items.map((p, i) => (
            <div key={i} className="lp-tk">
              <span className="lp-tk-name">{p.player}</span>
              <span className="lp-tk-prop">{p.prop}</span>
              <span className="lp-tk-pct">{p.pct.toFixed(1)}%</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

// ───────── Stats strip ─────────
function Stats() {
  const stats = [
    { v: 14823, suf: "+", l: "+EV bets surfaced this month" },
    { v: 58.4,  dec: 1, suf: "%", l: "Backtested hit rate" },
    { v: 3,     suf: " books", l: "FanDuel · DraftKings · Pinnacle" },
    { text: "All", suf: " major leagues", l: "NBA · NFL · MLB · NHL · WNBA · more" },
  ];
  return (
    <section className="lp-stats">
      {stats.map((s, i) => (
        <div key={i} className="lp-stat">
          <div className="lp-stat-v">
            {s.text ? <span>{s.text}{s.suf}</span> : <AnimatedNumber value={s.v} decimals={s.dec || 0} suffix={s.suf} />}
          </div>
          <div className="lp-stat-l">{s.l}</div>
        </div>
      ))}
    </section>
  );
}

// ───────── Yesterday's Winners ─────────
function YesterdaysWinners({ onStart }) {
  const [league, setLeague] = useState("All");
  const [hover, setHover] = useState(null);

  const all = YESTERDAY_WINS;
  const filtered = league === "All" ? all : all.filter(w => w.league === league);

  const leagues = ["All", "NBA", "WNBA", "NHL", "MLB"];
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const dateStr = yesterday.toLocaleDateString(undefined, { weekday: "long", month: "long", day: "numeric" });

  return (
    <section className="lp-yw" id="winners">
      <div className="lp-yw-inner">
        <header className="lp-yw-hd">
          <div className="lp-yw-hd-l">
            <span className="lp-eyebrow"><span className="lp-pulse" /> {dateStr}</span>
            <h2 className="lp-h2">Yesterday's Winners.</h2>
            <p className="lp-h2-sub" style={{margin:0}}>
              <b className="lp-yw-count-inline">{all.length}</b> +EV bets we surfaced that hit.
            </p>
          </div>
        </header>

        <div className="lp-yw-filters">
          {leagues.map(l => (
            <button
              key={l}
              className={"ev-chip " + (league === l ? "is-on" : "")}
              onClick={() => setLeague(l)}
            >
              {l}
              {l !== "All" && (
                <span className="lp-yw-count">
                  {all.filter(w => w.league === l).length}
                </span>
              )}
            </button>
          ))}
        </div>

        <div className="lp-yw-grid">
          {filtered.map((w, i) => (
            <WinnerCard
              key={w.player + w.prop}
              w={w}
              i={i}
              hovered={hover === i}
              onHover={(b) => setHover(b ? i : null)}
            />
          ))}
        </div>

        <div className="lp-yw-foot">
          <span>Want today's edges before they move?</span>
          <button className="cp-btn cp-btn-primary" onClick={onStart}>See today's board →</button>
        </div>
      </div>
    </section>
  );
}

function WinnerCard({ w, i, hovered, onHover }) {
  // Build a number-line visualization showing line vs. result.
  // We center the line in the middle and place the stat relative to it.
  const range = Math.max(Math.abs(w.stat - w.line) * 2, w.line * 0.6, 4); // dynamic scale
  const lineX = 50;
  const statX = Math.max(6, Math.min(94, lineX + ((w.stat - w.line) / range) * 50));
  const isOver = w.side === "OVER";
  const direction = isOver ? "right" : "left"; // which way the win goes from line
  return (
    <article
      className={"yw-card " + (hovered ? "is-hov" : "")}
      onMouseEnter={() => onHover(true)}
      onMouseLeave={() => onHover(false)}
    >
      <div className="yw-c-hd">
        <div className="yw-c-game">
          <LeaguePill league={w.league} />
          <span className="yw-c-matchup">{w.team} <em>vs</em> {w.opp}</span>
        </div>
        <div className="yw-c-win">
          <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M2 6.5l2.5 2.5 5.5-5.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/></svg>
          WIN
        </div>
      </div>

      <div className="yw-c-player">{w.player}</div>
      <div className="yw-c-bet">
        <span className={"yw-c-side yw-c-side-" + (isOver ? "over" : "under")}>{w.side}</span>
        <span className="yw-c-line">{w.line}</span>
        <span className="yw-c-prop">{w.prop}</span>
      </div>

      {/* Numberline viz */}
      <div className="yw-c-viz">
        <div className="yw-c-track">
          <div
            className={"yw-c-fill yw-c-fill-" + direction}
            style={{
              left: direction === "right" ? `${lineX}%` : `${statX}%`,
              width: `${Math.abs(statX - lineX)}%`,
            }}
          />
          <div className="yw-c-mark yw-c-line-mark" style={{ left: `${lineX}%` }}>
            <span className="yw-c-mark-lbl">Line {w.line}</span>
          </div>
          <div className="yw-c-mark yw-c-stat-mark" style={{ left: `${statX}%` }}>
            <span className="yw-c-mark-lbl yw-c-mark-lbl-stat">{w.statLabel}</span>
          </div>
        </div>
      </div>

      <footer className="yw-c-foot">
        <div className="yw-c-foot-l">
          <span className="yw-c-foot-k">True %</span>
          <span className="yw-c-foot-v"><TruePct value={w.truePct} /></span>
        </div>
        <div className="yw-c-foot-l">
          <span className="yw-c-foot-k">Odds</span>
          <span className="yw-c-foot-v mono">{w.odds > 0 ? "+" + w.odds : w.odds}</span>
        </div>
      </footer>
    </article>
  );
}

// ───────── Features ─────────
function Features() {
  const features = [
    {
      tag: "+EV ENGINE",
      title: "True probability, not vig-baked odds.",
      body: "We strip the house edge from every line and compare it against a player-specific model trained on five seasons of play-by-play data.",
      visual: <FeatureVizModel />,
    },
    {
      tag: "MULTI-BOOK",
      title: "FanDuel, DraftKings, Pinnacle in one view.",
      body: "Compare any prop across every major book in real time. Auto-flag the soft line and click straight through.",
      visual: <FeatureVizBooks />,
    },
    {
      tag: "BACKTEST",
      title: "Prove the edge before you ever bet a dollar.",
      body: "Replay the model on any window: last 7 days, last season, custom dates. See ROI, hit rate, and CLV broken out per sport.",
      visual: <FeatureVizBacktest />,
    },
    {
      tag: "SLIP BUILDER",
      title: "Power Plays, sized to the math.",
      body: "Build PrizePicks-style slips with our recommended leg counts. We tell you the break-even, the auto-correlation, and the expected value.",
      visual: <FeatureVizSlip />,
    },
  ];
  return (
    <section className="lp-features" id="features">
      <div className="lp-section-hd">
        <span className="lp-eyebrow lp-eyebrow-c">Why CoreProp</span>
        <h2 className="lp-h2">A sharper kind of betting.</h2>
        <p className="lp-h2-sub">Built by quants who got tired of running spreadsheets at 6 PM Sunday.</p>
      </div>
      <div className="lp-feature-grid">
        {features.map((f, i) => (
          <article key={i} className={"lp-feature lp-feature-" + i}>
            <div className="lp-feature-viz">{f.visual}</div>
            <div className="lp-feature-body">
              <span className="lp-feature-tag">{f.tag}</span>
              <h3 className="lp-feature-title">{f.title}</h3>
              <p className="lp-feature-text">{f.body}</p>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}

// Feature visuals — small bespoke SVG/HTML
function FeatureVizModel() {
  return (
    <div className="fv fv-model">
      <div className="fv-bar fv-bar-book"><span>BOOK</span><b>−130</b><i>56.5%</i></div>
      <div className="fv-arrow">→</div>
      <div className="fv-bar fv-bar-true"><span>TRUE</span><b>62.0%</b><i className="fv-edge">+5.5% EDGE</i></div>
    </div>
  );
}
function FeatureVizBooks() {
  return (
    <div className="fv fv-books">
      <div className="fv-book-row"><span className="cp-book fv-fd">FD</span><div className="fv-line">−138</div></div>
      <div className="fv-book-row fv-best"><span className="cp-book fv-dk">DK</span><div className="fv-line">−122 <em>BEST</em></div></div>
      <div className="fv-book-row"><span className="cp-book fv-pin">PIN</span><div className="fv-line">−135</div></div>
    </div>
  );
}
function FeatureVizBacktest() {
  const pts = [40, 45, 42, 50, 48, 56, 55, 62, 64, 60, 70, 75, 78];
  const w = 240, h = 90;
  const max = Math.max(...pts), min = Math.min(...pts);
  const d = pts.map((p, i) => {
    const x = (i / (pts.length - 1)) * w;
    const y = h - ((p - min) / (max - min)) * h;
    return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return (
    <div className="fv fv-bt">
      <svg viewBox={`0 0 ${w} ${h}`} width="100%" height="90" preserveAspectRatio="none">
        <defs>
          <linearGradient id="bt-fill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="#22C55E" stopOpacity="0.5" />
            <stop offset="100%" stopColor="#22C55E" stopOpacity="0" />
          </linearGradient>
        </defs>
        <path d={d + ` L${w},${h} L0,${h} Z`} fill="url(#bt-fill)" />
        <path d={d} fill="none" stroke="#22C55E" strokeWidth="2" />
      </svg>
      <div className="fv-bt-row"><span>ROI</span><b>+11.7%</b></div>
    </div>
  );
}
function FeatureVizSlip() {
  return (
    <div className="fv fv-slip">
      <div className="fv-slip-hd"><span>POWER · 6 LEGS</span><b>BE 54.07%</b></div>
      {["Makar O0.5","Wemby U3.5","Bueckers U8.5","Holmgren U7.5","Vassell U4.5","Wicks U4.5"].map((s, i) => (
        <div key={i} className="fv-slip-leg">
          <span className="fv-slip-i">{i+1}</span>
          <span className="fv-slip-n">{s}</span>
          <span className="fv-slip-c">✓</span>
        </div>
      ))}
    </div>
  );
}

// ───────── How it works ─────────
function HowItWorks() {
  const steps = [
    { n: "01", t: "Pick your sport", b: "We pull every active prop line. Pitcher walks, NBA rebounds, NHL shots, you name it." },
    { n: "02", t: "Filter by edge",  b: "Set your minimum True % and let CoreProp surface only the bets the model loves." },
    { n: "03", t: "Place + track",   b: "One click to the book. Slip builder, auto-backtest and CLV tracking come for free." },
  ];
  return (
    <section className="lp-how">
      <div className="lp-section-hd">
        <span className="lp-eyebrow lp-eyebrow-c">How it works</span>
        <h2 className="lp-h2">Three steps. Zero spreadsheets.</h2>
      </div>
      <div className="lp-steps">
        {steps.map((s, i) => (
          <div key={i} className="lp-step">
            <div className="lp-step-n">{s.n}</div>
            <h4 className="lp-step-t">{s.t}</h4>
            <p className="lp-step-b">{s.b}</p>
          </div>
        ))}
      </div>
    </section>
  );
}

// ───────── Demo preview ─────────
function DemoPreview({ onStart }) {
  const rows = EV_BETS.slice(0, 10);
  return (
    <section className="lp-demo">
      <div className="lp-section-hd">
        <span className="lp-eyebrow lp-eyebrow-c">A peek inside</span>
        <h2 className="lp-h2">The board, live.</h2>
        <p className="lp-h2-sub">Every line, every book, every edge. Refreshed every 30 seconds.</p>
      </div>
      <div className="lp-demo-frame">
        <div className="lp-demo-chrome">
          <span className="lp-demo-dot" style={{background:"#EF4444"}} />
          <span className="lp-demo-dot" style={{background:"#FBBF24"}} />
          <span className="lp-demo-dot" style={{background:"#22C55E"}} />
          <span className="lp-demo-url">coreprop.com/ev</span>
        </div>
        <div className="lp-demo-body">
          <div className="lp-demo-table">
            <div className="lp-demo-row lp-demo-hd">
              <span>PLAYER</span><span>LEAGUE</span><span>PROP</span><span>LINE</span><span>SIDE</span><span>TRUE %</span><span>BOOK ODDS</span>
            </div>
            {rows.map((b, i) => (
              <div
                key={i}
                className={"lp-demo-row " + (i >= 3 ? "is-locked" : "")}
              >
                <span className="lp-demo-p">{b.player}</span>
                <span><LeaguePill league={b.league} /></span>
                <span className="lp-demo-prop">{b.prop}</span>
                <span className="lp-demo-line">{b.line}</span>
                <span className={"lp-demo-side " + (b.side === "OVER" ? "is-over" : "is-under")}>{b.side}</span>
                <span><TruePct value={b.truePct} /></span>
                <span className="lp-demo-books">
                  {b.books.slice(0, 2).map(([bk, od], j) => <BookBadge key={j} book={bk} odds={od} />)}
                </span>
              </div>
            ))}
            <div className="lp-demo-paywall">
              <div className="lp-pw-inner">
                <div className="lp-pw-badge">
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none"><path d="M6 10V8a6 6 0 1 1 12 0v2" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/><rect x="4" y="10" width="16" height="11" rx="2" stroke="currentColor" strokeWidth="2"/></svg>
                  <span>+1,287 more edges available now</span>
                </div>
                <h3 className="lp-pw-h">Unlock the full board.</h3>
                <p className="lp-pw-sub">Live True % across every book, slip builder, backtester, and line movement history.</p>
                <button className="cp-btn cp-btn-primary cp-btn-lg" onClick={onStart}>
                  Start your 7 day trial →
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

// ───────── Testimonials ─────────
function Testimonials() {
  const quotes = [
    { q: "Replaced four spreadsheets and a Twitter list. The True % column alone pays for itself.", a: "Mike R.", r: "MLB props, 3 yrs" },
    { q: "Best multi-book comparison tool I've used. The backtest is what sold me.", a: "Devon S.", r: "NBA, NHL" },
    { q: "I was skeptical of another model, but the slip builder math is legit.", a: "Priya K.", r: "PrizePicks player" },
  ];
  return (
    <section className="lp-testimonials">
      <div className="lp-section-hd">
        <span className="lp-eyebrow lp-eyebrow-c">From the sharps</span>
        <h2 className="lp-h2">Built for people who run the math.</h2>
      </div>
      <div className="lp-quotes">
        {quotes.map((q, i) => (
          <figure key={i} className="lp-quote">
            <blockquote>"{q.q}"</blockquote>
            <figcaption>
              <span className="lp-q-av" style={{ background: ["#6366F1","#22C55E","#F97316"][i] }}>{q.a[0]}</span>
              <span><b>{q.a}</b><em>{q.r}</em></span>
            </figcaption>
          </figure>
        ))}
      </div>
    </section>
  );
}

// ───────── Final CTA ─────────
function FinalCTA({ onStart }) {
  return (
    <section className="lp-cta">
      <div className="lp-cta-card">
        <div className="lp-cta-bg" />
        <h2 className="lp-h2">Stop guessing. Start finding edge.</h2>
        <p className="lp-h2-sub">Free for the first 7 days. No credit card. Cancel anytime.</p>
        <div className="lp-cta-row lp-center">
          <button className="cp-btn cp-btn-primary cp-btn-lg" onClick={onStart}>Start now!</button>
          <button className="cp-btn cp-btn-ghost cp-btn-lg" onClick={onStart}>See pricing</button>
        </div>
      </div>
    </section>
  );
}

// ───────── Footer ─────────
function Footer() {
  return (
    <footer className="lp-foot">
      <div className="lp-foot-l">
        <Logo size={24} />
        <span className="lp-foot-tag">Sharper props, less guesswork.</span>
      </div>
      <div className="lp-foot-cols">
        <div><b>Product</b><a>+EV Bets</a><a>Combined Lines</a><a>PrizePicks</a><a>Backtest</a></div>
        <div><b>Company</b><a>About</a><a>Blog</a><a>Careers</a></div>
        <div><b>Legal</b><a>Terms</a><a>Privacy</a><a>Responsible play</a></div>
      </div>
      <div className="lp-foot-base">
        <span>© 2026 CoreProp</span>
        <span className="lp-foot-disc">18+. If you or someone you know has a gambling problem, call 1-800-GAMBLER.</span>
      </div>
    </footer>
  );
}

Object.assign(window, { Landing });
