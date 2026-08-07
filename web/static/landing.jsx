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

// The static worked example (LP_EXAMPLE / LP_EXAMPLE_MATH / ExampleCard) that
// used to fill the hero is gone: the hero is now the daily line challenge, a
// playable card fed by GET /api/public/daily-pick, whose reveal shows the same
// devig receipt on a LIVE market instead of a canned one. The ground rule
// didn't move — the game renders no probability, price, count or line that
// didn't arrive from the API in the same session.

// ─── MINIGAME v2 LAYOUT NOTE ───────────────────────────────────────────────
// v1 stacked a full-width paragraph hero on top of the game, so the card only
// became visible after a scroll and the reveal receipt was a cramped mono list
// under it. v2 makes the CARD the centre of the page: a two-line tagline, then
// a three-column hero grid — explainer | card | book panel — sized so the whole
// card including its action button fits above the fold on a 1280x800 desktop
// and a 390x844 phone. The two side columns are not decoration: the left one is
// the "what CoreProp does" explainer that the long hero paragraph used to
// carry, and the right one is the reveal, which needs the room (it renders four
// books x two sides). Both collapse under the card in source order at <1024px.
//
// The 3D card flip that used to reveal the CTA panel is GONE, and its absence
// is deliberate — see the comment above LineChallenge's action button.

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
      {/* HowItWorks restates the same three steps as the hero's left-hand
        * explainer at more length. Kept deliberately: the explainer is a
        * 3-line above-the-fold summary sized to the card, this section is the
        * readable version with the refresh cadence and the derived break-even.
        * If the duplication ever reads as padding, this is the one to cut —
        * not the explainer, which is load-bearing layout. */}
      <HowItWorks cov={cov} />
      <Method />
      <Limits />
      <FinalCTA onStart={onStart} onLogin={onLogin} cov={cov} />
      <Footer cov={cov} />
    </main>
  );
}

// ───────── Hero ─────────
// Owns the daily-pick fetch AND the current reveal, because the reveal is
// rendered by a SIBLING of the game (the right-hand book panel) rather than
// underneath the card. LineChallenge pushes its reveal up through onReveal;
// nothing else about the play state leaves the game.
//
// If /api/public/daily-pick fails or comes back empty, the hero renders the
// tagline, the explainer and the CTA row with no game and no book panel —
// anything the server can't answer, the page omits rather than guesses.
function Hero({ onLogin, onStart, cov }) {
  const sources = cov ? `${cov.books.length} ${cov.books_noun}` : null;
  // undefined = still loading (skeleton card holds the slot at its real size),
  // null = failed or zero picks (no game for the session).
  const [daily, setDaily] = React.useState(undefined);
  const [reveal, setReveal] = React.useState(null);
  // Stable identity so LineChallenge's effect doesn't re-fire on every render.
  const onReveal = React.useCallback((r) => setReveal(r), []);
  // Reused by LineChallenge when a reveal 404s: the board the visitor holds
  // may no longer exist server-side (tab open across the 8am ET boundary, or
  // the Render-wake unfrozen board replaced by the real freeze). Refetching
  // swaps the live board in instead of leaving a dead retry loop. A refresh
  // failure keeps whatever board is showing — never downgrade to null here.
  const fetchDaily = React.useCallback((isRefresh) => {
    fetch("/api/public/daily-pick")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => {
        const next = d && Array.isArray(d.picks) && d.picks.length > 0 ? d : null;
        if (next || !isRefresh) setDaily(next);
      })
      .catch(() => { if (!isRefresh) setDaily(null); });
  }, []);
  React.useEffect(() => { fetchDaily(false); }, [fetchDaily]);

  const loading = daily === undefined;
  return (
    <section className="lp-hero">
      <div className="lp-hero-hd">
        <h1 className="lp-h1">PrizePicks lines, priced against the sportsbooks.</h1>
        {/* ONE line. The three-sentence paragraph this replaced is now the
          * left column's three steps, where it doesn't push the card down. */}
        <p className="lp-sub">
          {sources
            ? `We take the margin out of what ${sources} charge, and show you which side of the line it was hiding.`
            : "We take the margin out of what the market charges, and show you which side of the line it was hiding."}
        </p>
      </div>

      {/* is-bare: the daily-pick fetch failed or came back empty, so there is
        * no card and no reveal to show. Collapse to one centred column rather
        * than leaving the explainer marooned in a left-hand third of an
        * otherwise empty three-column grid. */}
      <div className={"lp-hero-grid" + (daily === null ? " is-bare" : "")}>
        {/* Source order is card → books → explainer, which IS the <1024px
          * stacking order. Desktop reorders via grid-template-areas so the
          * card sits in the middle column without moving in the DOM (and so
          * the tab order still reaches the game before the prose). */}
        <div className="lp-hero-mid">
          {loading ? (
            <LpCardSkeleton />
          ) : daily ? (
            <LineChallenge
              // Keyed by board identity so a swapped-in board (same day_index,
              // different picks after a re-freeze) remounts the game with state
              // re-seeded from localStorage against the NEW pick ids.
              key={daily.day_index + ":" + daily.picks.map((p) => p.id).join(".")}
              daily={daily}
              cov={cov}
              onStart={onStart}
              onReveal={onReveal}
              onRefresh={() => fetchDaily(true)}
            />
          ) : null}
        </div>

        {/* The live region is a compact SUMMARY node, not the panel itself.
          * Making the whole panel role=status meant every pick — and every
          * "Next pick" that cleared it — re-read four book tiles, two bars and
          * the vig line end to end. A screen-reader user got a paragraph of
          * numbers read at them before they could navigate to any of it. The
          * summary announces the verdict and the two probabilities; the panel
          * stays ordinary content they can read at their own pace.
          *
          * Present from MOUNT (not conditionally rendered) — a live region
          * inserted at the same time as its text is not reliably announced. */}
        <p className="lp-sr-only" role="status" aria-live="polite">
          {reveal
            ? `${reveal.correct ? "Correct." : "Incorrect."} More ${reveal.p_more != null ? (reveal.p_more * 100).toFixed(1) : "?"} percent, Less ${reveal.p_less != null ? (reveal.p_less * 100).toFixed(1) : "?"} percent.`
            : ""}
        </p>
        <div className="lp-hero-books">
          {daily !== null && <LpBookPanel r={reveal} cov={cov} />}
        </div>

        <div className="lp-hero-why">
          <LpExplainer r={reveal} cov={cov} />
        </div>
      </div>

      <div className="lp-cta-row lp-center">
        <button className="cp-btn cp-btn-primary" onClick={onStart}>See pricing</button>
        <button className="cp-btn cp-btn-ghost" onClick={onLogin}>Log in</button>
      </div>
    </section>
  );
}

// ───────── Daily line challenge ─────────
//
// Frozen API contract (the backend builds to the same document):
//   GET  /api/public/daily-pick            -> { day_index, picks: [...] }
//        No probabilities, no favored side, no book odds — the answer must not
//        be readable in the Network tab before the visitor commits to a side.
//   GET  /api/public/daily-pick/reveal?id= -> { id, favored, p_more, p_less,
//        vig_pct, books: [{ book, side, american }] }
//        p_more/p_less/vig_pct are computed server-side FROM exactly that
//        books array, so the on-screen receipt is re-derivable by a reader.
//   POST /api/public/event                 -> 204, fire-and-forget analytics.
//
// Same ground rule as the rest of this file, enforced by
// tests/api_tests/test_landing_claims.py: every number the game renders —
// lines, trending counts, probabilities, book prices, vig — arrived from one
// of those responses at runtime. Nothing is typed into JSX.

const LP_GAME_LS = "coreprop:minigame";
const LP_SOUND_LS = "coreprop:minigame:sound";

// Fire-and-forget analytics. sendBeacon survives the navigation that
// cta_clicked immediately triggers; the fetch fallback sets keepalive for the
// same reason. Analytics must never break the game, hence the broad try.
function lpEvent(event, dayIndex, pickId, meta) {
  try {
    const body = JSON.stringify({
      event,
      day_index: dayIndex,
      ...(pickId ? { pick_id: pickId } : {}),
      ...(meta ? { meta } : {}),
    });
    if (navigator.sendBeacon) {
      navigator.sendBeacon("/api/public/event", new Blob([body], { type: "application/json" }));
    } else {
      fetch("/api/public/event", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body,
        keepalive: true,
      }).catch(() => {});
    }
  } catch (e) { /* never surface */ }
}

// localStorage: { day_index, plays: [{ id, side, correct }], dismissed }. A
// different day_index resets the game — the key stores exactly one day's
// state. Restored plays are intersected with the CURRENT board's pick ids:
// the same day_index can serve two boards (the Render-wake unfrozen board vs
// the blob frozen minutes later), and counting foreign plays would flip a
// visitor straight to the paywall with dots painted on picks they never saw.
function lpLoadPlays(dayIndex, picks) {
  try {
    const s = JSON.parse(localStorage.getItem(LP_GAME_LS) || "null");
    if (!s || s.day_index !== dayIndex || !Array.isArray(s.plays)) return [];
    const ids = new Set((picks || []).map((p) => p.id));
    return s.plays.filter((p) => p && p.id && p.side && ids.has(p.id));
  } catch (e) { return []; }
}
// A dismissal belongs to the BOARD it was made on, not just the day. The board
// can be upgraded once mid-day (web/minigame.py's provisional -> earned step),
// and lpLoadPlays drops plays whose ids aren't on the new board — so a
// day-scoped flag would survive into a game the visitor hasn't finished and
// suppress the CTA state, or worse, replace a live third reveal with the
// minbar the instant it rendered. Honour it only when the stored plays still
// cover every pick on the board in hand.
function lpLoadDismissed(dayIndex, picks) {
  try {
    const s = JSON.parse(localStorage.getItem(LP_GAME_LS) || "null");
    if (!s || s.day_index !== dayIndex || !s.dismissed) return false;
    const played = lpLoadPlays(dayIndex, picks);
    return picks && picks.length > 0 && played.length >= picks.length;
  } catch (e) { return false; }
}
// Patch-style writer so plays and dismissed don't clobber each other; a
// different stored day is discarded wholesale (same reset semantics as load).
function lpSaveState(dayIndex, patch) {
  try {
    let s = JSON.parse(localStorage.getItem(LP_GAME_LS) || "null");
    if (!s || s.day_index !== dayIndex || !Array.isArray(s.plays)) s = { day_index: dayIndex, plays: [] };
    localStorage.setItem(LP_GAME_LS, JSON.stringify({ ...s, ...patch }));
  } catch (e) { /* private mode: the game still works, it just won't persist */ }
}

// Checked at fire time, not mount time, so an OS-level toggle mid-session is
// honoured. The CSS side of the same setting lives in index.html (.lp-game
// block, reduced-motion re-grant).
function lpReducedMotion() {
  return !!(window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches);
}

// ───────── Sound ─────────
// Synthesised with the Web Audio API — no audio files, no CDN, no fourth
// third-party script. The page deliberately ships exactly three.
//
// The context is created LAZILY inside the click handler that first needs it:
// constructing one at module scope on a page the user hasn't interacted with
// leaves it `suspended` under every browser's autoplay policy, and Chrome logs
// a console warning for it. One context is reused for the whole session
// (browsers cap them at ~6 per page, and each holds an audio thread).
const lpSound = { ctx: null };

function lpAudioContext() {
  try {
    if (!lpSound.ctx) {
      const AC = window.AudioContext || window.webkitAudioContext;
      if (!AC) return null;
      lpSound.ctx = new AC();
    }
    // A context created during a gesture can still come back suspended if the
    // tab was backgrounded since; resume() is a no-op when it's already running.
    // resume() returns a promise that rejects when the browser declines (no
    // gesture, or a policy block). Swallow it — an unhandled rejection here
    // would surface as a console error on a marketing page.
    if (lpSound.ctx.state === "suspended" && lpSound.ctx.resume) {
      const p = lpSound.ctx.resume();
      if (p && p.catch) p.catch(() => {});
    }
    return lpSound.ctx;
  } catch (e) { return null; }
}

// One oscillator + its own gain envelope. exponentialRampToValueAtTime can
// never touch zero (it's undefined at 0), hence the 0.0001 floor at both ends —
// setting 0 there throws and would take the whole game's click handler with it.
function lpTone(ctx, type, f0, f1, at, dur, peak) {
  const osc = ctx.createOscillator();
  const gain = ctx.createGain();
  osc.type = type;
  osc.frequency.setValueAtTime(f0, at);
  if (f1 !== f0) osc.frequency.exponentialRampToValueAtTime(f1, at + dur);
  gain.gain.setValueAtTime(0.0001, at);
  gain.gain.exponentialRampToValueAtTime(peak, at + 0.012);   // fast attack
  gain.gain.exponentialRampToValueAtTime(0.0001, at + dur);   // exponential decay
  osc.connect(gain);
  gain.connect(ctx.destination);
  osc.start(at);
  osc.stop(at + dur + 0.02);
}

// win: A5 -> E6, a bright ascending two-note ding (~180ms total).
// loss: a blunt square-wave thud sweeping 200Hz -> 90Hz (~250ms).
// Wrapped end-to-end: audio must never break the game.
function lpPlaySound(kind) {
  try {
    const ctx = lpAudioContext();
    if (!ctx) return;
    const t = ctx.currentTime;
    if (kind === "win") {
      lpTone(ctx, "sine", 880, 880, t, 0.09, 0.22);
      lpTone(ctx, "sine", 1318.5, 1318.5, t + 0.085, 0.13, 0.20);
    } else {
      lpTone(ctx, "square", 200, 90, t, 0.25, 0.14);
    }
  } catch (e) { /* never surface */ }
}

// Default is UNMUTED — the sound is the point of the feature. The one
// exception is prefers-reduced-motion: a visitor who has asked the OS to stop
// things moving has not asked for surprise audio either, so that setting seeds
// the default to muted. It only seeds it: an explicit stored choice always
// wins, in both directions.
function lpLoadMuted() {
  try {
    const v = localStorage.getItem(LP_SOUND_LS);
    if (v === "on") return false;
    if (v === "off") return true;
  } catch (e) { /* private mode: fall through to the default */ }
  return lpReducedMotion();
}
function lpSaveMuted(muted) {
  try { localStorage.setItem(LP_SOUND_LS, muted ? "off" : "on"); } catch (e) {}
}

function LpSpeakerIcon({ muted }) {
  return (
    <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor"
         strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M4 9.5v5h3.5L12 18.5v-13L7.5 9.5H4z" />
      {muted
        ? <path d="M16 9.5l4 5M20 9.5l-4 5" />
        : <path d="M15.4 9.2a4 4 0 0 1 0 5.6M17.9 6.8a7.5 7.5 0 0 1 0 10.4" />}
    </svg>
  );
}

// trending_count -> the compact form the PP card uses ("7013" -> "7.0K").
// Non-positive returns null so the flame hides (the backend defaults sparse
// boards to 0, and PP itself shows no flame without a count — a flame with a
// literal "0" beside it would be worse than no flame).
function lpTrending(n) {
  if (typeof n !== "number" || !isFinite(n) || n <= 0) return null;
  return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : String(n);
}

// game_start (ISO-8601 UTC) -> "Thu 7:00pm" in America/New_York, the wall
// clock the card's audience reads schedules in.
function lpGameTime(iso) {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      weekday: "short",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    }).formatToParts(new Date(iso));
    const get = (t) => (parts.find((p) => p.type === t) || {}).value || "";
    return `${get("weekday")} ${get("hour")}:${get("minute")}${get("dayPeriod").toLowerCase()}`;
  } catch (e) { return ""; }
}

function lpAmerican(n) { return n > 0 ? `+${n}` : String(n); }

// PrizePicks' exact more/less arrow (26x26, round-cap 2px stem + chevron
// head). This path IS the down arrow; the up arrow is the same path rotated
// 180deg, which is how PP renders theirs too.
const LP_ARROW_D = "M11.9223 17.5426L11.9223 5.83687C11.9223 5.28811 12.3671 4.84326 12.9159 4.84326C13.4646 4.84326 13.9095 5.28811 13.9095 5.83687L13.9095 17.5426L18.9472 13.2216C19.3548 12.872 19.9674 12.9138 20.3238 13.3154C20.6888 13.7267 20.6459 14.3573 20.2284 14.7154L13.8842 20.157C13.3271 20.6349 12.5047 20.6349 11.9476 20.157L5.60333 14.7154C5.18591 14.3573 5.14296 13.7267 5.50798 13.3154C5.86439 12.9138 6.47698 12.872 6.88456 13.2216L11.9223 17.5426Z";
function LpArrow({ dir }) {
  return (
    <svg
      viewBox="0 0 26 26" width="26" height="26" fill="currentColor" aria-hidden="true"
      style={dir === "up" ? { transform: "rotate(180deg)" } : undefined}
    >
      <path d={LP_ARROW_D} />
    </svg>
  );
}

// The win-burst mascot, drawn once and kept isolated so it can be swapped for
// different art without touching the particle physics: a bright-green sphere
// (two overlapping circles fake the radial shading), two up-and-out horns,
// arched closed eyes, small smirk. Returns markup — the FX layer sets it as
// innerHTML on throwaway particles.
function goblinSVG(size) {
  return (
    `<svg width="${size}" height="${size}" viewBox="0 0 32 32" xmlns="http://www.w3.org/2000/svg">` +
    `<path d="M8.5 8.5 L3.5 1.5 L11.5 5.5 Z" fill="#5CA806"/>` +
    `<path d="M23.5 8.5 L28.5 1.5 L20.5 5.5 Z" fill="#5CA806"/>` +
    `<circle cx="16" cy="17.5" r="12" fill="#78D808"/>` +
    `<circle cx="12.5" cy="14" r="8" fill="#93E82B" opacity=".55"/>` +
    `<path d="M9.5 15.5 Q12 12.8 14.5 15.5" stroke="#123300" stroke-width="1.8" fill="none" stroke-linecap="round"/>` +
    `<path d="M17.5 15.5 Q20 12.8 22.5 15.5" stroke="#123300" stroke-width="1.8" fill="none" stroke-linecap="round"/>` +
    `<path d="M12.5 21.5 Q16.5 24.5 20.5 20.8" stroke="#123300" stroke-width="1.8" fill="none" stroke-linecap="round"/>` +
    `</svg>`
  );
}

function lpXSVG(size) {
  return (
    `<svg width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">` +
    `<path d="M5 5 L19 19 M19 5 L5 19" stroke="#FF4A4A" stroke-width="4" stroke-linecap="round"/>` +
    `</svg>`
  );
}

// Shared particle-arc helper: WAAPI keyframes for a launch at (vx, vy) px/s
// under gravity, fading over the last 30%.
function lpArcKeyframes(vx, vy, durMs, spinDeg) {
  const g = 980;
  return [0, 0.33, 0.66, 1].map((k) => {
    const t = (durMs / 1000) * k;
    return {
      offset: k,
      transform: `translate(${vx * t}px, ${vy * t + 0.5 * g * t * t}px) rotate(${spinDeg * k}deg)`,
      opacity: k < 0.7 ? 1 : 1 - (k - 0.7) / 0.3,
    };
  });
}

function lpMakeOverlay(hostEl) {
  const overlay = document.createElement("div");
  overlay.className = "lp-game-fx";
  overlay.setAttribute("aria-hidden", "true");
  hostEl.appendChild(overlay);
  return overlay;
}

// Burst size. The full 44 (goblins + confetti, the i % 5 mix below) is the
// intended spec read, but a phone or a low-core laptop animating 44 elements
// on gravity arcs is exactly the stutter the owner reported as "laggy". Halve
// it where the device tells us it's weak: a coarse pointer (touch) or few
// logical cores. navigator.hardwareConcurrency is absent on some browsers, so
// the check must not treat "unknown" as "weak".
function lpBurstCount() {
  try {
    const cores = navigator.hardwareConcurrency;
    const coarse = window.matchMedia && window.matchMedia("(pointer: coarse)").matches;
    if (coarse || (typeof cores === "number" && cores > 0 && cores <= 4)) return 22;
  } catch (e) { /* fall through to the full burst */ }
  return 44;
}

// WIN: card pop, radial flash from the tapped button, particle burst on
// gravity arcs (goblin sprites + confetti rects — the mix is the intended read
// of the "goblin burst" spec; don't "fix" the i % 5 ratio), floating "+EV"
// chips. All hand-rolled WAAPI — no libraries, no new script tags (the page
// deliberately ships exactly three third-party scripts). Skipped entirely
// under prefers-reduced-motion.
function lpWinFx(hostEl, btnEl) {
  if (lpReducedMotion() || !hostEl || !hostEl.animate) return;
  const card = hostEl.querySelector(".lp-game-card");
  if (card) {
    card.animate(
      [{ transform: "scale(1)" }, { transform: "scale(1.04)" }, { transform: "scale(1)" }],
      { duration: 250, easing: "ease-out" }
    );
  }
  const overlay = lpMakeOverlay(hostEl);
  if (btnEl) {
    const hr = hostEl.getBoundingClientRect();
    const br = btnEl.getBoundingClientRect();
    const flash = document.createElement("span");
    flash.className = "lp-game-flash";
    flash.style.left = `${br.left - hr.left + br.width / 2}px`;
    flash.style.top = `${br.top - hr.top + br.height / 2}px`;
    overlay.appendChild(flash);
    flash.animate(
      [
        { transform: "translate(-50%,-50%) scale(.2)", opacity: 0.9 },
        { transform: "translate(-50%,-50%) scale(3)", opacity: 0 },
      ],
      { duration: 450, easing: "ease-out", fill: "forwards" }
    );
  }
  const cx = hostEl.offsetWidth / 2;
  const cy = hostEl.offsetHeight / 2;
  const sizes = [16, 22, 28];
  const burst = lpBurstCount();
  for (let i = 0; i < burst; i++) {
    const el = document.createElement("span");
    el.className = "lp-game-particle";
    if (i % 5 < 2) {
      el.innerHTML = goblinSVG(sizes[i % 3]);
    } else {
      el.className += " lp-game-confetti";
      const s = 5 + (i % 4) * 2;
      el.style.width = `${s}px`;
      el.style.height = `${Math.round(s * 1.6)}px`;
    }
    el.style.left = `${cx}px`;
    el.style.top = `${cy}px`;
    overlay.appendChild(el);
    const dur = 1400 + Math.random() * 400;
    const ang = Math.random() * Math.PI * 2;
    const v = 200 + Math.random() * 280;
    const spin = (Math.random() < 0.5 ? -1 : 1) * (360 + Math.random() * 540);
    el.animate(
      lpArcKeyframes(Math.cos(ang) * v, Math.sin(ang) * v - 260, dur, spin),
      { duration: dur, easing: "linear", fill: "forwards" }
    );
  }
  for (let i = 0; i < 3; i++) {
    const chip = document.createElement("span");
    chip.className = "lp-game-evchip";
    chip.textContent = "+EV";
    chip.style.left = `${cx + (i - 1) * 64}px`;
    chip.style.top = `${cy}px`;
    overlay.appendChild(chip);
    chip.animate(
      [
        { transform: "translate(-50%,-50%)", opacity: 0 },
        { transform: "translate(-50%,-80%)", opacity: 1, offset: 0.2 },
        { transform: "translate(-50%,-240%)", opacity: 0 },
      ],
      { duration: 1100, delay: 150 * i, easing: "ease-out", fill: "forwards" }
    );
  }
  setTimeout(() => overlay.remove(), 2200);
}

// LOSS: shake, red wash, a giant X stamped over the card with overshoot that
// lingers ~1.2s, and 16 red X shards on the same gravity arcs. The content
// desaturation is a CSS class (.lp-game-body.is-lost), not FX, so it survives
// reduced-motion and stays until "Next pick".
function lpLossFx(hostEl) {
  if (lpReducedMotion() || !hostEl || !hostEl.animate) return;
  const card = hostEl.querySelector(".lp-game-card");
  if (card) {
    const xs = [0, -9, 8, -6, 5, -3, 0];
    card.animate(
      xs.map((x, i) => ({ transform: `translateX(${x}px)`, offset: i / (xs.length - 1) })),
      { duration: 500, easing: "ease-out" }
    );
  }
  const overlay = lpMakeOverlay(hostEl);
  const wash = document.createElement("span");
  wash.className = "lp-game-wash";
  overlay.appendChild(wash);
  wash.animate([{ opacity: 0.55 }, { opacity: 0 }], { duration: 600, easing: "ease-out", fill: "forwards" });
  const stamp = document.createElement("span");
  stamp.className = "lp-game-stamp";
  stamp.innerHTML = lpXSVG(120);
  overlay.appendChild(stamp);
  stamp.animate(
    [
      { transform: "translate(-50%,-50%) scale(2.4)", opacity: 0 },
      { transform: "translate(-50%,-50%) scale(.92)", opacity: 1, offset: 0.2 },
      { transform: "translate(-50%,-50%) scale(1)", opacity: 1, offset: 0.3 },
      { transform: "translate(-50%,-50%) scale(1)", opacity: 1, offset: 0.9 },
      { transform: "translate(-50%,-50%) scale(1)", opacity: 0 },
    ],
    { duration: 1650, easing: "ease-out", fill: "forwards" }
  );
  const cx = hostEl.offsetWidth / 2;
  const cy = hostEl.offsetHeight / 2;
  for (let i = 0; i < 16; i++) {
    const shard = document.createElement("span");
    shard.className = "lp-game-particle";
    shard.innerHTML = lpXSVG(10 + (i % 3) * 5);
    shard.style.left = `${cx}px`;
    shard.style.top = `${cy}px`;
    overlay.appendChild(shard);
    const dur = 900 + Math.random() * 400;
    const ang = Math.random() * Math.PI * 2;
    const v = 260 + Math.random() * 220;
    const spin = (Math.random() < 0.5 ? -1 : 1) * (240 + Math.random() * 360);
    shard.animate(
      lpArcKeyframes(Math.cos(ang) * v, Math.sin(ang) * v - 180, dur, spin),
      { duration: dur, easing: "linear", fill: "forwards" }
    );
  }
  setTimeout(() => overlay.remove(), 2200);
}

// Held while /api/public/daily-pick is in flight, at the real card's geometry.
// Without it the hero painted its header, then a beat later grew ~450px of
// card underneath and shoved the page down — the "takes way too long to load"
// complaint was partly a layout-shift illusion. aria-hidden: it is scaffolding,
// and the live region in the right-hand column is what announces state.
function LpCardSkeleton() {
  return (
    <div className="lp-game lp-game-sk" aria-hidden="true">
      <div className="lp-game-meter"><span className="lp-sk lp-sk-meter" /></div>
      <p className="lp-game-demo">Demo only · No wagering · No prizes</p>
      <div className="lp-game-stagebg">
        <div className="lp-game-stage">
          <div className="lp-game-card">
            <div className="lp-game-body">
              <div className="lp-game-top" />
              <div className="lp-game-photo"><span className="lp-sk lp-sk-photo" /></div>
              <div className="lp-game-meta">
                <span className="lp-sk lp-sk-l1" />
                <span className="lp-sk lp-sk-l2" />
                <span className="lp-sk lp-sk-l3" />
              </div>
              <div className="lp-game-line"><span className="lp-sk lp-sk-line" /></div>
              <div className="lp-game-btns"><span className="lp-sk-btn" /><span className="lp-sk-btn" /></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function LineChallenge({ daily, cov, onStart, onReveal, onRefresh }) {
  const dayIndex = daily.day_index;
  const total = daily.picks.length;   // "3 free" when possible; fewer if the backfill ran dry
  const [plays, setPlays] = React.useState(() => lpLoadPlays(dayIndex, daily.picks));
  const [reveal, setReveal] = React.useState(null);   // reveal payload + { side, correct }
  const [sel, setSel] = React.useState(null);         // side tapped on the current card
  const [busy, setBusy] = React.useState(false);      // reveal fetch in flight
  const [dismissed, setDismissed] = React.useState(() => lpLoadDismissed(dayIndex, daily.picks));
  const [muted, setMuted] = React.useState(lpLoadMuted);
  const [imgFailed, setImgFailed] = React.useState({});
  const [loadErr, setLoadErr] = React.useState(false);
  const stageRef = React.useRef(null);
  const actionRef = React.useRef(null);
  const btnsRef = React.useRef(null);

  // "Next pick" and a failed grade both unmount the control that had focus,
  // dropping a keyboard user to <body> mid-game. Send focus back to the fresh
  // card's first button — but only if focus was inside the card to begin with,
  // so a mouse user who has since clicked elsewhere on the page isn't yanked.
  const refocusCard = React.useCallback(() => {
    const stage = stageRef.current;
    if (!stage || !stage.contains(document.activeElement)) return;
    window.requestAnimationFrame(() => {
      const first = btnsRef.current && btnsRef.current.querySelector("button:not([disabled])");
      if (!first) return;
      try { first.focus({ preventScroll: true }); } catch (e) { first.focus(); }
    });
  }, []);

  // Once per mount, including the restored already-played state.
  React.useEffect(() => { lpEvent("game_viewed", dayIndex); }, []);

  // The reveal is rendered by a sibling column (Hero -> LpBookPanel), so it has
  // to travel up. Pushing it from an effect rather than from pickSide keeps the
  // unmount case honest: a board swap clears the panel instead of leaving a
  // receipt for a pick that no longer exists.
  React.useEffect(() => {
    if (onReveal) onReveal(reveal);
    return () => { if (onReveal) onReveal(null); };
  }, [reveal, onReveal]);

  const playedIds = new Set(plays.map((p) => p.id));
  // While the reveal is open the card must stay on the pick just graded, not
  // advance to the next unplayed one — that's what "Next pick" is for.
  const revealPick = reveal ? daily.picks.find((p) => p.id === reveal.id) : null;
  const nextPick = daily.picks.find((p) => !playedIds.has(p.id)) || null;
  const card = revealPick || nextPick;
  const done = plays.length >= total;

  // Keyboard users must not be stranded on a disabled More/Less pair after a
  // grade: focus follows the one live control. preventScroll because the card
  // is already in view — scrolling the hero on a mouse click would be a jump
  // nobody asked for. Keyed on the pick id so it fires once per reveal.
  const revealedId = reveal ? reveal.id : null;
  React.useEffect(() => {
    if (!revealedId || !actionRef.current) return;
    try { actionRef.current.focus({ preventScroll: true }); }
    catch (e) { actionRef.current.focus(); }
    // Single column (<1024px): the book panel is stacked BELOW the card, so
    // the receipt the whole game exists to show lands off-screen on a phone.
    // Desktop keeps preventScroll — there the panel is the right-hand column
    // and already visible, so scrolling would be a jump nobody asked for.
    try {
      const stacked = window.matchMedia && window.matchMedia("(max-width: 1023px)").matches;
      const panel = stacked && document.querySelector(".lp-books");
      if (panel && panel.scrollIntoView) {
        panel.scrollIntoView({ block: "nearest", behavior: lpReducedMotion() ? "auto" : "smooth" });
      }
    } catch (e) { /* scrolling is a nicety; never break the reveal */ }
  }, [revealedId]);

  const pickSide = async (side, ev) => {
    if (!card || busy || sel || reveal) return;
    const btn = ev.currentTarget;
    // Warm the AudioContext HERE, synchronously inside the click handler. The
    // sound itself can only play after the reveal fetch resolves, and by then
    // the user-gesture window has closed — Safari and iOS refuse to start a
    // context outside a gesture, so the first ding of the session was silent.
    // Creating it now (and resuming it) means the later lpPlaySound only has to
    // schedule oscillators on an already-running context.
    if (!muted) lpAudioContext();
    setBusy(true);
    setSel(side);
    setLoadErr(false);
    lpEvent("pick_made", dayIndex, card.id, { side });
    let r = null;
    try {
      const res = await fetch(`/api/public/daily-pick/reveal?id=${encodeURIComponent(card.id)}`);
      if (res.ok) r = await res.json();
    } catch (e) { /* falls through to the retry state */ }
    setBusy(false);
    if (!r || (r.favored !== "more" && r.favored !== "less")) {
      // Couldn't grade it. Put the card back rather than inventing a result —
      // and refetch the board: the id may belong to a board that no longer
      // exists server-side (8am ET rollover, Render-wake re-freeze), where
      // retrying the same dead id would loop forever. If the board actually
      // changed, Hero's key swap remounts the game on the live picks.
      setSel(null);
      setLoadErr(true);
      refocusCard();
      if (onRefresh) onRefresh();
      return;
    }
    const correct = side === r.favored;
    const nextPlays = [...plays, { id: card.id, side, correct }];
    setPlays(nextPlays);
    lpSaveState(dayIndex, { plays: nextPlays });
    setReveal({ ...r, side, correct });
    lpEvent("revealed", dayIndex, card.id, { correct });
    if (stageRef.current) (correct ? lpWinFx : lpLossFx)(stageRef.current, btn);
    if (!muted) lpPlaySound(correct ? "win" : "loss");
    // Fired as the LAST reveal renders. v1 fired this from a 1.6s timer that
    // also flipped the card to the paywall; the timer is gone (see below) but
    // the funnel event still marks the same moment.
    if (nextPlays.length >= total) lpEvent("plays_exhausted", dayIndex);
  };

  const next = () => { setReveal(null); setSel(null); refocusCard(); };
  const cta = () => { lpEvent("cta_clicked", dayIndex); onStart && onStart(); };
  const dismiss = () => { setDismissed(true); lpSaveState(dayIndex, { dismissed: true }); };
  const toggleMute = () => { setMuted((m) => { lpSaveMuted(!m); return !m; }); };

  // Disclosed BEFORE the first play: this is a limited free game.
  const meterText = done ? "Free picks played" : `Play ${plays.length + 1} of ${total} free`;

  // A RETURNING visitor (three plays already in localStorage, so no card left
  // to draw) gets the CTA panel in the card's place, still dismissible to the
  // one-line minbar. That is now the ONLY path to the CTA panel: mid-session,
  // the third reveal stays on screen until the visitor clicks through it.
  const standaloneBack = done && !card;

  const btnCls = (side) => {
    if (sel !== side) return "";
    if (reveal) return reveal.correct ? " is-win" : " is-loss";
    return " is-sel";
  };

  return (
    <div className="lp-game" aria-label="Daily line challenge">
      <div className="lp-game-meter">
        <span className="lp-game-dots" aria-hidden="true">
          {daily.picks.map((p) => {
            // Keyed by pick id, not position: restored plays can be a partial
            // subset, and a positional lookup would paint win/loss dots on
            // picks the plays don't belong to.
            const pl = plays.find((x) => x.id === p.id);
            return (
              <i
                key={p.id}
                className={"lp-game-dot" + (pl ? (pl.correct ? " is-won" : " is-lost") : "")}
              />
            );
          })}
        </span>
        <span>{meterText}</span>
        {/* Real <button>, always visible, in the tab order, state in the
          * accessible name via aria-pressed + label. */}
        <button
          type="button"
          className={"lp-game-mute" + (muted ? " is-muted" : "")}
          aria-label={muted ? "Turn sound on" : "Turn sound off"}
          aria-pressed={!muted}
          onClick={toggleMute}
        >
          <LpSpeakerIcon muted={muted} />
        </button>
      </div>

      {/* Visible from the FIRST play, not just post-exhaustion: the card is a
        * deliberate PP near-clone, so what this is (and is not) must be
        * readable while it's playable. This is the game's ONE copy of the
        * line — the CTA back panel and minbar don't repeat it, per the
        * disclaimer-placement rule in CLAUDE.md. */}
      <p className="lp-game-demo">Demo only · No wagering · No prizes</p>

      {done && dismissed ? (
        // The dismissed paywall collapses to one line — it must stay dismissible.
        <div className="lp-game-minbar">
          <span>Free picks done for today</span>
          <button type="button" className="cp-link" onClick={cta}>See pricing</button>
        </div>
      ) : (
        <div className="lp-game-stagebg">
          {/* .lp-game-stage is the FX host: position:relative and exactly the
            * card's box, so the overlay's inset:0 wash lines up with the card's
            * 22px radius. (It replaced the 3D flip container, which was the
            * host before.) */}
          <div className="lp-game-stage" ref={stageRef}>
            {standaloneBack ? (
              <LpCtaPanel cov={cov} onCta={cta} onDismiss={dismiss} />
            ) : card ? (
              <div className={"lp-game-card" + (reveal && reveal.correct ? " is-winner" : "")}>
                <div className={"lp-game-body" + (reveal && !reveal.correct ? " is-lost" : "")}>
                  <div className="lp-game-top">
                    {/* PP's card also carries an "L5" (last-5-games) stat
                      * toggle here. Deliberately NOT cloned: "L5" implies
                      * five games of data behind it and no last-5 datum
                      * exists in the daily-pick payload — same ground rule
                      * as every other figure. Add it back only if/when the
                      * API ships one. */}
                    {lpTrending(card.trending_count) && (
                      <span className="lp-game-trend">
                        <svg viewBox="0 0 24 24" width="16" height="16" fill="#FF7A1A" aria-hidden="true">
                          <path d="M12 2c1.2 3-.9 4.6-2.2 6.1C8.3 9.8 7 11.4 7 14a5 5 0 0 0 10 0c0-1.9-.8-3.3-1.8-4.5-.4 1-.9 1.7-1.7 2.2.4-2.7-.2-6.6-1.5-9.7z" />
                        </svg>
                        {lpTrending(card.trending_count)}
                      </span>
                    )}
                  </div>
                  <div className="lp-game-photo">
                    {card.image_url && !imgFailed[card.id] ? (
                      <img
                        src={card.image_url}
                        alt=""
                        onError={() => setImgFailed((f) => ({ ...f, [card.id]: true }))}
                      />
                    ) : (
                      // Never a broken image: same initials-tile idiom as
                      // the avatar in components.jsx, keyed on the team.
                      <span className="lp-game-photo-fb" aria-hidden="true">{card.team}</span>
                    )}
                  </div>
                  <div className="lp-game-meta">
                    <div className="lp-game-teampos">{card.team} - {card.position}</div>
                    <div className="lp-game-player">{card.player}</div>
                    <div className="lp-game-matchup">
                      <b>vs {card.opponent}</b> <span>{lpGameTime(card.game_start)}</span>
                    </div>
                  </div>
                  <div className="lp-game-line">
                    <span className="lp-game-swap" aria-hidden="true">
                      <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="#C9C8D1" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M4 8h13l-3-3M20 16H7l3 3" />
                      </svg>
                    </span>
                    <span className="lp-game-num">{card.line}</span>
                    <span className="lp-game-prop">{card.prop}</span>
                  </div>
                  <div ref={btnsRef} className={"lp-game-btns" + (!sel && !reveal && !busy ? " is-idle" : "")}>
                    <button
                      type="button"
                      className={"lp-game-btn" + btnCls("less")}
                      disabled={busy || !!sel || !!reveal}
                      onClick={(e) => pickSide("less", e)}
                    >
                      <LpArrow dir="down" /> Less
                    </button>
                    <button
                      type="button"
                      className={"lp-game-btn lp-game-btn-r" + btnCls("more")}
                      disabled={busy || !!sel || !!reveal}
                      onClick={(e) => pickSide("more", e)}
                    >
                      <LpArrow dir="up" /> More
                    </button>
                  </div>
                </div>
                {/* THE next action, on the card, full width and 54px tall.
                  * v1 put a small ghost "Next pick" at the bottom of the
                  * reveal panel and auto-flipped to the paywall 1.6s after
                  * the third grade. Both are gone:
                  *   - one obvious control, where the eye already is;
                  *   - nothing auto-advances, so the third receipt stays up
                  *     for as long as the visitor wants to read it. The
                  *     button just changes its label and its handler, which
                  *     is what turns the habit built on picks 1-2 into the
                  *     click that opens pricing.
                  * It is a SIBLING of .lp-game-body, not a child: the loss
                  * state puts a grayscale filter on the body, and the one
                  * live control on the card must not be greyed out by it. */}
                {reveal && (
                  <div className="lp-game-act">
                    <button
                      ref={actionRef}
                      type="button"
                      className="lp-game-actbtn"
                      onClick={done ? cta : next}
                    >
                      {done ? "See pricing" : "Next pick"}
                    </button>
                  </div>
                )}
              </div>
            ) : null}
          </div>
          {loadErr && (
            // role="alert": the side buttons silently re-enable on a grading
            // failure, so a screen reader must hear why nothing happened.
            <p className="lp-game-err" role="alert">Couldn't grade that one — tap a side to try again.</p>
          )}
        </div>
      )}
    </div>
  );
}

// ───────── Book panel (the reveal) ─────────
// Ben's note on v1: "how we're displaying the info as to why they're correct or
// wrong is sloppy and hard to read... they need to be much more visible (maybe
// added on the side after selection) also doubles as filling space as well as
// they need to be color coded to the colors we have for those sportsbooks."
// So: its own column, one tile per book, and the pill colours are BookBadge's
// (components.jsx) verbatim — same book, same colour, everywhere on the site.
//
// Every figure still renders verbatim from the reveal response. The only
// arithmetic is percent formatting and the implied-probability total, which for
// a two-sided devig is 100 plus the vig by construction (the backend computes
// vig_pct from exactly the books array shown).
const LP_BOOK_STYLE = {
  FanDuel:    { ab: "FD",  bg: "rgba(239,68,68,.16)",  fg: "#FCA5A5" },
  DraftKings: { ab: "DK",  bg: "rgba(34,197,94,.16)",  fg: "#86EFAC" },
  Pinnacle:   { ab: "PIN", bg: "rgba(250,204,21,.18)", fg: "#FDE68A" },
  Novig:      { ab: "NV",  bg: "rgba(45,212,191,.18)", fg: "#5EEAD4" },
};
// A book we have no colour for still renders, in neutral. Silently dropping a
// quote would break the one property this panel exists to have: the
// probabilities above it are re-derivable from the rows below it.
const LP_BOOK_FALLBACK = { bg: "rgba(255,255,255,.07)", fg: "#C9C8D1" };

// books: [{ book, side, american }] -> one row per book, in first-seen order,
// with a null for any side that book didn't quote. Nulls render as an em dash;
// they are NOT filled in from the other side's price, which would put a number
// on screen that no book ever posted.
function lpGroupBooks(books) {
  const order = [];
  const by = {};
  (books || []).forEach((b) => {
    if (!b || !b.book) return;
    if (!by[b.book]) { by[b.book] = { book: b.book, more: null, less: null }; order.push(b.book); }
    if (b.side === "more") by[b.book].more = b.american;
    else if (b.side === "less") by[b.book].less = b.american;
  });
  return order.map((k) => by[k]);
}

function LpBookPanel({ r, cov }) {
  if (!r) {
    // Never an empty column. States what the tap buys without implying odds.
    const what = cov ? `all ${cov.books.length} ${cov.books_noun}` : "every price we have";
    return (
      <div className="lp-books lp-books-idle">
        <h2 className="lp-books-h">The other side of the line</h2>
        <p className="lp-books-idle-b">Pick a side to see {what} on this player, and what the price says once their margin comes out.</p>
        {/* Three quiet placeholder rows so the column holds the height the
          * real receipt will need — no layout jump when it arrives. */}
        <div className="lp-bk-ghosts" aria-hidden="true">
          <span /><span /><span />
        </div>
      </div>
    );
  }
  const rows = lpGroupBooks(r.books);
  const favoredMore = r.favored === "more";
  const morePct = (r.p_more * 100).toFixed(1);
  const lessPct = (r.p_less * 100).toFixed(1);
  const impliedTotal = (100 + r.vig_pct).toFixed(1);
  return (
    <div className="lp-books lp-books-live">
      <p className={"lp-books-verdict " + (r.correct ? "is-good" : "is-bad")}>
        {r.correct ? "You took the better side." : "That was the worse side."}
      </p>
      <div className="lp-books-bars">
        <div className="lp-books-barrow">
          <span className="lp-books-barlbl">More</span>
          <div className="lp-books-bartrack">
            {/* --p is the probability (0..1) straight off the response; the
              * keyframe scales X to it. Never animate width here. */}
            <span className={"lp-books-barfill" + (favoredMore ? " is-fav" : "")} style={{ "--p": r.p_more }} />
          </div>
          <span className="lp-books-barpct mono">{morePct}%</span>
        </div>
        <div className="lp-books-barrow">
          <span className="lp-books-barlbl">Less</span>
          <div className="lp-books-bartrack">
            <span className={"lp-books-barfill" + (!favoredMore ? " is-fav" : "")} style={{ "--p": r.p_less }} />
          </div>
          <span className="lp-books-barpct mono">{lessPct}%</span>
        </div>
      </div>

      <h2 className="lp-books-h">What each book charged</h2>
      <div className="lp-bk-list">
        <div className="lp-bk-head" aria-hidden="true">
          <span />
          <span>More</span>
          <span>Less</span>
        </div>
        {rows.map((row) => {
          const st = LP_BOOK_STYLE[row.book];
          const c = st || LP_BOOK_FALLBACK;
          return (
            <div key={row.book} className="lp-bk-row">
              <span className="lp-bk-id">
                <span className="lp-bk-pill" style={{ background: c.bg, color: c.fg }}>
                  {st ? st.ab : row.book.slice(0, 3).toUpperCase()}
                </span>
                <span className="lp-bk-name">{row.book}</span>
              </span>
              <span className={"lp-bk-q mono" + (favoredMore ? " is-fav" : "")}>
                {row.more === null || row.more === undefined ? "—" : lpAmerican(row.more)}
              </span>
              <span className={"lp-bk-q mono" + (!favoredMore ? " is-fav" : "")}>
                {row.less === null || row.less === undefined ? "—" : lpAmerican(row.less)}
              </span>
            </div>
          );
        })}
      </div>

      <p className="lp-books-vig mono">
        implied total {impliedTotal}% — {r.vig_pct}% vig
      </p>
      <p className="lp-books-close">Same payout either way — but it was never a coin flip.</p>
    </div>
  );
}

// ───────── Hero explainer (left column) ─────────
// The three-sentence paragraph that used to sit under the headline, cut into
// the three steps the engine actually runs, plus the vig visual. It fills the
// dead space on the left of the card AND it is the only place on the first
// screen that says what the product does.
//
// NUMBERS: the break-even is DERIVED from the payout table (same rule as
// HowItWorks). Everything else in here waits for the reveal payload — before a
// pick, LpVigViz is schematic and carries no figures at all.
function LpExplainer({ r, cov }) {
  const refresh = cov ? fmtRefresh(cov.refresh_minutes) : null;
  const sources = cov ? `${cov.books.length} ${cov.books_noun}` : "the market";
  const steps = [
    { t: "Match the market", b: `The same player prop, found at ${sources}${refresh ? `, re-read every ${refresh}` : ""}.` },
    { t: "Strip the margin", b: "Both sides of each price are devigged with Shin's method, then averaged into one fair number." },
    { t: "Rank against break-even", b: `What's left is compared to what a slip needs: ${LP_POWER_6_BE_PCT.toFixed(2)}% per leg on a 6-leg Power.` },
  ];
  return (
    <div className="lp-why">
      <h2 className="lp-why-h">What CoreProp does</h2>
      <ol className="lp-why-steps">
        {steps.map((s, i) => (
          <li key={s.t}>
            <span className="lp-why-n mono" aria-hidden="true">{i + 1}</span>
            <div>
              <b>{s.t}</b>
              <span>{s.b}</span>
            </div>
          </li>
        ))}
      </ol>
      <LpVigViz r={r} />
    </div>
  );
}

// Two-segment bar: the fair market price, and the overround sitting on top of
// it. Before a reveal it is drawn in fixed SVG user units and carries NO
// labels — it shows the shape of the idea, not a quantity. Once a pick is
// revealed the segments are re-proportioned from that pick's own vig_pct and
// labelled with it. There is no percentage literal in here at any point.
function LpVigViz({ r }) {
  const W = 250, H = 14;
  // Schematic split when there's nothing to source: a wide fair segment and a
  // narrow margin segment, in user units.
  let fairW = 214;
  if (r) {
    const total = 100 + r.vig_pct;
    fairW = (100 / total) * W;
  }
  const vigW = Math.max(W - fairW, 1);
  return (
    <figure className="lp-vig">
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H} aria-hidden="true" className="lp-vig-svg">
        <rect x="0" y="0" width={fairW} height={H} rx="4" fill="#1E6FB0" />
        <rect x={fairW + 2} y="0" width={Math.max(vigW - 2, 1)} height={H} rx="4" fill="#FF4A4A" />
      </svg>
      <figcaption className="lp-vig-cap">
        <span><i className="lp-vig-key is-fair" />{r ? "fair price" : "the fair price"}</span>
        <span><i className="lp-vig-key is-vig" />{r ? `${r.vig_pct}% margin` : "their margin"}</span>
      </figcaption>
    </figure>
  );
}

// The CTA card shown in the card's place to a RETURNING visitor whose three
// plays are already in localStorage. The source-count/cadence line interpolates
// coverage facts the same way the rest of the page does (books_noun,
// fmtRefresh); with no coverage payload it states no figures.
function LpCtaPanel({ cov, onCta, onDismiss }) {
  const sub = cov
    ? `CoreProp finds the mispriced side across ${cov.books.length} ${cov.books_noun}, every ${fmtRefresh(cov.refresh_minutes)}.`
    : "CoreProp finds the mispriced side on every line it prices.";
  return (
    <div className="lp-game-ctap">
      <button type="button" className="lp-game-ctap-x" aria-label="Dismiss" onClick={onDismiss}>✕</button>
      {/* Not "Want to keep playing?" — the paid app has no minigame, so that
        * heading promised something the subscription doesn't deliver. The
        * pivot is to what it DOES deliver: the full board, all day. The demo
        * disclaimer line lives above the stage (visible from play one), not
        * here. */}
      <h3 className="lp-game-ctap-h">Want the full board?</h3>
      <p className="lp-game-ctap-sub">{sub}</p>
      <button type="button" className="cp-btn cp-btn-primary cp-btn-lg" onClick={onCta}>
        See pricing
      </button>
    </div>
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
        * page's FAQ and its own footer each repeated. The non-affiliation
        * sentence joined this same block when the hero minigame started
        * rendering a PrizePicks-styled card — one footer line, said once, per
        * the disclaimer-placement rule in CLAUDE.md. */}
      <p className="lp-foot-disc">
        CoreProp is an analytics tool and does not accept wagers. Not affiliated
        with, endorsed by, or sponsored by PrizePicks. 21+ where applicable. If
        you or someone you know has a gambling problem, call 1-800-GAMBLER.
      </p>
    </footer>
  );
}

Object.assign(window, { Landing });
