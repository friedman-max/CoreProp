/**
 * CoreProp — PrizePicks DOM adapter.
 *
 * Every assumption about PrizePicks' markup lives in THIS file and nowhere
 * else, so a PP redesign is a one-file fix rather than an archaeology dig
 * through the staging logic.
 *
 * The selectors below were read directly out of PrizePicks' production bundle
 * (https://app.prizepicks.com/app.<hash>.js, verified 2026-08-01). PP ships
 * stable `id` attributes for exactly the elements we need. Note that these ids
 * are DUPLICATED across every card — that is invalid HTML, so `getElementById`
 * returns only the first one and is useless here. Always use the attribute
 * selector form with querySelectorAll, scoped to a card element.
 */

const PP = {
  CARD:        'li[id="test-projection-li"]',
  PLAYER_NAME: '[id="test-player-name"]',
  TEAM_POS:    '[id="test-team-position"]',
  MORE_BTN:    '[id="test-more"]',
  LESS_BTN:    '[id="test-less"]',
  SWAP_BTN:    '[id="test-projection-swap"]',
  SEARCH_WRAP: '#pp-search-bar',
  // MUI merges inputProps.className onto the real <input>, but don't rely on
  // that alone — aria-label and placeholder are set in the same props object.
  SEARCH_INPUT: '#pp-search-bar input.search-input, #pp-search-bar input[aria-label="search"], ' +
                '#pp-search-bar input[placeholder="Search"]',
  // The toggle that REVEALS the search bar. It is not inside #pp-search-bar —
  // it lives in the stat-navigation strip, and is rendered twice (a sticky
  // mobile copy and a desktop copy), only one of which is visible.
  SEARCH_TOGGLE_BOX: '.pp-search-filter-desktop, .pp-search-filter',
  // PP marks a staged pick by swapping the button's background utility class.
  SELECTED_CLASS: 'bg-atlien-100',

  // ── Entry form (stake + submit) — AUTO-SUBMIT ONLY ────────────────────────
  // ⚠️ Unlike the selectors above, these were NOT verified against the PP
  // production bundle (auto-submit is new). They are best-effort id guesses
  // with generic fallbacks in the pp*() helpers below. If auto-submit silently
  // fails ("could not find the entry field / submit button"), inspect the live
  // entry rail on app.prizepicks.com and correct THESE strings — that is the
  // one-file fix.
  ENTRY_AMOUNT: '[id="test-entry-input"], [id="test-entry-amount"], input[name="entryAmount"], ' +
                'input[inputmode="decimal"], input[inputmode="numeric"], input[type="number"]',
  SUBMIT_BTN:   '[id="test-submit"], [id="test-entry-submit"], [id="test-place-entry"]',
  POWER_TAB:    '[id="test-power-play"], [id="test-power"]',
  FLEX_TAB:     '[id="test-flex-play"], [id="test-flex"]',
};

// DataDome interstitial markers. When PP challenges the tab there is no board
// at all, and every downstream "couldn't find the card" message is a lie about
// what actually went wrong.
const DATADOME_MARKERS = ['#cmsg', 'iframe[src*="captcha-delivery.com"]'];

function ppIsChallenged() {
  if (DATADOME_MARKERS.some(sel => document.querySelector(sel))) return true;
  return [...document.querySelectorAll('script')].some(s =>
    (s.textContent || '').includes('geo.captcha-delivery.com'));
}

/** Visible in the layout (not display:none and not zero-sized). */
function ppVisible(el) {
  return !!el && el.offsetParent !== null && el.getBoundingClientRect().width > 0;
}

/**
 * The search input, only if it is actually usable.
 *
 * On the v2 board #pp-search-bar is rendered with Tailwind's `!hidden` until
 * the user starts searching (`isHidden: !!v2 && !isSearching` in PP's source),
 * so the input exists in the DOM while being impossible to type into.
 */
function ppSearchInput() {
  const el = document.querySelector(PP.SEARCH_INPUT);
  return ppVisible(el) ? el : null;
}

/**
 * The magnifier button that reveals the search bar.
 *
 * This is the thing that was missing: the search bar cannot open itself. PP
 * hides it until redux `isSearching` is true, and the only thing that flips
 * that is this button's onClick — so with no click, the input stays hidden
 * forever and every player search silently no-ops.
 *
 * Identifying it is fiddly because its sibling (the games filter) has a
 * byte-identical className, `stat stat-rounded relative min-w-0 text-center`.
 * The separators that do hold: the search icon renders at 22x22 and the
 * filter's at 19x19, and the search toggle is always rendered first. The
 * filter is also feature-flagged per league and frequently absent entirely.
 */
function ppFindSearchToggle() {
  for (const box of document.querySelectorAll(PP.SEARCH_TOGGLE_BOX)) {
    if (!ppVisible(box)) continue;                     // wrong breakpoint's copy
    const btns = [...box.querySelectorAll('button')].filter(ppVisible);
    if (!btns.length) continue;
    const bySize = btns.find(b => b.querySelector('svg[width="22"]'));
    if (bySize) return bySize;
    // Fall back to DOM order: the search toggle always renders before the filter.
    const iconOnly = btns.filter(b => !(b.innerText || '').trim() && b.querySelector('svg'));
    return iconOnly[0] || btns[0];
  }
  return null;
}

function ppLooksSignedOut() {
  if (/\/login\b/.test(location.pathname)) return true;
  return !!document.querySelector('input[type="password"]');
}

// ---------------------------------------------------------------------------
// Text normalisation
// ---------------------------------------------------------------------------

/** Canonical form for comparing stat labels: "Pts+Rebs" === "pts + rebs". */
function canonStat(s) {
  return (s || '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

/** Canonical form for player names — keeps word structure, drops punctuation. */
function canonName(s) {
  return (s || '')
    .toLowerCase()
    .replace(/[‘’]/g, "'")
    .replace(/[^a-z0-9' ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Is this element visually struck through? PrizePicks flash-sale cards render
 * BOTH the original line and the discounted one, with the original struck out —
 * and the struck-out number is exactly the line_score our scraper captured. If
 * we match on it we stage a bet at a line the user never agreed to, and it
 * looks like success. So struck-through numbers are never eligible.
 */
function isStruckThrough(el) {
  for (let n = el; n && n !== document.body; n = n.parentElement) {
    const cls = String(n.className && n.className.baseVal || n.className || '');
    if (/line-through/.test(cls)) return true;
    try {
      if (/line-through/.test(getComputedStyle(n).textDecorationLine || '')) return true;
    } catch (e) { /* detached node */ }
  }
  return false;
}

/**
 * The line-score element and the row that holds it.
 *
 * PP renders these together (bundle module 70031):
 *
 *   <div class="body-sm items-last-baseline flex gap-1">
 *     <div>Cur. <span>12</span></div>            // live games only
 *     <div class="heading-md">
 *       <span class="... line-through">27.5</span>   // flash-sale ORIGINAL
 *       <span>24.5</span>                            // the bettable line
 *     </div>
 *     <span class="...">[leagueIcon] Ks</span>       // the stat label
 *   </div>
 *
 * Anchoring on `.heading-md` and walking to its sibling is what makes both the
 * line and the label exact, rather than inferred from surrounding text.
 */
function cardStatRow(card) {
  const lineEl = card.querySelector('.heading-md');
  return lineEl && lineEl.parentElement ? { lineEl, row: lineEl.parentElement } : null;
}

/**
 * Candidate line scores on the card.
 *
 * Struck-through values are excluded: on a flash-sale card the struck-out
 * number is exactly the `line_score` our scraper captured, while the bettable
 * one is `flash_sale_line_score`. Matching the struck-out value would stage a
 * bet at a line the user never agreed to and report it as success.
 */
function cardLineCandidates(card) {
  const r = cardStatRow(card);
  if (r) {
    const out = [];
    for (const el of r.lineEl.querySelectorAll('*')) {
      if (el.children.length) continue;
      const txt = (el.textContent || '').trim();
      if (!/^\d{1,3}(\.\d+)?$/.test(txt)) continue;
      if (isStruckThrough(el)) continue;
      out.push({ el, value: parseFloat(txt) });
    }
    // PP may render the number as a bare text node with no element wrapper.
    if (!out.length) {
      const m = (r.lineEl.textContent || '').trim().match(/(\d{1,3}(?:\.\d+)?)\s*$/);
      if (m) out.push({ el: r.lineEl, value: parseFloat(m[1]) });
    }
    if (out.length) return out;
  }
  return cardLineCandidatesLoose(card);
}

/** Structure-free fallback, used only if `.heading-md` disappears in a redesign. */
function cardLineCandidatesLoose(card) {
  const out = [];
  for (const el of card.querySelectorAll('*')) {
    if (el.children.length) continue;
    const txt = (el.textContent || '').trim();
    if (!/^\d{1,3}(\.\d+)?$/.test(txt)) continue;
    if (el.closest('button, [role="button"]')) continue;
    if (isStruckThrough(el)) continue;
    out.push({ el, value: parseFloat(txt) });
  }
  return out;
}

/**
 * The card's stat label, e.g. "Ks" or "Pts+Rebs+Asts".
 *
 * Read from the element immediately after the line score. Must use textContent
 * of that container, never per-leaf text: PP splits combo labels on "+" into
 * separate fragments for word-wrapping, so the leaves of "Pts+Rebs+Asts" are
 * "Pts", "+Rebs", "+Asts".
 *
 * The previous "longest text leaf on the card" heuristic silently broke almost
 * every match — a card's start time ("7:05 PM", 7 chars) outranks a real label
 * like "Ks" (2 chars), so the stat compared against the leg was the game time
 * and the card was skipped despite being visible on screen.
 */
function cardStatLabel(card) {
  const r = cardStatRow(card);
  if (r) {
    for (let el = r.lineEl.nextElementSibling; el; el = el.nextElementSibling) {
      const txt = (el.textContent || '').trim();
      if (txt) return txt;
    }
  }
  return cardStatLabelLoose(card);
}

/** Structure-free fallback for the stat label. */
function cardStatLabelLoose(card) {
  const player = card.querySelector(PP.PLAYER_NAME);
  const team = card.querySelector(PP.TEAM_POS);
  let best = '';
  for (const el of card.querySelectorAll('*')) {
    if (el.children.length) continue;
    if (player && player.contains(el)) continue;
    if (team && team.contains(el)) continue;
    if (el.closest('button, [role="button"], time')) continue;
    const txt = (el.textContent || '').trim();
    if (!txt || txt.length > 40) continue;
    if (!/[a-z]{2}/i.test(txt)) continue;
    if (/^(more|less|over|under|higher|lower|cur\.?|vs|@)$/i.test(txt)) continue;
    if (/\d{1,2}:\d{2}/.test(txt)) continue;        // start time, never a stat
    if (txt.length > best.length) best = txt;
  }
  return best;
}

/**
 * Read one card into a plain object. Returns null when the element does not
 * carry the structure of a real card, which is how we reject the row/grid
 * containers the old heuristic used to pick up (those matched a player name
 * from card 1 and a line from card 3 — a Frankenstein match that staged a
 * completely different player's pick and reported success).
 */
function readCard(card) {
  const nameEl = card.querySelector(PP.PLAYER_NAME);
  const more = card.querySelector(PP.MORE_BTN);
  const less = card.querySelector(PP.LESS_BTN);
  if (!nameEl) return null;
  // Exactly one of each side button — more than one means this is a container
  // holding several cards, not a card.
  if (card.querySelectorAll(PP.MORE_BTN).length > 1) return null;
  if (card.querySelectorAll(PP.PLAYER_NAME).length > 1) return null;

  return {
    el: card,
    player: (nameEl.textContent || '').trim(),
    team: (card.querySelector(PP.TEAM_POS)?.textContent || '').trim(),
    stat: cardStatLabel(card),
    lines: cardLineCandidates(card),
    moreBtn: more,
    lessBtn: less,
    swapBtn: card.querySelector(PP.SWAP_BTN),
  };
}

/**
 * All cards currently on the board.
 *
 * Falls back to a structural scan if the test-id disappears in a PP redesign:
 * any element holding exactly one #test-more and one #test-less. If BOTH paths
 * come back empty the caller reports "board not loaded" rather than silently
 * treating an empty board as "no match found".
 */
function findCards() {
  let els = [...document.querySelectorAll(PP.CARD)];
  if (!els.length) {
    const seen = new Set();
    for (const btn of document.querySelectorAll(PP.MORE_BTN)) {
      for (let p = btn.parentElement, i = 0; p && i < 8; p = p.parentElement, i++) {
        if (p.querySelector(PP.PLAYER_NAME) && p.querySelectorAll(PP.MORE_BTN).length === 1) {
          seen.add(p);
          break;
        }
      }
    }
    els = [...seen];
  }
  return els.map(readCard).filter(Boolean);
}

/**
 * The payout multiplier PrizePicks prints inside a side button ("More 1.25x"),
 * or null on a plain standard button.
 *
 * This is how goblins and demons are told apart from standard lines. It matters
 * for more than cosmetics: CoreProp's slip EV is computed against the standard
 * Power/Flex payout table, so staging a goblin at a friendlier number quietly
 * places a bet whose payout structure the model never priced.
 */
function cardMultiplier(info, side) {
  const btn = side === 'over' ? info.moreBtn : info.lessBtn;
  if (!btn) return null;
  const m = (btn.innerText || '').match(/(\d+(?:\.\d+)?)\s*x/i);
  return m ? parseFloat(m[1]) : null;
}

/** A standard (non-goblin, non-demon) card: neither button carries a multiplier. */
function cardIsStandard(info) {
  return cardMultiplier(info, 'over') === null && cardMultiplier(info, 'under') === null;
}

/**
 * Identity key for de-duplicating cards across scroll passes. The board is
 * virtualized, so the same logical card is a different DOM node each time it
 * scrolls back into view — comparing elements would count it repeatedly and
 * make "did this pass reveal anything new?" always answer yes.
 */
function cardKey(info) {
  return [canonName(info.player), canonStat(info.stat),
          info.lines.map(l => l.value).sort((a, b) => a - b).join('/')].join('|');
}

/**
 * Cycle a card's alternate-line control one step and report the line values now
 * showing. PrizePicks hides alternate lines for the same player+prop behind
 * this swap button, so a line that looks "gone" is frequently one click away.
 * Returns null when the card has no swap control.
 */
async function swapCardLine(info, waitMs = 700) {
  if (!info.swapBtn || !info.swapBtn.isConnected) return null;
  const before = info.lines.map(l => l.value).join(',');
  const player = canonName(info.player), stat = canonStat(info.stat);
  info.swapBtn.click();

  const deadline = Date.now() + waitMs;
  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 100));
    // Re-find in the LIVE DOM rather than re-reading the element we captured.
    // Swapping makes React re-render the card, which detaches the old node —
    // and a detached node keeps reporting the OLD line forever, so every swap
    // looks like a no-op and alternate lines appear not to exist.
    const fresh = info.el.isConnected
      ? readCard(info.el)
      : findCards().find(c => canonName(c.player) === player && canonStat(c.stat) === stat);
    if (fresh && fresh.lines.map(l => l.value).join(',') !== before) return fresh;
  }
  return null;
}

/** Is this side already staged? PP's More/Less are TOGGLES — clicking an
 *  already-selected pick REMOVES it, which would silently shrink the slip. */
function isSideSelected(info, side) {
  const btn = side === 'over' ? info.moreBtn : info.lessBtn;
  if (!btn) return false;
  const cls = String(btn.className && btn.className.baseVal || btn.className || '');
  if (cls.includes(PP.SELECTED_CLASS)) return true;
  return btn.getAttribute('aria-pressed') === 'true' ||
         btn.getAttribute('aria-selected') === 'true';
}

// ── Entry-form finders (auto-submit) ─────────────────────────────────────────
// Each tries the (unverified) id selectors first, then a generic fallback, so a
// PP id change degrades to the fallback instead of failing outright. Auto-submit
// refuses to proceed if any of these return null (it never submits blind).

function ppStakeInput() {
  for (const sel of PP.ENTRY_AMOUNT.split(',')) {
    const el = document.querySelector(sel.trim());
    if (el && ppVisible(el)) return el;
  }
  const inputs = [...document.querySelectorAll('input')].filter(ppVisible);
  return inputs.find(i => /amount|entry|wager|stake/i.test(
    (i.getAttribute('aria-label') || '') + (i.getAttribute('placeholder') || '') + (i.name || ''))) || null;
}

function ppSubmitButton() {
  for (const sel of PP.SUBMIT_BTN.split(',')) {
    const el = document.querySelector(sel.trim());
    if (el && ppVisible(el) && !el.disabled) return el;
  }
  const btns = [...document.querySelectorAll('button')].filter(b => ppVisible(b) && !b.disabled);
  return btns.find(b => /^(submit|place entry|place|enter)\b/i.test((b.innerText || '').trim())) || null;
}

function ppEntryTypeButton(type) {
  const t = String(type || '').toLowerCase();
  const sel = t === 'flex' ? PP.FLEX_TAB : PP.POWER_TAB;
  for (const s of sel.split(',')) {
    const el = document.querySelector(s.trim());
    if (el && ppVisible(el)) return el;
  }
  const rx = t === 'flex' ? /flex/i : /power/i;
  const btns = [...document.querySelectorAll('button, [role="tab"], [role="button"]')].filter(ppVisible);
  return btns.find(b => rx.test((b.innerText || '').trim())) || null;
}

function ppSubmitConfirmed() {
  // Best-effort success signal: a confirmation toast/modal, or the entry field
  // clearing back to empty after a successful submit.
  if (/entry submitted|good luck|success|your entry is in/i.test(document.body.innerText || '')) return true;
  const inp = ppStakeInput();
  return !!(inp && (inp.value === '' || inp.value === '0'));
}

Object.assign(self, {
  PP, ppIsChallenged, ppLooksSignedOut,
  ppVisible, ppSearchInput, ppFindSearchToggle,
  canonStat, canonName, isStruckThrough,
  cardLineCandidates, cardStatLabel, readCard, findCards, isSideSelected,
  cardMultiplier, cardIsStandard, cardKey, swapCardLine,
  ppStakeInput, ppSubmitButton, ppEntryTypeButton, ppSubmitConfirmed,
});
