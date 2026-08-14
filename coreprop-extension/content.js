/**
 * CoreProp — PrizePicks Slip Builder (content script)
 *
 * Stages the legs of a CoreProp slip on the PrizePicks board so the user only
 * has to enter a stake and submit. It NEVER submits a wager and never touches
 * the entry amount — that is deliberate and should stay that way.
 *
 * Design rule, learned the hard way: a leg counts as staged only when PP's own
 * UI says it is staged. Every "success" here is read back off the board, never
 * inferred from "we dispatched a click". The previous version returned success
 * from a path that had clicked nothing, printed "All 6 legs added", and then
 * DELETEd the slip from the server so the user could not even retry.
 *
 * DOM knowledge lives in pp-dom.js. This file is flow control.
 */

const DEBUG = true;
function log(...a) { if (DEBUG) console.log('[CoreProp]', ...a); }

const VERSION = chrome.runtime.getManifest().version;

// ---------------------------------------------------------------------------
// Slip token
//
// The website hands us the token in the URL fragment (never the query string:
// a query is sent to PrizePicks' servers and lands in their access logs, and
// that token alone authorises reading and deleting the slip). Fragments are
// never transmitted. Captured synchronously at load because PP's SPA router
// rewrites the URL via replaceState before any deferred read.
// ---------------------------------------------------------------------------

const URL_TOKEN = (() => {
  try {
    const frag = new URLSearchParams((location.hash || '').replace(/^#/, ''));
    // Query form kept for one release so links opened by an older build still work.
    return frag.get('cp_slip') || new URLSearchParams(location.search).get('cp_slip') || '';
  } catch (e) { return ''; }
})();

const TOKEN_KEY = 'coreprop:slip-token';

/**
 * Resolve the token, preferring the URL but falling back to session storage.
 *
 * This is what survives the login bounce. A signed-out user opening the board
 * gets navigated to /login, which drops the fragment; the content script then
 * re-injects into a fresh document with no token, and the old build gave up
 * permanently at that point ("No pending slip") while the slip sat on the
 * server until it expired. Now the token is stashed on first sight and read
 * back after the redirect.
 */
async function getToken() {
  if (URL_TOKEN) {
    try { await chrome.storage.session.set({ [TOKEN_KEY]: URL_TOKEN }); } catch (e) {}
    return URL_TOKEN;
  }
  try {
    const got = await chrome.storage.session.get(TOKEN_KEY);
    return got[TOKEN_KEY] || '';
  } catch (e) { return ''; }
}

async function forgetToken() {
  try { await chrome.storage.session.remove(TOKEN_KEY); } catch (e) {}
}

// Strip the token from the visible URL as soon as we have it, so it isn't
// sitting in the address bar or in PP's history entries.
if (URL_TOKEN && location.hash) {
  try { history.replaceState(null, '', location.pathname + location.search); } catch (e) {}
}

// ---------------------------------------------------------------------------
// Status panel — docked TOP-RIGHT.
//
// PrizePicks' entry/bet-slip rail also lives on the right, so the panel can
// overlap the stake field and submit button once staging finishes. Hence the
// collapse toggle: one click shrinks it to a title-bar pill that keeps the
// summary visible without covering anything.
// ---------------------------------------------------------------------------

let panel = null, panelLegList = null;

function ensurePanel() {
  if (panel && panel.isConnected) return panel;

  panel = document.createElement('div');
  panel.id = 'coreprop-panel';
  Object.assign(panel.style, {
    position: 'fixed', top: '80px', right: '16px', zIndex: '2147483000',
    background: '#0f172a', border: '1px solid #1e3a5f', borderRadius: '12px',
    padding: '14px 16px', minWidth: '280px', maxWidth: '340px',
    fontFamily: 'system-ui, -apple-system, sans-serif', fontSize: '13px',
    color: '#e2e8f0', boxShadow: '0 8px 32px rgba(0,0,0,.6)',
  });
  panel.innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <span style="font-weight:700;font-size:14px;color:#3da9f0;">CoreProp</span>
      <span style="display:flex;gap:6px;align-items:center;">
        <button id="coreprop-retry" style="display:none;background:#1e3a5f;border:none;color:#e2e8f0;font-size:11px;cursor:pointer;padding:3px 8px;border-radius:6px;">Retry</button>
        <button id="coreprop-min" title="Collapse" style="background:transparent;border:none;color:#64748b;font-size:15px;cursor:pointer;padding:0 4px;line-height:1;">&minus;</button>
        <button id="coreprop-close" title="Close" style="background:transparent;border:none;color:#64748b;font-size:18px;cursor:pointer;padding:0 4px;line-height:1;">&times;</button>
      </span>
    </div>
    <div id="coreprop-body">
      <div id="coreprop-status" style="font-size:12px;color:#94a3b8;margin:10px 0;">Starting…</div>
      <div id="coreprop-banner" style="display:none;margin-bottom:10px;padding:8px 10px;border-radius:8px;font-weight:700;font-size:12px;text-align:center;"></div>
      <div id="coreprop-leg-list" style="max-height:50vh;overflow-y:auto;"></div>
      <div id="coreprop-footer" style="margin-top:10px;font-size:11px;color:#64748b;text-align:center;line-height:1.5;"></div>
    </div>`;
  document.body.appendChild(panel);

  panel.querySelector('#coreprop-close').addEventListener('click', () => panel.remove());
  panel.querySelector('#coreprop-min').addEventListener('click', (e) => {
    const body = panel.querySelector('#coreprop-body');
    const hidden = body.style.display === 'none';
    body.style.display = hidden ? 'block' : 'none';
    e.target.innerHTML = hidden ? '&minus;' : '&plus;';
    e.target.title = hidden ? 'Collapse' : 'Expand';
  });

  panelLegList = panel.querySelector('#coreprop-leg-list');
  return panel;
}

function setStatus(text, color = '#94a3b8') {
  ensurePanel();
  const el = panel.querySelector('#coreprop-status');
  if (el) { el.textContent = text; el.style.color = color; }
}

function setFooter(html, color = '#64748b') {
  ensurePanel();
  const el = panel.querySelector('#coreprop-footer');
  if (el) { el.innerHTML = html; el.style.color = color; }
}

/** Big persistent banner — used for the Power/Flex reminder, which the user
 *  MUST act on before submitting or the payout structure won't be what
 *  CoreProp modelled. */
function setBanner(text, bg = '#7c2d12', fg = '#fed7aa') {
  ensurePanel();
  const el = panel.querySelector('#coreprop-banner');
  if (!el) return;
  if (!text) { el.style.display = 'none'; return; }
  el.style.display = 'block';
  el.style.background = bg;
  el.style.color = fg;
  el.textContent = text;
}

function showRetry(handler) {
  ensurePanel();
  const btn = panel.querySelector('#coreprop-retry');
  if (!btn) return;
  btn.style.display = handler ? 'inline-block' : 'none';
  btn.onclick = handler || null;
}

function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s == null ? '' : String(s);
  return d.innerHTML;
}

function renderLegs(legs) {
  ensurePanel();
  panelLegList.innerHTML = '';
  legs.forEach((leg, i) => {
    const row = document.createElement('div');
    row.id = `coreprop-leg-${i}`;
    Object.assign(row.style, {
      display: 'flex', alignItems: 'center', gap: '8px',
      padding: '6px 0', borderBottom: '1px solid #1e293b',
    });
    row.innerHTML = `
      <span class="coreprop-status-icon" style="font-size:14px;min-width:18px;">⏳</span>
      <div style="flex:1;min-width:0;">
        <div style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${escapeHtml(leg.player)}</div>
        <div style="font-size:11px;color:#94a3b8;">${escapeHtml(leg.prop)} ${escapeHtml((leg.side || '').toUpperCase())} ${escapeHtml(leg.line)}</div>
        <div class="coreprop-leg-note" style="font-size:10px;color:#f87171;"></div>
      </div>`;
    panelLegList.appendChild(row);
  });
}

function setLegStatus(index, status, note = '') {
  const row = document.getElementById(`coreprop-leg-${index}`);
  if (!row) return;
  const icon = row.querySelector('.coreprop-status-icon');
  const noteEl = row.querySelector('.coreprop-leg-note');
  if (icon) {
    if (status === 'searching') icon.textContent = '🔍';
    else if (status === 'done') { icon.textContent = '✅'; row.style.opacity = '.6'; }
    else if (status === 'skipped') { icon.textContent = '⏭️'; }
    else if (status === 'error') { icon.textContent = '❌'; }
  }
  if (noteEl) {
    noteEl.textContent = note || '';
    noteEl.style.color = status === 'done' ? '#64748b' : '#f87171';
  }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

const sleep = ms => new Promise(r => setTimeout(r, ms));

/** Poll until `fn()` is truthy. Returns the value, or null on timeout. */
async function until(fn, timeoutMs, stepMs = 150) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = fn();
    if (v) return v;
    await sleep(stepMs);
  }
  return null;
}

/** Native click that React's synthetic event system actually honours. */
function nativeClick(el) {
  const r = el.getBoundingClientRect();
  const o = { bubbles: true, cancelable: true, view: window,
              clientX: r.left + r.width / 2, clientY: r.top + r.height / 2, button: 0 };
  el.dispatchEvent(new PointerEvent('pointerdown', o));
  el.dispatchEvent(new MouseEvent('mousedown', o));
  el.dispatchEvent(new PointerEvent('pointerup', o));
  el.dispatchEvent(new MouseEvent('mouseup', o));
  el.dispatchEvent(new MouseEvent('click', o));
}

/**
 * Scroll an element into view and wait until it is actually there.
 *
 * behavior:'auto' on purpose — smooth scrolling is distance-dependent and
 * routinely outruns a fixed sleep, and on a virtualized board the card can be
 * recycled mid-scroll, leaving a detached node whose rect is all zeros. Clicks
 * dispatched at 0,0 hit nothing and the old code counted that as success.
 */
async function scrollToEl(el) {
  el.scrollIntoView({ behavior: 'auto', block: 'center' });
  await until(() => {
    if (!el.isConnected) return false;
    const r = el.getBoundingClientRect();
    return r.height > 0 && r.top >= 0 && r.bottom <= window.innerHeight + 1;
  }, 1200);
}

/** The scrollable ancestor holding the board — PP nests the grid inside an
 *  overflow container, so window.scrollBy() is frequently a no-op. */
function scrollContainerFor(el) {
  for (let n = el?.parentElement; n && n !== document.body; n = n.parentElement) {
    const oy = getComputedStyle(n).overflowY;
    if ((oy === 'auto' || oy === 'scroll') && n.scrollHeight > n.clientHeight + 10) return n;
  }
  return null;
}

/** Page down through the board. Returns false when nothing moved. */
async function scrollBoard(anchorEl, px = 600) {
  const box = scrollContainerFor(anchorEl);
  if (box) {
    const before = box.scrollTop;
    box.scrollTop += px;
    await sleep(500);
    return box.scrollTop !== before;
  }
  const before = window.scrollY;
  window.scrollBy({ top: px, behavior: 'auto' });
  await sleep(500);
  return window.scrollY !== before;
}

// ---------------------------------------------------------------------------
// Backend messaging (via the service worker — MV3 content scripts inherit the
// page origin and can't reliably fetch cross-origin even with host_permissions)
// ---------------------------------------------------------------------------

function sendToBackground(message) {
  return new Promise(resolve => {
    try {
      chrome.runtime.sendMessage(message, resp => {
        if (chrome.runtime.lastError) {
          resolve({ ok: false, status: 0, error: chrome.runtime.lastError.message });
        } else {
          resolve(resp || { ok: false, status: 0, error: 'no response from background' });
        }
      });
    } catch (err) {
      resolve({ ok: false, status: 0, error: err.message });
    }
  });
}

/**
 * Fetch the pending slip, retrying the case that can actually recover.
 *
 * The old loop had this backwards: it retried an empty-but-valid response five
 * times (a worker recycle wipes the slip permanently, so that can never
 * succeed) and returned immediately on a hard network error — which is the
 * cold-start case, where CoreProp's free-tier instance can take ~50s to wake
 * and where its own error text told the user "the server may be waking up".
 */
async function fetchPendingSlip(token, onProgress) {
  let lastErr = null;
  const backoff = [0, 1500, 3000, 5000, 8000, 12000, 20000];
  for (let i = 0; i < backoff.length; i++) {
    if (backoff[i]) {
      onProgress?.(`Waking up CoreProp… (${i}/${backoff.length - 1})`);
      await sleep(backoff[i]);
    }
    const resp = await sendToBackground({ type: 'coreprop:get-pending-slip', token });
    if (resp.ok) {
      const d = resp.data;
      if (d && Array.isArray(d.legs) && d.legs.length) return { slip: d, error: null };
      // A valid "no slip here" answer. Retry briefly (the POST and our GET can
      // race), then accept it.
      if (i >= 2) return { slip: null, error: null };
      continue;
    }
    lastErr = resp.error || `HTTP ${resp.status}`;
  }
  return { slip: null, error: lastErr };
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

// PrizePicks debounces the search field by 500ms before it dispatches the
// query, so the board cannot possibly refilter sooner than that.
const SEARCH_DEBOUNCE_MS = 500;

/**
 * Reveal PP's search input and return it.
 *
 * The input does NOT open itself. On the v2 board PP renders #pp-search-bar
 * with Tailwind's `!hidden` and only removes it when redux `isSearching` goes
 * true — which nothing but the magnifier button's onClick does. Focusing a
 * display:none input is a no-op, so without clicking that button first the
 * search bar stays invisible and every player lookup silently finds nothing.
 * That was the blocker.
 */
async function openSearch() {
  let input = ppSearchInput();
  if (input) { input.focus(); return input; }

  const toggle = ppFindSearchToggle();
  if (!toggle) {
    log('search toggle not found — is the stat-navigation bar rendered?');
    return null;
  }

  await scrollToEl(toggle);
  nativeClick(toggle);

  input = await until(ppSearchInput, 2500);
  if (!input) {
    // Clicking the toggle also scrolls the page; give the bar one more beat.
    await sleep(600);
    input = ppSearchInput();
  }
  if (input) input.focus();
  else log('clicked the search toggle but the input never became visible');
  return input;
}

/** React caches the DOM value, so a bare `input.value = x` is ignored. */
function setReactInputValue(input, value) {
  const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
  setter.call(input, value);
  input.dispatchEvent(new Event('input', { bubbles: true }));
  input.dispatchEvent(new Event('change', { bubbles: true }));
}

/**
 * Type a player's name into PP's search and wait for the board to refilter.
 *
 * Dispatches a real key sequence after setting the value: search UIs commonly
 * debounce on keyup or gate on `e.key`, and neither fires from a bare `input`
 * event. Success is measured as "the set of cards on the board changed", not
 * "some card contains the name" — the latter is already true whenever the
 * player happened to be visible on the unfiltered board, which made the old
 * check pass without the search having done anything.
 */
async function searchForPlayer(name) {
  const input = await openSearch();
  if (!input) return false;

  const before = document.querySelectorAll(PP.CARD).length;
  input.focus();
  setReactInputValue(input, '');
  await sleep(120);
  setReactInputValue(input, name);

  const last = name.slice(-1);
  for (const type of ['keydown', 'keyup']) {
    input.dispatchEvent(new KeyboardEvent(type, { key: last, code: 'Key' + last.toUpperCase(), bubbles: true }));
  }

  // Nothing can happen until PP's 500ms debounce elapses; polling before that
  // just measures the unfiltered board.
  await sleep(SEARCH_DEBOUNCE_MS + 200);

  const changed = await until(() => document.querySelectorAll(PP.CARD).length !== before, 4000, 200);
  await sleep(400);   // let the filtered render settle
  return changed !== null;
}

async function clearSearch() {
  const input = ppSearchInput();
  if (!input) return;
  input.focus();
  setReactInputValue(input, '');
  await sleep(SEARCH_DEBOUNCE_MS + 150);   // let the board come back
}

// ---------------------------------------------------------------------------
// League tabs
// ---------------------------------------------------------------------------

/**
 * Find the board's league tab by label. PP's live board carries ~28 leagues and
 * the label is PP's own league name, so we match the leg's league string
 * directly rather than against a hardcoded five-entry table (which made every
 * NFL and SOCCER leg unreachable: no tab was ever found, and the code then
 * searched a board that was still showing the wrong sport).
 */
function findLeagueTab(league) {
  const want = String(league || '').toUpperCase().trim();
  if (!want) return null;
  const alts = [want, want.replace(/^NCAA/, 'C'), want === 'NCAAB' ? 'CBB' : want];
  const sels = ['[role="tab"]', 'button', 'a[href]', '[class*="tab" i]:not(div)'];
  for (const sel of sels) {
    for (const el of document.querySelectorAll(sel)) {
      const txt = (el.innerText || el.textContent || '').trim().toUpperCase();
      if (!txt || txt.length > 30) continue;
      if (alts.some(w => txt === w || txt.startsWith(w + ' ') || txt.startsWith(w + '\n'))) return el;
    }
  }
  return null;
}

/**
 * Switch the board to `league`.
 *
 * Confirmation is "the visible card set changed", not an aria-selected probe.
 * PP builds with hashed CSS-module class names and no ARIA on the tabs, so the
 * old active-tab detection could never return true — every switch burned three
 * full retries (~9s) and then re-ran per leg, spending well over a minute of
 * dead time on a five-leg slip while also resetting the search filter each time.
 */
async function switchToLeague(league) {
  const tab = findLeagueTab(league);
  if (!tab) return { ok: false, reason: `PrizePicks has no ${league} tab on the board right now` };

  const before = document.querySelectorAll(PP.CARD).length;
  await scrollToEl(tab);
  nativeClick(tab);
  await until(() => document.querySelectorAll(PP.CARD).length !== before, 4000, 200);
  await sleep(600);
  return { ok: true };
}

// ---------------------------------------------------------------------------
// Matching
//
// leg.prop is PrizePicks' OWN stat_type string, verbatim — web/app.py sets
// prop_type=m.pp.stat_type straight off the scraped PP line and nothing renames
// it on the way to us. So the comparison against a card's stat label is close
// to an identity check, and the old 60-entry hand-written synonym table was
// only ever bridging PP's API string to PP's abbreviated card label.
//
// That table had 23 blind spots that fell through to a lenient any-word
// fallback, where a leg for "3-PT Made" would match a "Free Throws Made" card
// on the shared word "made". Anything not resolved by canonical equality is now
// reported as ambiguous rather than guessed at.
// ---------------------------------------------------------------------------

// CoreProp stores PrizePicks' `stat_type`; the board prints
// `stat_display_name || stat_type` (PP bundle module 70031). Where those differ,
// a leg can only be matched to a card through this table — "Pitcher Strikeouts"
// is printed as "Ks", "Total Bases" as "TB".
//
// Generated from 9,063 live projections on 2026-08-02 (95 stat types, 19 whose
// printed label differs). Regenerate with:
//   python3 tests/extension/regen-stat-aliases.py
// Do not hand-edit: guessing these was why abbreviated props silently failed.
const STAT_ALIASES = {
  '3ptmade':              ['3ptm'],           // 3-PT Made
  '3ptmadecombo':         ['3ptm'],           // 3-PT Made (Combo)
  'assistscombo':         ['assists'],        // Assists (Combo)
  'astspergame':          ['apg'],            // Asts Per Game
  'hitterstrikeouts':     ['hitterks'],       // Hitter Strikeouts
  'outfieldfantasyscore': ['fantasyscore'],   // Outfield Fantasy Score
  'pitcherfantasyscore':  ['pitcherfs'],      // Pitcher Fantasy Score
  'pitcherstrikeouts':    ['ks'],             // Pitcher Strikeouts
  'pitchingouts':         ['po'],             // Pitching Outs
  'pointscombo':          ['points'],         // Points (Combo)
  'pointspergame':        ['ppg'],            // Points Per Game
  'ptsrebsasts':          ['pra'],            // Pts+Rebs+Asts
  'reboundscombo':        ['rebounds'],       // Rebounds (Combo)
  'rebspergame':          ['rpg'],            // Rebs Per Game
  'receivingyards':       ['recyards'],       // Receiving Yards
  'receptions':           ['recs'],           // Receptions
  'shotsontarget':        ['sot'],            // Shots On Target
  'stolenbases':          ['sb'],             // Stolen Bases
  'totalbases':           ['tb'],             // Total Bases
};

/**
 * Does a card's printed stat label refer to the same market as the leg?
 *
 * Checked in both directions, since either side may be the abbreviation
 * depending on which name PrizePicks happened to store on the projection.
 */
function statMatches(cardStat, legProp) {
  let c = canonStat(cardStat), l = canonStat(legProp);
  if (!c || !l) return false;
  if (c === l) return true;
  if ((STAT_ALIASES[l] || []).includes(c)) return true;
  if ((STAT_ALIASES[c] || []).includes(l)) return true;
  // "(Combo)" variants print under the plain name; strip it and retry so a new
  // combo market works before anyone regenerates the table.
  const strip = s => s.replace(/combo$/, '');
  c = strip(c); l = strip(l);
  if (c === l) return true;
  return (STAT_ALIASES[l] || []).includes(c) || (STAT_ALIASES[c] || []).includes(l);
}

/**
 * Player match. Exact normalised equality first; the fallback still demands the
 * surname as a whole word.
 *
 * The old bigram similarity returned 0.9 for ANY substring relation, so a card
 * fragment like "LA" scored 0.9 against "LaMelo Ball" and cleared the 0.65
 * gate on no real name evidence at all.
 */
function playerMatches(cardPlayer, legPlayer) {
  const c = canonName(cardPlayer), l = canonName(legPlayer);
  if (!c || !l) return false;
  if (c === l) return true;
  const surname = l.split(' ').filter(Boolean).sort((a, b) => b.length - a.length)[0] || '';
  if (surname.length < 4) return false;
  const initial = l.split(' ')[0]?.[0] || '';
  const words = c.split(' ');
  return words.includes(surname) && (!initial || c.startsWith(initial) || words.length === 1);
}

function lineMatches(info, line) {
  const target = parseFloat(line);
  return info.lines.some(n => Math.abs(n.value - target) < 0.001);
}

// Per-leg time budget for the whole hunt (search + scroll + alt-line cycling).
const LEG_BUDGET_MS = 8000;
const MAX_SWAPS = 6;      // alternates to cycle through on one card
const SWAP_WAIT_MS = 400;

/**
 * Is `candidate` a strictly better number than the slip's line for this side?
 * Over wants a LOWER line, Under wants a HIGHER one — both are easier to hit.
 *
 * Bounded on purpose. "Better" is only meaningful for a line that MOVED; an
 * arbitrarily lower number is a different market, not a bargain. Without the
 * bound, a leg for Over 99.5 would treat a 26.5 card as an upgrade and stage a
 * bet bearing no relation to the one that was modelled. The tolerance scales
 * with the line so it fits both Points (~25) and Passing Yards (~250).
 */
function betterLineTolerance(legLine) {
  return Math.max(1.0, Math.abs(parseFloat(legLine)) * 0.05);
}

function isBetterLine(candidate, legLine, side) {
  const want = parseFloat(legLine);
  if (!Number.isFinite(candidate) || !Number.isFinite(want)) return false;
  if (Math.abs(candidate - want) > betterLineTolerance(want)) return false;
  return side === 'over' ? candidate < want : candidate > want;
}

/**
 * Cycle a card's alternate lines looking for the leg's exact number.
 *
 * PrizePicks hides alternates for the same player+prop behind a swap control,
 * so a line that isn't on the card face is very often present but not shown —
 * previously reported as "line moved" when it was actually one click away.
 *
 * Returns { card } on an exact hit, or { better } with the best strictly-better
 * STANDARD alternate seen along the way. Goblins/demons are never offered as a
 * substitute: their payout multiplier differs from the standard table CoreProp
 * priced the slip against, so swapping one in would change the bet's economics
 * without changing the number the user reads.
 */
async function cycleAlternates(info, leg, deadline) {
  let cur = info;
  let better = null;

  const consider = (c) => {
    if (!cardIsStandard(c)) return;
    for (const { value } of c.lines) {
      if (!isBetterLine(value, leg.line, leg.side)) continue;
      // Closest better line wins — least deviation from what was modelled.
      if (!better || Math.abs(value - parseFloat(leg.line)) < Math.abs(better.line - parseFloat(leg.line))) {
        better = { line: value, swaps: cur.__swaps || 0 };
      }
    }
  };

  consider(cur);

  for (let i = 0; i < MAX_SWAPS && Date.now() < deadline; i++) {
    const next = await swapCardLine(cur, SWAP_WAIT_MS);
    if (!next) break;                                  // no swap control, or it stopped changing
    next.__swaps = i + 1;
    cur = next;
    if (!statMatches(cur.stat, leg.prop)) break;       // swap moved us off this prop entirely
    // Same rule as the face check: only a STANDARD card is a valid substitute,
    // since a demon/goblin at the same number pays differently.
    if (lineMatches(cur, leg.line) && cardIsStandard(cur)) return { card: cur };
    consider(cur);
  }
  return { better };
}

/**
 * Hunt for a leg across the ENTIRE filtered result set.
 *
 * This is the fix for the bug that made the whole feature unreliable: the old
 * code called a one-shot selector against whatever happened to be rendered and
 * returned an error the instant the player was visible but the exact prop+line
 * wasn't in that first viewport. Since PrizePicks virtualizes the board, a card
 * four rows down reported "line moved" without a single scroll ever happening.
 *
 * Now: scroll until two consecutive passes reveal no new cards (or the budget
 * runs out), deduping by identity because a virtualized card is a new DOM node
 * every time it re-enters view. Only once the set is exhausted do we decide.
 */
async function huntLeg(leg, deadline) {
  const seen = new Set();
  const statsSeen = new Set();
  const linesSeen = new Set();
  let sawPlayer = false;
  let better = null;
  let nonStandardHit = null;   // exact line found, but only as a demon/goblin
  let dry = 0;

  while (Date.now() < deadline && dry < 2) {
    const board = findCards();
    const mine = board.filter(c => playerMatches(c.player, leg.player));
    if (mine.length) sawPlayer = true;

    let fresh = 0;
    for (const c of mine) {
      const k = cardKey(c);
      if (seen.has(k)) continue;
      seen.add(k);
      fresh++;

      if (c.stat) statsSeen.add(c.stat);
      if (!statMatches(c.stat, leg.prop)) continue;
      c.lines.forEach(l => linesSeen.add(l.value));

      // Exact line already on the face — take it, but only on a STANDARD card.
      // Demons and goblins are 73% of the live board and carry their own payout
      // multipliers, so staging one at the same number silently changes the
      // slip's economics. Logged CoreProp legs are standard by construction.
      if (lineMatches(c, leg.line)) {
        if (cardIsStandard(c)) return { card: c, exact: true };
        // Remember it, but do NOT skip the card: PP groups alternates by
        // group_key, so the standard variant of this same prop can be sitting
        // behind the swap control on this very card.
        nonStandardHit = cardMultiplier(c, leg.side) || true;
      }

      // Otherwise dig through this card's alternates.
      const alt = await cycleAlternates(c, leg, deadline);
      if (alt.card) return { card: alt.card, exact: true };
      if (alt.better && (!better || Math.abs(alt.better.line - parseFloat(leg.line))
                                  < Math.abs(better.line - parseFloat(leg.line)))) {
        better = { ...alt.better, key: k };
      }
      if (Date.now() >= deadline) break;
    }

    dry = fresh ? 0 : dry + 1;
    if (!await scrollBoard(board[0]?.el)) break;   // hit the bottom
  }

  return { sawPlayer, better, nonStandardHit,
           statsSeen: [...statsSeen], linesSeen: [...linesSeen].sort((a, b) => a - b) };
}

/**
 * Re-locate a card by identity key and set it to `line` via its swap control.
 * Needed because the better-line fallback is only chosen after the full scroll,
 * by which point the original element has almost certainly been recycled.
 */
async function refindAtLine(leg, line, deadline) {
  for (let pass = 0; pass < 10 && Date.now() < deadline; pass++) {
    const mine = findCards().filter(x => playerMatches(x.player, leg.player)
                                      && statMatches(x.stat, leg.prop));
    for (const c of mine) {
      let cur = c;
      for (let i = 0; i <= MAX_SWAPS && Date.now() < deadline; i++) {
        if (cardIsStandard(cur) && cur.lines.some(l => Math.abs(l.value - line) < 0.001)) return cur;
        const next = await swapCardLine(cur, SWAP_WAIT_MS);
        if (!next) break;
        cur = next;
      }
    }
    if (!await scrollBoard(findCards()[0]?.el)) break;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Staging one leg
// ---------------------------------------------------------------------------

/**
 * Click a side and CONFIRM the pick landed by reading PP's own selected state
 * back off the button. Returns true only on confirmation.
 */
/**
 * Re-find a card in the LIVE DOM by identity. The board is virtualized and PP
 * re-renders on swap, so any element captured more than a moment ago may be
 * detached — and a detached node still answers queries with stale content,
 * which is worse than it being missing.
 */
function liveCardLike(info, targetLine) {
  for (const c of findCards()) {
    if (canonName(c.player) !== canonName(info.player)) continue;
    if (canonStat(c.stat) !== canonStat(info.stat)) continue;
    if (targetLine != null && !c.lines.some(l => Math.abs(l.value - targetLine) < 0.001)) continue;
    return c;
  }
  return null;
}

async function clickAndVerify(info, side, targetLine) {
  const resolve = () => (info.el.isConnected ? info : liveCardLike(info, targetLine));

  let cur = resolve();
  if (!cur) return { ok: false, reason: 'card left the board before it could be clicked' };

  // Idempotency: More/Less are toggles, so clicking an already-staged pick
  // REMOVES it. That would silently shrink the slip while we report success.
  if (isSideSelected(cur, side)) return { ok: true, already: true };

  await scrollToEl(cur.el);
  cur = resolve();                       // scrolling can recycle the node
  if (!cur) return { ok: false, reason: 'card was recycled mid-scroll' };
  if (isSideSelected(cur, side)) return { ok: true, already: true };

  const btn = side === 'over' ? cur.moreBtn : cur.lessBtn;
  if (!btn) return { ok: false, reason: `PrizePicks shows no ${side === 'over' ? 'More' : 'Less'} button on this card` };
  nativeClick(btn);

  // Confirm against whatever node is live NOW, not the one we clicked.
  const confirmed = await until(() => {
    const live = resolve();
    return live && isSideSelected(live, side);
  }, 2500, 150);
  if (confirmed) return { ok: true };
  return { ok: false, reason: 'clicked, but PrizePicks did not mark it selected' };
}

async function stageLeg(leg, index) {
  setLegStatus(index, 'searching');

  // Goblins/demons are More-only on PrizePicks — there is no Less button to
  // click. Say so instead of hunting for an element that was never rendered.
  if (leg.side === 'under' && leg.odds_type && leg.odds_type !== 'standard') {
    setLegStatus(index, 'skipped', 'PrizePicks only allows More on green devils');
    return false;
  }

  // Sport tab, per leg. PrizePicks' search is scoped to the active league, so
  // typing a WNBA player's name while NBA is selected returns nothing — or,
  // worse, collides with a same-named player in the wrong sport.
  const wantLeague = (leg.league || '').toUpperCase();
  if (wantLeague && wantLeague !== currentLeague) {
    setLegStatus(index, 'searching', `switching to ${wantLeague}…`);
    const sw = await switchToLeague(wantLeague);
    if (!sw.ok) {
      setLegStatus(index, 'error', sw.reason);
      return false;
    }
    currentLeague = wantLeague;
  }

  // Type the player's name so the board filters down to them, then hunt the
  // filtered set. If the search bar can't be opened we still hunt — it just
  // means scrolling the whole league board instead of a handful of cards.
  const searched = await searchForPlayer(leg.player);
  if (!searched) log(`leg ${index}: search unavailable, scanning the full board`);

  // Start the hunt budget only after the search settles — a slow search must
  // not eat into the time huntLeg gets to walk the board.
  const deadline = Date.now() + LEG_BUDGET_MS;
  const found = await huntLeg(leg, deadline);

  if (found.card) {
    const res = await clickAndVerify(found.card, leg.side, parseFloat(leg.line));
    await clearSearch();
    if (res.ok) {
      setLegStatus(index, 'done', res.already ? 'already on your board' : '');
      return true;
    }
    setLegStatus(index, 'error', res.reason);
    return false;
  }

  // No exact line anywhere. Fall back to a strictly better STANDARD line if one
  // turned up — better meaning easier to hit for this side.
  if (found.better) {
    const card = await refindAtLine(leg, found.better.line, Date.now() + 4000);
    if (card) {
      const res = await clickAndVerify(card, leg.side, found.better.line);
      await clearSearch();
      if (res.ok) {
        setLegStatus(index, 'done',
          `⚠ took ${found.better.line} instead of ${leg.line} (better for ${leg.side})`);
        betterLineSwaps.push({ ...leg, took: found.better.line });
        return true;
      }
      setLegStatus(index, 'error', res.reason);
      return false;
    }
  }

  await clearSearch();

  // Say what was actually on the board — "no card at line 27.5" is useless when
  // the number simply moved and the user could still decide to take it.
  if (!found.sawPlayer) {
    setLegStatus(index, 'error', `${leg.player} not on the ${wantLeague || 'current'} board`);
  } else if (found.nonStandardHit) {
    setLegStatus(index, 'error',
      `${leg.line} is only offered as a demon/goblin — different payout, add manually`);
  } else if (found.linesSeen.length) {
    setLegStatus(index, 'error', `line moved — board shows ${found.linesSeen.join(', ')}`);
  } else if (found.statsSeen.length) {
    setLegStatus(index, 'error', `no "${leg.prop}" card (board has: ${found.statsSeen.slice(0, 4).join(', ')})`);
  } else {
    setLegStatus(index, 'error', `no "${leg.prop}" card for ${leg.player}`);
  }
  return false;
}

// ---------------------------------------------------------------------------
// Main flow
// ---------------------------------------------------------------------------

let running = false;
let completed = false;

// Which league tab the board is currently showing, as last confirmed by
// switchToLeague. stageLeg consults this before every leg.
let currentLeague = '';

// Legs staged at a different number than the slip asked for (a strictly better
// standard line). Surfaced prominently at the end — the placed slip no longer
// matches the backtested one, and the user needs to know that.
let betterLineSwaps = [];

async function run() {
  if (running || completed) return;
  running = true;
  try {
    await runInner();
  } finally {
    running = false;
  }
}

async function runInner() {
  const token = await getToken();

  // No token means the user opened PrizePicks on their own. Do nothing at all:
  // no panel, no network calls. A third-party overlay appearing on someone's
  // real-money betting account every single visit reads as adware.
  if (!token) { log('no slip token — standing down'); return; }

  ensurePanel();
  setStatus('Checking for your slip…');
  showRetry(null);

  const { slip, error } = await fetchPendingSlip(token, msg => setStatus(msg));

  if (error) {
    setStatus('Could not reach CoreProp.', '#f87171');
    setFooter(`<b>${escapeHtml(error)}</b><br>If CoreProp was asleep it may still be waking up.`, '#f87171');
    showRetry(() => run());
    return;
  }
  if (!slip) {
    setStatus('No pending slip.', '#94a3b8');
    setFooter('Click <b>Place on PrizePicks</b> in CoreProp to send a slip here.');
    setTimeout(() => panel && panel.remove(), 8000);
    return;
  }

  const legs = slip.legs || [];
  // Reset per-run state — run() is re-entrant via Retry and the sign-in watcher.
  currentLeague = '';
  betterLineSwaps = [];
  setStatus(`Staging ${legs.length} legs…`, '#3da9f0');
  renderLegs(legs);

  // Power vs Flex is the one thing the user MUST set themselves — a 6-leg
  // Power (~37.5x, all must hit) and a 6-leg Flex are different products with
  // different EV, and staging legs without flagging it is a silent money bug.
  if (slip.slip_type) {
    setBanner(`Set entry type to ${String(slip.slip_type).toUpperCase()} before submitting`);
  }

  // Wait for the board. This gate existed in the old build but was never
  // called, so on a cold load behind auth and a geo check the whole flow ran
  // against an empty page and blamed the line matching.
  setFooter('Waiting for the PrizePicks board…');
  const ready = await until(() => {
    if (ppIsChallenged()) return 'challenge';
    if (ppLooksSignedOut()) return 'signedout';
    return findCards().length ? 'board' : false;
  }, 30000, 500);

  if (ready === 'challenge') {
    setStatus('PrizePicks is showing a security check.', '#f59e0b');
    setFooter('Complete the check, then press Retry. Your slip is still saved.', '#f59e0b');
    showRetry(() => { completed = false; run(); });
    return;
  }
  if (ready === 'signedout' || !ready) {
    setStatus(ready ? 'Sign in to PrizePicks.' : 'The board never loaded.', '#f59e0b');
    setFooter('Sign in and finish any location check — this will continue automatically.', '#f59e0b');
    watchForBoard();
    return;
  }

  // Group by league so we switch tabs once per sport, in first-appearance order.
  const groups = new Map();
  legs.forEach((leg, i) => {
    const k = (leg.league || '').toUpperCase() || 'OTHER';
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push({ leg, i });
  });

  const failures = [];
  let staged = 0;

  // Grouping only minimises tab switches — stageLeg verifies the active league
  // itself before every single leg, so an out-of-order or failed switch can
  // never leave a leg being searched against the wrong sport's board.
  for (const [league, items] of groups) {
    for (const { leg, i } of items) {
      setFooter(`[${escapeHtml(league)}] ${escapeHtml(leg.player)} — leg ${i + 1} of ${legs.length}`, '#3da9f0');
      const ok = await stageLeg(leg, i);
      if (ok) staged++;
      else failures.push({ ...leg, reason: document.querySelector(`#coreprop-leg-${i} .coreprop-leg-note`)?.textContent || 'unknown' });
      await sleep(300);
    }
  }

  // Only clear the server-side slip on a FULL success. A partial run must stay
  // retryable — the old build deleted it whenever a single leg "succeeded",
  // which combined with the always-true success path meant a totally failed
  // run destroyed the slip and the user had to re-place it from scratch.
  if (staged === legs.length) {
    completed = true;
    await sendToBackground({ type: 'coreprop:clear-pending-slip', token });
    await forgetToken();
  }

  await sendToBackground({
    type: 'coreprop:report-status',
    token,
    body: {
      version: VERSION, legs_total: legs.length, legs_staged: staged, failures,
      line_changes: betterLineSwaps.map(b => ({ player: b.player, prop: b.prop, want: b.line, took: b.took })),
    },
  });

  // A swapped line means the slip on screen is NOT the slip that was
  // backtested, so this outranks the Power/Flex reminder in the banner.
  if (betterLineSwaps.length) {
    setBanner(`${betterLineSwaps.length} leg(s) staged at a different line — check before submitting`);
  }

  if (staged === legs.length) {
    setStatus(`✅ All ${legs.length} legs staged.`, '#22c55e');
    setFooter(slip.slip_type
      ? `Set the entry to <b>${escapeHtml(slip.slip_type)}</b>, enter your stake, and submit.`
      : 'Enter your stake and submit.', '#22c55e');
  } else {
    setStatus(`⚠️ Staged ${staged} of ${legs.length}.`, '#f59e0b');
    setFooter(`${legs.length - staged} leg(s) need adding by hand — see the notes above.`, '#f59e0b');
    showRetry(() => run());
  }
}

/**
 * Keep watching after a sign-in / location-check redirect. The old build's
 * URL watcher only wrote to the console, so once PP bounced the tab to /login
 * the flow was dead for good even though the slip was still on the server.
 */
function watchForBoard() {
  let lastUrl = location.href;
  const iv = setInterval(() => {
    if (completed) { clearInterval(iv); return; }
    const urlChanged = location.href !== lastUrl;
    lastUrl = location.href;
    if ((urlChanged || findCards().length) && !running && !ppLooksSignedOut()) {
      clearInterval(iv);
      run();
    }
  }, 1500);
}

run().catch(err => {
  log('fatal:', err);
  setStatus('Extension error: ' + err.message, '#f87171');
  showRetry(() => { running = false; run(); });
});

watchForBoard();
