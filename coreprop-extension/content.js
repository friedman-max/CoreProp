/**
 * CoreProp — PrizePicks Slip Builder (content script)
 *
 * Auto-clicks player cards to build your CoreProp slip on PrizePicks.
 * Never submits — user reviews and places the wager manually.
 */

const COREPROP_URL = "http://localhost:8000";
const DEBUG = true;

function log(...args) { if (DEBUG) console.log("[CoreProp]", ...args); }

// ---------------------------------------------------------------------------
// Floating status panel — always shows on page load
// ---------------------------------------------------------------------------

let panel = null;
let panelLegList = null;

function ensurePanel() {
  if (panel) return panel;

  panel = document.createElement("div");
  panel.id = "coreprop-panel";
  Object.assign(panel.style, {
    position:     "fixed",
    top:          "80px",
    right:        "16px",
    zIndex:       "2147483647",
    background:   "#0f172a",
    border:       "1px solid #1e3a5f",
    borderRadius: "12px",
    padding:      "14px 16px",
    minWidth:     "280px",
    maxWidth:     "340px",
    fontFamily:   "system-ui, -apple-system, sans-serif",
    fontSize:     "13px",
    color:        "#e2e8f0",
    boxShadow:    "0 8px 32px rgba(0,0,0,0.6)",
  });

  const header = document.createElement("div");
  Object.assign(header.style, {
    display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "10px",
  });
  header.innerHTML = `
    <span style="font-weight:700; font-size:14px; color:#4f8cff;">CoreProp</span>
    <button id="coreprop-close" style="background:transparent;border:none;color:#64748b;font-size:18px;cursor:pointer;padding:0 4px;line-height:1;">×</button>
  `;

  const status = document.createElement("div");
  status.id = "coreprop-status";
  status.style.cssText = "font-size:12px; color:#94a3b8; margin-bottom:10px;";
  status.textContent = "Connecting to CoreProp…";

  const list = document.createElement("div");
  list.id = "coreprop-leg-list";
  list.style.cssText = "max-height:60vh; overflow-y:auto;";

  const footer = document.createElement("div");
  footer.id = "coreprop-footer";
  footer.style.cssText = "margin-top:10px; font-size:11px; color:#64748b; text-align:center; line-height:1.5;";

  panel.appendChild(header);
  panel.appendChild(status);
  panel.appendChild(list);
  panel.appendChild(footer);
  document.body.appendChild(panel);

  document.getElementById("coreprop-close").addEventListener("click", () => panel.remove());

  panelLegList = list;
  return panel;
}

function setStatus(text, color = "#94a3b8") {
  ensurePanel();
  const el = document.getElementById("coreprop-status");
  if (el) { el.textContent = text; el.style.color = color; }
}

function setFooter(text, color = "#64748b") {
  ensurePanel();
  const el = document.getElementById("coreprop-footer");
  if (el) { el.innerHTML = text; el.style.color = color; }
}

function renderLegs(legs) {
  ensurePanel();
  panelLegList.innerHTML = "";
  legs.forEach((leg, i) => {
    const row = document.createElement("div");
    row.id = `coreprop-leg-${i}`;
    Object.assign(row.style, {
      display: "flex", alignItems: "center", gap: "8px",
      padding: "6px 0", borderBottom: "1px solid #1e293b",
    });
    row.innerHTML = `
      <span class="coreprop-status-icon" style="font-size:14px; min-width:18px;">⏳</span>
      <div style="flex:1; min-width:0;">
        <div style="font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">${escapeHtml(leg.player)}</div>
        <div style="font-size:11px; color:#94a3b8;">${escapeHtml(leg.prop)} ${leg.side.toUpperCase()} ${leg.line}</div>
      </div>
    `;
    panelLegList.appendChild(row);
  });
}

function setLegStatus(index, status, note = "") {
  const row = document.getElementById(`coreprop-leg-${index}`);
  if (!row) return;
  const icon = row.querySelector(".coreprop-status-icon");
  if (!icon) return;
  if (status === "searching") icon.textContent = "🔍";
  else if (status === "done") { icon.textContent = "✅"; row.style.opacity = "0.6"; }
  else if (status === "error") { icon.textContent = "❌"; icon.style.color = "#f87171"; }
  if (note) row.title = note;
}

function escapeHtml(s) {
  const d = document.createElement("div");
  d.textContent = s == null ? "" : String(s);
  return d.innerHTML;
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function normalize(s) {
  return (s || "").toLowerCase()
    .replace(/['']/g, "'")
    .replace(/[^a-z0-9' ]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function similarity(a, b) {
  a = normalize(a); b = normalize(b);
  if (!a || !b) return 0;
  if (a === b) return 1;
  if (a.includes(b) || b.includes(a)) return 0.9;
  // bigram similarity
  const bigrams = s => {
    const out = new Set();
    for (let i = 0; i < s.length - 1; i++) out.add(s.slice(i, i + 2));
    return out;
  };
  const A = bigrams(a), B = bigrams(b);
  if (!A.size || !B.size) return 0;
  let shared = 0;
  B.forEach(g => { if (A.has(g)) shared++; });
  return (2 * shared) / (A.size + B.size);
}

/**
 * Real, native click that React respects. PrizePicks listens to pointer/mouse
 * events through React's synthetic system; a bare .click() sometimes does
 * nothing on certain card types.
 */
function nativeClick(el) {
  const rect = el.getBoundingClientRect();
  const x = rect.left + rect.width / 2;
  const y = rect.top + rect.height / 2;
  const opts = { bubbles: true, cancelable: true, view: window, clientX: x, clientY: y, button: 0 };
  el.dispatchEvent(new PointerEvent("pointerdown", opts));
  el.dispatchEvent(new MouseEvent("mousedown", opts));
  el.dispatchEvent(new PointerEvent("pointerup", opts));
  el.dispatchEvent(new MouseEvent("mouseup", opts));
  el.dispatchEvent(new MouseEvent("click", opts));
}

// ---------------------------------------------------------------------------
// API calls
// ---------------------------------------------------------------------------

// Network calls run in the background service worker — content scripts in MV3
// can't reliably make cross-origin requests even with host_permissions.

function sendToBackground(message) {
  return new Promise(resolve => {
    try {
      chrome.runtime.sendMessage(message, resp => {
        if (chrome.runtime.lastError) {
          resolve({ ok: false, status: 0, error: chrome.runtime.lastError.message });
        } else {
          resolve(resp || { ok: false, status: 0, error: "no response from background" });
        }
      });
    } catch (err) {
      resolve({ ok: false, status: 0, error: err.message });
    }
  });
}

async function fetchPendingSlip() {
  const resp = await sendToBackground({ type: "coreprop:get-pending-slip" });
  log("pending-slip response:", resp);

  if (!resp.ok) {
    return { error: resp.error || `HTTP ${resp.status}`, slip: null };
  }
  const data = resp.data;
  if (!data || !data.legs || !data.legs.length) {
    return { error: null, slip: null };
  }
  return { error: null, slip: data };
}

async function clearPendingSlip() {
  await sendToBackground({ type: "coreprop:clear-pending-slip" });
}

// ---------------------------------------------------------------------------
// Card discovery — multiple strategies, since PP DOM varies
// ---------------------------------------------------------------------------

/**
 * Find every element on the page that looks like a player prop card.
 * Strategy: walk the DOM looking for elements containing both a player-like
 * text node and a line score. This is far more robust than fixed selectors.
 */
function discoverCards() {
  const candidates = new Set();

  // Strategy 1: known PP class/test-id patterns
  const fixedSelectors = [
    '[data-testid*="player-card" i]',
    '[data-testid*="projection" i]',
    '[class*="PlayerCard"]',
    '[class*="player-card"]',
    '[class*="ProjectionCard"]',
    '[class*="projection-card"]',
    '[class*="Card_card" i]',
    'div[role="button"][aria-label*="select" i]',
  ];
  for (const sel of fixedSelectors) {
    document.querySelectorAll(sel).forEach(el => candidates.add(el));
  }

  // Strategy 2: any div containing both an "Over"/"Under"/"More"/"Less"
  // button and a numeric line score child. PP cards typically have either:
  //   - "More" / "Less" text labels (current default)
  //   - "Over" / "Under" labels
  const allButtons = document.querySelectorAll('button, [role="button"]');
  allButtons.forEach(btn => {
    const txt = (btn.innerText || "").trim().toLowerCase();
    if (["more", "less", "over", "under"].includes(txt)) {
      // Walk up to find the card container
      let parent = btn.parentElement;
      for (let i = 0; i < 8 && parent; i++) {
        const ptxt = (parent.innerText || "");
        if (/\d+(\.\d+)?/.test(ptxt) && ptxt.length < 400) {
          candidates.add(parent);
          break;
        }
        parent = parent.parentElement;
      }
    }
  });

  return Array.from(candidates);
}

/**
 * Inside a card element, find the More/Less/Over/Under button matching `side`.
 *  - PrizePicks default: side "over" maps to "More", "under" maps to "Less"
 *  - Some boards still show literal "Over"/"Under"
 */
function findSideButton(card, side) {
  const want = side === "over" ? ["more", "over", "higher"] : ["less", "under", "lower"];
  const btns = card.querySelectorAll('button, [role="button"]');
  for (const b of btns) {
    const t = (b.innerText || "").trim().toLowerCase();
    if (want.includes(t)) return b;
  }
  return null;
}

/**
 * Extract every numeric token from a card so we can verify the EXACT line
 * shows up (not just any number that happens to share digits with the line).
 */
function cardNumbers(card) {
  const text = (card.innerText || card.textContent || "");
  const out = [];
  // Match decimals (4.5, 27.5) and integers; ignore percent signs and dates.
  const re = /(?<![%\d])(\d{1,3}(?:\.\d+)?)(?![%\/\d])/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    out.push(parseFloat(m[1]));
  }
  return out;
}

/**
 * Strict card match: player name fuzzy ≥ 0.65, prop word found, and the
 * EXACT line value appears as a numeric token.
 */
function cardMatchesLeg(card, leg) {
  const text = (card.innerText || card.textContent || "").toLowerCase();
  const propLower = normalize(leg.prop);

  // Prop type word match (lenient — handles "Pts+Rebs+Asts" vs "Points")
  const propWords = propLower.split(" ").filter(w => w.length >= 3);
  if (!propWords.some(w => text.includes(w))) return { match: false, score: 0 };

  // EXACT line match — line must appear as a parsed numeric token
  const nums = cardNumbers(card);
  const targetLine = parseFloat(leg.line);
  const hasExactLine = nums.some(n => Math.abs(n - targetLine) < 0.001);
  if (!hasExactLine) return { match: false, score: 0 };

  // Player-name similarity using the most prominent text lines
  const cardLines = (card.innerText || card.textContent || "")
    .split("\n").map(s => s.trim()).filter(Boolean);
  let bestSim = 0;
  for (const line of cardLines.slice(0, 5)) {
    const s = similarity(leg.player, line);
    if (s > bestSim) bestSim = s;
  }
  return { match: bestSim >= 0.65, score: bestSim };
}

// ---------------------------------------------------------------------------
// Build flow
// ---------------------------------------------------------------------------

async function waitForBoard(timeoutMs = 25000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const cards = discoverCards();
    if (cards.length > 0) return cards;
    await sleep(500);
  }
  return [];
}

// ---------------------------------------------------------------------------
// Sport-tab navigation — each leg's `league` maps to a tab labeled with the
// abbreviation (NBA, MLB, NHL, NCAAB). PP's URL doesn't change per sport,
// so we must click the tab in the DOM. The tab bar can scroll out of view,
// so we always scroll to top before clicking.
// ---------------------------------------------------------------------------

const LEAGUE_TAB_LABELS = {
  NBA:    ["NBA"],
  WNBA:   ["WNBA"],
  MLB:    ["MLB"],
  NHL:    ["NHL"],
  NCAAB:  ["NCAAB", "CBB"],
};

async function scrollToTop() {
  window.scrollTo({ top: 0, behavior: "smooth" });
  await sleep(500);
}

/**
 * Find a sport-tab element matching one of the candidate labels. Looks for
 * any clickable element whose visible text exactly equals the label.
 */
function findSportTab(labels) {
  const wanted = new Set(labels.map(s => s.toUpperCase()));
  const candidates = document.querySelectorAll(
    'button, a, [role="tab"], [role="button"], [class*="tab" i], [class*="league" i], [class*="sport" i]'
  );
  for (const el of candidates) {
    const txt = (el.innerText || el.textContent || "").trim().toUpperCase();
    if (!txt) continue;
    // Match if the text equals the label OR starts with it (handles "NBA · 8")
    if (wanted.has(txt)) return el;
    for (const w of wanted) {
      if (txt === w || txt.startsWith(w + " ") || txt.startsWith(w + "\n")) return el;
    }
  }
  return null;
}

/**
 * Switch PP's board to the given league. No-ops if no tab is found
 * (some boards may default to one sport without tabs).
 */
async function switchToLeague(league) {
  const labels = LEAGUE_TAB_LABELS[league] || [league];
  log(`Switching to ${league} (labels: ${labels.join(", ")})`);

  await scrollToTop();
  const tab = findSportTab(labels);
  if (!tab) {
    log(`No sport tab found for ${league} — proceeding without tab switch`);
    return false;
  }
  log(`Clicking sport tab "${tab.innerText.trim()}"`);
  tab.scrollIntoView({ behavior: "auto", block: "center" });
  nativeClick(tab);
  await sleep(1200);   // give the React board time to refilter
  return true;
}

// ---------------------------------------------------------------------------
// Search bar interaction — PP has a global search that filters across sports
// ---------------------------------------------------------------------------

/**
 * Find PP's player search input. Returns null if the input isn't currently
 * mounted (PP hides it until the magnifying-glass icon is clicked).
 */
function findSearchInput() {
  const candidates = [
    'input[type="search"]',
    'input[placeholder*="search" i]',
    'input[placeholder*="player" i]',
    'input[aria-label*="search" i]',
    'input[name*="search" i]',
    'input[id*="search" i]',
    'input[class*="search" i]',
  ];
  for (const sel of candidates) {
    const el = document.querySelector(sel);
    if (el && el.offsetParent !== null) return el;  // must be visible
  }
  // Last-resort fallback: any visible text input on the page
  const inputs = [...document.querySelectorAll('input[type="text"], input:not([type])')];
  for (const i of inputs) {
    if (i.offsetParent === null) continue;
    const name = (i.placeholder || i.name || i.id || "").toLowerCase();
    if (name.includes("search") || name.includes("player") || name.includes("find")) return i;
  }
  return null;
}

/**
 * Return all candidate clickable elements in the top-left of the page that
 * could be the search icon, sorted by likelihood (highest first).
 *
 * The search icon lives in the same top-left nav bar as the prop-type chips
 * and date-range picker. We constrain to the top ~300px / leftmost ~50% of
 * the viewport and accept any clickable element containing an SVG/icon.
 */
function findSearchIconCandidates() {
  const TOP_REGION_PX = 300;
  const LEFT_FRACTION = 0.55;
  const leftLimit = window.innerWidth * LEFT_FRACTION;

  const all = document.querySelectorAll(
    'button, [role="button"], a[href], [tabindex], div[onclick], svg'
  );
  const seen = new Set();
  const matches = [];
  for (const raw of all) {
    // If a candidate is an SVG, walk up to the nearest clickable ancestor.
    let el = raw;
    if (el.tagName.toLowerCase() === "svg") {
      let p = el.parentElement;
      while (p && p !== document.body) {
        const tag = p.tagName.toLowerCase();
        if (tag === "button" || tag === "a" || p.getAttribute("role") === "button" ||
            p.getAttribute("tabindex") !== null) {
          el = p;
          break;
        }
        p = p.parentElement;
      }
    }
    if (seen.has(el)) continue;
    seen.add(el);

    const rect = el.getBoundingClientRect();
    if (rect.top > TOP_REGION_PX || rect.bottom < 0) continue;
    if (rect.left > leftLimit) continue;
    if (rect.width === 0 || rect.height === 0) continue;
    if (rect.width > 280) continue;   // skip wide bars / containers

    const aria = (el.getAttribute("aria-label") || "").toLowerCase();
    const tid  = (el.getAttribute("data-testid") || "").toLowerCase();
    const cls  = String((el.className && el.className.baseVal) || el.className || "").toLowerCase();
    const titleAttr = (el.getAttribute("title") || "").toLowerCase();
    const txt  = (el.innerText || "").trim().toLowerCase();

    const labelHit = [aria, tid, cls, titleAttr].some(s => s.includes("search") || s.includes("magnif"));
    const hasSvg = !!el.querySelector("svg");
    const looksIconish = rect.width <= 80 && rect.height <= 80 && hasSvg && txt.length === 0;

    if (labelHit) {
      matches.push({ el, score: 100, why: "labelled-search" });
    } else if (looksIconish) {
      matches.push({ el, score: 50, why: "icon-button-no-text" });
    } else if (hasSvg && rect.width <= 200) {
      matches.push({ el, score: 20, why: "small-with-svg" });
    }
  }

  matches.sort((a, b) => b.score - a.score);
  log(`Search-icon candidates: ${matches.length}`,
    matches.slice(0, 6).map(m => ({ why: m.why, rect: m.el.getBoundingClientRect(), html: m.el.outerHTML.slice(0, 120) })));
  return matches.map(m => m.el);
}

/**
 * Try keyboard shortcuts that commonly open search bars on web apps.
 * Returns true if an input becomes visible.
 */
async function tryKeyboardSearchShortcut(timeoutMs = 1200) {
  const shortcuts = [
    { key: "/", code: "Slash" },
    { key: "k", code: "KeyK", metaKey: true },   // Cmd+K (mac)
    { key: "k", code: "KeyK", ctrlKey: true },   // Ctrl+K (win/linux)
  ];
  for (const sc of shortcuts) {
    const opts = { key: sc.key, code: sc.code, bubbles: true, cancelable: true,
                   metaKey: !!sc.metaKey, ctrlKey: !!sc.ctrlKey };
    document.body.dispatchEvent(new KeyboardEvent("keydown", opts));
    document.body.dispatchEvent(new KeyboardEvent("keyup", opts));
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      await sleep(120);
      const input = findSearchInput();
      if (input) {
        log(`Search input opened via "${sc.metaKey ? "Cmd+" : sc.ctrlKey ? "Ctrl+" : ""}${sc.key}" shortcut`);
        input.focus();
        return input;
      }
    }
  }
  return null;
}

/**
 * Open the search input. Tries, in order:
 *   1. Already-mounted input (nothing to do)
 *   2. Each candidate icon in the top-left nav, clicked one at a time
 *   3. Keyboard shortcuts ('/' and Cmd/Ctrl+K)
 *
 * Returns the input element, or null if it never appeared.
 */
async function openSearchInput(timeoutMs = 4000) {
  let input = findSearchInput();
  if (input) { input.focus(); return input; }

  const candidates = findSearchIconCandidates();
  for (const icon of candidates) {
    log(`Trying search-icon candidate:`, icon);
    icon.scrollIntoView({ behavior: "auto", block: "center" });
    nativeClick(icon);
    const deadline = Date.now() + 1500;
    while (Date.now() < deadline) {
      await sleep(140);
      input = findSearchInput();
      if (input) {
        log("Search input mounted after clicking candidate");
        input.focus();
        return input;
      }
    }
    // Wrong candidate — try to close any panel it opened, then continue
    nativeClick(icon);
    await sleep(150);
  }

  // Last resort: keyboard shortcuts
  input = await tryKeyboardSearchShortcut(1500);
  if (input) return input;

  log("Could not open search — no candidate revealed an input");
  return null;
}

/**
 * React-controlled inputs ignore direct .value = "X" assignment because
 * React caches the DOM value. We have to call the native setter and then
 * dispatch an `input` event so the React handler runs.
 */
function setReactInputValue(input, value) {
  const proto = window.HTMLInputElement.prototype;
  const setter = Object.getOwnPropertyDescriptor(proto, "value").set;
  setter.call(input, value);
  input.dispatchEvent(new Event("input", { bubbles: true }));
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

/**
 * Open the search bar (clicking the icon if necessary), type a query, and
 * wait for the board to refilter to the player.
 *
 * Returns:
 *   true  — search opened, query typed, and at least one card with the
 *           player's name is now visible
 *   false — couldn't open the search input, or no matching card appeared
 *           within the timeout
 */
async function searchForPlayer(playerName, timeoutMs = 5000) {
  const input = await openSearchInput(3000);
  if (!input) {
    log(`Search input unavailable — cannot search for "${playerName}"`);
    return false;
  }

  log(`Searching for "${playerName}"`);
  input.focus();
  // Clear any leftover query, then type the new one
  setReactInputValue(input, "");
  await sleep(150);
  setReactInputValue(input, playerName);

  // Wait until cards appear with the player's name in them. Bail early if
  // we hit the deadline so the caller can fall back to scrolling.
  const target = normalize(playerName);
  const targetWords = target.split(" ").filter(w => w.length >= 3);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    await sleep(250);
    const cards = discoverCards();
    for (const card of cards) {
      const text = normalize(card.innerText || "");
      if (!text) continue;
      if (text.includes(target)) return true;
      if (targetWords.length && targetWords.every(w => text.includes(w))) return true;
    }
  }
  return false;
}

/**
 * Clear the search input so the next leg's search starts fresh. Doesn't
 * close the search overlay — leaves the input in place since reopening
 * it costs another click cycle.
 */
async function clearSearch() {
  const input = findSearchInput();
  if (!input) return;
  input.focus();
  setReactInputValue(input, "");
  await sleep(200);
}

async function tryClickMatchingCard(leg, index) {
  const cards = discoverCards();
  let bestCard = null, bestScore = 0;
  for (const card of cards) {
    const m = cardMatchesLeg(card, leg);
    if (m.match && m.score > bestScore) {
      bestScore = m.score;
      bestCard = card;
    }
  }
  if (!bestCard) return false;

  log(`Leg ${index} — found card (score ${bestScore.toFixed(2)})`, bestCard);
  bestCard.scrollIntoView({ behavior: "smooth", block: "center" });
  await sleep(450);

  // Step 1: side button directly inside the card
  let sideBtn = findSideButton(bestCard, leg.side);
  if (sideBtn) {
    log(`Leg ${index} — clicking side button "${sideBtn.innerText}"`);
    nativeClick(sideBtn);
    await sleep(700);
    setLegStatus(index, "done");
    return true;
  }

  // Step 2: click the card, look for picker in the resulting modal
  log(`Leg ${index} — clicking card, looking for side picker`);
  nativeClick(bestCard);
  await sleep(700);
  sideBtn = findSideButton(document.body, leg.side);
  if (sideBtn) {
    log(`Leg ${index} — clicking modal side button "${sideBtn.innerText}"`);
    nativeClick(sideBtn);
    await sleep(500);
    setLegStatus(index, "done");
    return true;
  }

  setLegStatus(index, "done", "Clicked — verify side manually if needed");
  return true;
}

async function buildLeg(leg, index) {
  setLegStatus(index, "searching");

  // Strategy 1: open PP's search bar, type the player's name, scroll the
  // filtered results to find the exact prop+line+side. This is the primary
  // path — a single player may have multiple props on PP (Points, Rebounds,
  // Pts+Rebs, etc.) and only one will match the leg, so we may still need
  // to scroll a small amount within the filtered results.
  const searched = await searchForPlayer(leg.player, 5000);
  if (searched) {
    log(`Leg ${index} — search succeeded, scanning filtered results`);
    const deadline = Date.now() + 12000;
    let attempts = 0;
    while (Date.now() < deadline) {
      attempts++;
      if (await tryClickMatchingCard(leg, index)) {
        await clearSearch();
        return true;
      }
      // Scroll within the filtered results to expose more cards for this
      // player (alternate lines, additional prop types).
      window.scrollBy({ top: 500, behavior: "smooth" });
      await sleep(700);
    }
    log(`Leg ${index} — search returned cards but none matched line ${leg.line} ${leg.side}`);
    setLegStatus(index, "error", `Player found, but no card at line ${leg.line} ${leg.side}`);
    await clearSearch();
    return false;
  }

  // Strategy 2: search bar wasn't reachable (icon missing, input never
  // mounted). Fall back to the slower scroll-the-whole-board approach.
  log(`Leg ${index} — search unavailable, falling back to full-board scroll`);

  const deadline = Date.now() + 30000;
  let attempts = 0;
  while (Date.now() < deadline) {
    attempts++;
    if (await tryClickMatchingCard(leg, index)) return true;
    window.scrollBy({ top: 600, behavior: "smooth" });
    await sleep(900);
  }

  setLegStatus(index, "error", `Could not find card after ${attempts} scroll attempts`);
  return false;
}

async function run() {
  log("CoreProp content script loaded on", location.href);
  ensurePanel();
  setStatus("Checking for pending slip…");
  setFooter("");

  await sleep(1000);

  const { slip, error } = await fetchPendingSlip();

  if (error) {
    setStatus("Could not reach CoreProp.", "#f87171");
    setFooter(
      `<b>${escapeHtml(error)}</b><br>` +
      `Verify the server is running at <code>http://localhost:8000</code>. ` +
      `If you reload the extension at <code>chrome://extensions</code>, the new background worker will be picked up.`,
      "#f87171"
    );
    return;
  }

  if (!slip) {
    setStatus("No pending slip from CoreProp.", "#94a3b8");
    setFooter("Click <b>Place on PrizePicks</b> in CoreProp to send a slip here.");
    setTimeout(() => panel && panel.remove(), 8000);
    return;
  }

  const legs = slip.legs;
  setStatus(`Found slip with ${legs.length} legs.`, "#4f8cff");
  renderLegs(legs);
  setFooter("Waiting for the PrizePicks page to settle…");

  // Give the page a couple of seconds to finish its initial render.
  // Critically: we do NOT scan the board at this point — per requirements,
  // every card scan must happen AFTER the player's name has been typed
  // into the search bar.
  await sleep(2500);

  // ── Group legs by league, preserving the order legs first appear so the
  // bot navigates to the FIRST leg's sport first. ──────────────────────────
  const groups = new Map();              // league -> [{leg, originalIndex}]
  legs.forEach((leg, i) => {
    const key = (leg.league || "").toUpperCase() || "OTHER";
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push({ leg, originalIndex: i });
  });
  log(`Sport groups (in navigation order):`, [...groups.keys()]);

  let success = 0;

  for (const [league, items] of groups) {
    setStatus(`Switching to ${league} (${items.length} leg${items.length > 1 ? "s" : ""})…`, "#4f8cff");
    setFooter(`Selecting ${league} sport tab…`);
    await switchToLeague(league);
    // Brief wait for the sport tab's content to mount, but no card scan.
    await sleep(800);

    for (const { leg, originalIndex } of items) {
      setFooter(`[${league}] Adding leg ${originalIndex + 1} of ${legs.length} — ${leg.player}…`, "#4f8cff");
      const ok = await buildLeg(leg, originalIndex);
      if (ok) success++;
      await sleep(400);
    }
  }

  await clearPendingSlip();

  if (success === legs.length) {
    setStatus(`✅ All ${legs.length} legs added.`, "#22c55e");
    setFooter("Review your slip, enter your wager, and submit manually.", "#22c55e");
  } else {
    setStatus(`⚠️ Added ${success} of ${legs.length} legs.`, "#f59e0b");
    setFooter(`${legs.length - success} leg${legs.length - success > 1 ? "s" : ""} couldn't be matched. Add them manually if needed.`, "#f59e0b");
  }
}

// Run after the page settles. PP is a SPA — rerun on URL changes too.
let lastUrl = location.href;
setInterval(() => {
  if (location.href !== lastUrl) {
    lastUrl = location.href;
    log("URL changed to", lastUrl);
  }
}, 1000);

run().catch(err => {
  log("Fatal error:", err);
  setStatus("Extension error: " + err.message, "#f87171");
});
