/**
 * CoreProp — presence beacon (runs on CoreProp's own pages, not PrizePicks).
 *
 * Lets the website find out whether the extension is installed, so the Backtest
 * page can stop rendering "Queued for extension ✓" at users who have no
 * extension. Before this existed the site asserted success purely on its own
 * POST returning 200: the user clicked Place, a PrizePicks tab opened, nothing
 * happened, and CoreProp showed a green checkmark.
 *
 * Mechanism: a DOM marker plus a postMessage handshake.
 *
 * Deliberately NOT `externally_connectable` + chrome.runtime.sendMessage(ID).
 * That approach needs a stable, known extension ID, which only a Chrome Web
 * Store listing (or a pinned manifest "key") provides — an unpacked extension
 * gets a different ID on every machine it is loaded on. Since the shipping
 * install path is a self-hosted zip loaded in Developer Mode, an ID-based probe
 * would report "not installed" for every real user. The DOM marker works
 * regardless of ID.
 *
 * A content script runs in an isolated JS world but shares the DOM, so the
 * dataset attribute is visible to the page while the page cannot reach into
 * extension internals.
 */

const VERSION = chrome.runtime.getManifest().version;

document.documentElement.dataset.corepropExt = VERSION;

// The marker alone is racy: content scripts run at document_idle, which can be
// after React has already mounted and read it. The handshake covers that — the
// page can ask at any time and get an answer.
window.addEventListener('message', (ev) => {
  if (ev.source !== window) return;
  const d = ev.data;
  if (!d || d.source !== 'coreprop-page' || d.type !== 'ping') return;
  window.postMessage({ source: 'coreprop-extension', type: 'pong', version: VERSION }, location.origin);
});

// Announce ourselves once unprompted, for listeners that mounted before us.
window.postMessage({ source: 'coreprop-extension', type: 'ready', version: VERSION }, location.origin);
