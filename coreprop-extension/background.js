/**
 * CoreProp — Background service worker.
 *
 * Acts as a network proxy for the content script. Content scripts in MV3
 * inherit the page origin (app.prizepicks.com) and Chrome restricts their
 * cross-origin fetches even with host_permissions. Service workers run in
 * the extension origin and can fetch any URL declared in host_permissions.
 *
 * Protocol: content script sends { type, ... } messages; this worker
 * fetches CoreProp and replies with { ok, status, data, error }.
 */

// Every CoreProp backend the extension knows about. The website's
// "Place on PrizePicks" button POSTs the slip to whichever of these the
// user has open — localhost when developing, onrender in production.
// The extension doesn't know which is in play, so it asks all of them
// and uses whichever has the slip. Single fetch per URL, no retries.
const COREPROP_URLS = [
  "http://localhost:8000",
  "http://127.0.0.1:8000",
  "https://coreprop.onrender.com",
];

async function tryFetch(path, init) {
  let lastErr = null;
  for (const base of COREPROP_URLS) {
    try {
      const resp = await fetch(`${base}${path}`, init);
      const text = await resp.text();
      let data = null;
      try { data = text ? JSON.parse(text) : null; } catch { data = text; }
      return { ok: resp.ok, status: resp.status, data, base };
    } catch (err) {
      lastErr = err;
    }
  }
  return { ok: false, status: 0, error: lastErr ? lastErr.message : "Unknown network error" };
}

/**
 * Fetch the pending slip from each known CoreProp backend exactly once.
 * Return the response from whichever server actually has a slip. If no
 * server has one, return the first successful (empty) response. If every
 * URL fails outright, return a network error.
 *
 * This is the "get the slip right the first time" version: one round of
 * single fetches, no retries, no polling. The extension never asks the
 * same URL twice.
 */
async function fetchPendingSlipFromAny(token) {
  let firstEmpty = null;
  let lastErr = null;
  const q = token ? `?cp_slip=${encodeURIComponent(token)}` : "";
  for (const base of COREPROP_URLS) {
    try {
      const resp = await fetch(`${base}/api/pending-slip${q}`, {
        method: "GET",
        headers: { "Accept": "application/json" },
      });
      if (!resp.ok) { lastErr = `HTTP ${resp.status} from ${base}`; continue; }
      const text = await resp.text();
      let data = null;
      try { data = text ? JSON.parse(text) : null; } catch { data = text; }
      // Prefer whichever backend actually has a slip in flight.
      if (data && Array.isArray(data.legs) && data.legs.length > 0) {
        return { ok: true, status: resp.status, data, base };
      }
      // Remember the first 200-but-empty as a fallback so we can still
      // return a real "no pending slip" answer rather than a network error
      // if no server has anything.
      if (!firstEmpty) firstEmpty = { ok: true, status: resp.status, data, base };
    } catch (err) {
      lastErr = err.message;
    }
  }
  if (firstEmpty) return firstEmpty;
  return { ok: false, status: 0, error: lastErr || "Could not reach CoreProp" };
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (!msg || !msg.type) return false;

  if (msg.type === "coreprop:get-pending-slip") {
    fetchPendingSlipFromAny(msg.token || "").then(sendResponse);
    return true;  // async
  }

  if (msg.type === "coreprop:clear-pending-slip") {
    const q = msg.token ? `?cp_slip=${encodeURIComponent(msg.token)}` : "";
    tryFetch(`/api/pending-slip${q}`, { method: "DELETE" }).then(sendResponse);
    return true;
  }

  return false;
});
