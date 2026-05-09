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

const COREPROP_URLS = [
  "http://localhost:8000",
  "http://127.0.0.1:8000",
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

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (!msg || !msg.type) return false;

  if (msg.type === "coreprop:get-pending-slip") {
    tryFetch("/api/pending-slip", {
      method: "GET",
      headers: { "Accept": "application/json" },
    }).then(sendResponse);
    return true;  // async
  }

  if (msg.type === "coreprop:clear-pending-slip") {
    tryFetch("/api/pending-slip", { method: "DELETE" }).then(sendResponse);
    return true;
  }

  return false;
});
