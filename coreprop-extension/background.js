/**
 * CoreProp — background service worker.
 *
 * Network proxy for the content script. MV3 content scripts inherit the page
 * origin (app.prizepicks.com) and Chrome restricts their cross-origin fetches
 * even with host_permissions; a service worker runs in the extension origin and
 * can fetch anything declared there.
 *
 * Protocol: content script sends {type, ...}; this replies {ok, status, data, error}.
 */

// Every CoreProp backend the extension knows about. The site's "Place on
// PrizePicks" button queues the slip on whichever the user has open —
// localhost while developing, onrender in production — and the extension has
// no way to know which, so it asks all of them.
const COREPROP_URLS = [
  "https://coreprop.onrender.com",
  "http://localhost:8000",
  "http://127.0.0.1:8000",
];

const BASE_KEY = "coreprop:base";
const FETCH_TIMEOUT_MS = 12000;

/** fetch with a hard timeout — a hung base must not stall the whole fan-out. */
async function timedFetch(url, opts = {}) {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), FETCH_TIMEOUT_MS);
  try {
    return await fetch(url, { ...opts, signal: ctl.signal });
  } finally {
    clearTimeout(t);
  }
}

/**
 * Bases to try, most-likely first. Once a base has answered with a slip we
 * remember it for the session, so subsequent calls (the status report in
 * particular) go straight there instead of re-probing localhost — which on a
 * normal user's machine is a guaranteed connection-refused and a console error
 * on every single request.
 */
async function orderedBases() {
  try {
    const { [BASE_KEY]: known } = await chrome.storage.session.get(BASE_KEY);
    if (known) return [known, ...COREPROP_URLS.filter(b => b !== known)];
  } catch (e) {}
  return COREPROP_URLS;
}

async function rememberBase(base) {
  try { await chrome.storage.session.set({ [BASE_KEY]: base }); } catch (e) {}
}

/**
 * Fetch the pending slip from whichever backend actually has it.
 * One round of single fetches; the content script owns the retry policy.
 */
async function fetchPendingSlipFromAny(token) {
  let firstEmpty = null;
  let lastErr = null;
  const q = token ? `?cp_slip=${encodeURIComponent(token)}` : "";

  for (const base of await orderedBases()) {
    try {
      const resp = await timedFetch(`${base}/api/pending-slip${q}`, {
        method: "GET",
        headers: { "Accept": "application/json" },
      });
      if (!resp.ok) { lastErr = `HTTP ${resp.status} from ${base}`; continue; }
      const text = await resp.text();
      let data = null;
      try { data = text ? JSON.parse(text) : null; } catch { data = text; }

      if (data && Array.isArray(data.legs) && data.legs.length > 0) {
        await rememberBase(base);
        return { ok: true, status: resp.status, data, base };
      }
      // Remember the first 200-but-empty so a "no pending slip" answer still
      // comes back as a real answer rather than a network error.
      if (!firstEmpty) firstEmpty = { ok: true, status: resp.status, data, base };
    } catch (err) {
      lastErr = err.name === "AbortError" ? `timed out reaching ${base}` : err.message;
    }
  }
  if (firstEmpty) return firstEmpty;
  return { ok: false, status: 0, error: lastErr || "Could not reach CoreProp" };
}

/** Best-effort POST/DELETE against every base (the slip may live on any). */
async function broadcast(path, opts, token) {
  const q = token ? `?cp_slip=${encodeURIComponent(token)}` : "";
  const results = await Promise.allSettled(
    (await orderedBases()).map(base => timedFetch(`${base}${path}${q}`, opts))
  );
  const ok = results.some(r => r.status === "fulfilled" && r.value.ok);
  return { ok, status: ok ? 200 : 0 };
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (!msg || !msg.type) return false;

  if (msg.type === "coreprop:get-pending-slip") {
    fetchPendingSlipFromAny(msg.token || "").then(sendResponse);
    return true;
  }

  if (msg.type === "coreprop:clear-pending-slip") {
    broadcast("/api/pending-slip", { method: "DELETE" }, msg.token).then(sendResponse);
    return true;
  }

  // Report what actually happened back to CoreProp, so the website can replace
  // its optimistic "Queued ✓" with the real outcome and so failures are
  // diagnosable without asking the user to open devtools.
  if (msg.type === "coreprop:report-status") {
    chrome.storage.session.set({ "coreprop:last-result": msg.body }).catch(() => {});
    broadcast("/api/pending-slip/status", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(msg.body || {}),
    }, msg.token).then(sendResponse);
    return true;
  }

  if (msg.type === "coreprop:get-last-result") {
    chrome.storage.session.get("coreprop:last-result")
      .then(r => sendResponse({ ok: true, data: r["coreprop:last-result"] || null }))
      .catch(() => sendResponse({ ok: false }));
    return true;
  }

  return false;
});
