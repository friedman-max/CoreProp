/* CoreProp service worker — installable PWA shell + Web Push receiver.
 *
 * Caching is deliberately conservative because this is a LIVE, paywalled
 * product: /api responses are NEVER cached (a stale +EV board, or one user's
 * cached payload served to another, is worse than a spinner). The worker is
 * network-first for the same-origin app shell with a cache fallback only for
 * offline. Its real jobs are (a) make the app installable/standalone reliably
 * and (b) receive Web Push. Cross-origin requests (CDN scripts, Supabase) pass
 * straight through untouched.
 *
 * Registered at /sw.js (root scope) — see web/app.py::service_worker.
 */
const CACHE = "coreprop-shell-ee120b4fd4";

self.addEventListener("install", () => {
  // Activate immediately; the activate handler claims already-open clients.
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)));
    await self.clients.claim();
  })());
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;                    // writes: passthrough
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;     // CDN / Supabase: passthrough
  if (url.pathname.startsWith("/api/")) return;        // live/paid data: never cache
  if (url.pathname === "/sw.js") return;               // never cache the worker itself

  event.respondWith((async () => {
    try {
      const fresh = await fetch(req);
      // Cache successful same-origin shell/static responses for offline use.
      if (fresh && fresh.ok && fresh.type === "basic") {
        const cache = await caches.open(CACHE);
        cache.put(req, fresh.clone());
      }
      return fresh;
    } catch (e) {
      const cached = await caches.match(req);
      if (cached) return cached;
      if (req.mode === "navigate") {
        const shell = await caches.match("/");
        if (shell) return shell;
        return new Response(
          "<!doctype html><meta charset=utf-8>" +
          "<meta name=viewport content='width=device-width,initial-scale=1'>" +
          "<title>CoreProp — offline</title>" +
          "<body style='margin:0;background:#0a0a0d;color:#e7e7ea;" +
          "font:16px system-ui,sans-serif;display:grid;place-items:center;height:100vh'>" +
          "<div style='text-align:center;padding:24px'>You're offline. " +
          "CoreProp needs a connection for live lines.<br><br>" +
          "<button onclick='location.reload()' style='padding:10px 18px;border-radius:999px;" +
          "border:0;background:#1E6FB0;color:#fff;font-weight:600;cursor:pointer'>Retry</button>" +
          "</div>",
          { headers: { "Content-Type": "text/html; charset=utf-8" } }
        );
      }
      throw e;
    }
  })());
});

// ── Web Push ───────────────────────────────────────────────────────────────
// The server sends {title, body, url, tag} (see the push send path in
// web/app.py). userVisibleOnly subscriptions must always show a notification.
self.addEventListener("push", (event) => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch (e) { data = {}; }
  const title = data.title || "CoreProp";
  const options = {
    body: data.body || "",
    icon: "/static/icon-192.png",
    badge: "/static/icon-192.png",
    tag: data.tag || "coreprop",
    data: { url: data.url || "/" },
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const target = (event.notification.data && event.notification.data.url) || "/";
  event.waitUntil((async () => {
    const all = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
    for (const c of all) {
      if ("focus" in c) {
        await c.focus();
        if ("navigate" in c) { try { await c.navigate(target); } catch (e) {} }
        return;
      }
    }
    if (self.clients.openWindow) await self.clients.openWindow(target);
  })());
});
