/* CoreProp service worker.
 *
 * Exists for one reason: Web Push is only deliverable to a page that has a
 * registered service worker. There is deliberately NO offline caching here --
 * every number on this site is a live price, and a cached +EV board showing
 * yesterday's lines is worse than no board at all.
 *
 * Scope note: this file MUST be served from the origin root (/sw.js), not
 * /static/sw.js. A worker's default scope is its own directory, so one served
 * from /static/ could only control /static/* and would never receive push for
 * the app itself. web/app.py has an explicit route for this.
 */

// Take over immediately rather than waiting for every existing tab to close.
// Without these two, a user who grants permission has to fully quit the app
// before notifications start arriving.
self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('push', (event) => {
  // A push with no payload is legal and used by some services as a ping.
  // Show something rather than throwing, or iOS counts it as a failed
  // delivery and can revoke the subscription after repeated failures.
  let data = { title: 'CoreProp', body: 'New backtest activity.', url: '/' };
  if (event.data) {
    try {
      data = Object.assign(data, event.data.json());
    } catch (e) {
      data.body = event.data.text() || data.body;
    }
  }

  event.waitUntil(
    self.registration.showNotification(data.title, {
      body: data.body,
      icon: '/static/icon-192.png',
      badge: '/static/icon-192.png',
      // Collapses repeat notifications into one entry instead of stacking a
      // new row every refresh cycle.
      tag: 'coreprop-backtest',
      renotify: true,
      data: { url: data.url || '/' },
    })
  );
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const target = (event.notification.data && event.notification.data.url) || '/';

  // Focus an already-open CoreProp window if there is one, rather than opening
  // a second copy of a single-page app that is probably already on the page
  // the user wants.
  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((list) => {
      for (const client of list) {
        if (client.url.indexOf(self.registration.scope) === 0 && 'focus' in client) {
          client.navigate(target);
          return client.focus();
        }
      }
      return self.clients.openWindow(target);
    })
  );
});
