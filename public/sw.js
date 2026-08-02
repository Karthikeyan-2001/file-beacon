/* File Beacon service worker.
 * Exists for one reason: to be a Web Share Target. Files shared from the
 * OS share sheet arrive here as a POST, get forwarded to any open app
 * window via postMessage (File objects survive structured clone), and the
 * user is redirected back into the app. Nothing is cached or stored. */

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  if (event.request.method === 'POST' && url.pathname === '/share-target') {
    event.respondWith((async () => {
      try {
        const formData = await event.request.formData();
        const files = formData.getAll('files').filter((f) => f instanceof File);
        const clients = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
        for (const client of clients) {
          client.postMessage({ type: 'shared-files', files });
        }
      } catch {
        /* malformed share: just bounce back to the app */
      }
      return Response.redirect('/', 303);
    })());
  }
});
