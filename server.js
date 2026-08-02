// File Beacon — signaling server.
// Responsibilities: serve the static frontend, mint 4-digit room codes,
// relay SDP/ICE between exactly two matched sockets. File bytes never
// pass through this process — they travel over a WebRTC data channel.

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8787;
const PUBLIC_DIR = path.join(__dirname, 'public');
const ROOM_TTL_MS = 5 * 60 * 1000;     // unpaired rooms expire after 5 minutes
const MAX_ROOMS = 4000;                // stay well under the 9000-code space
const MAX_BAD_JOINS = 5;               // guesses per socket before disconnect
const MAX_SIGNAL_PAYLOAD = 64 * 1024;  // SDP+ICE fit comfortably; files never come here

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.webmanifest': 'application/manifest+json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain; charset=utf-8',
  '.md': 'text/markdown; charset=utf-8',
};

// ---------------------------------------------------------------- static
const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  // Share-target fallback when no service worker intercepted the POST:
  // never read the body, just bounce back to the app.
  if (url.pathname === '/share-target') {
    res.writeHead(303, { Location: '/' });
    res.end();
    req.resume(); // drain without buffering
    return;
  }

  let filePath = url.pathname === '/' ? '/index.html' : url.pathname;
  filePath = path.normalize(filePath).replace(/^(\.\.[/\\])+/, '');
  const abs = path.join(PUBLIC_DIR, filePath);
  if (!abs.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.readFile(abs, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
      return;
    }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(abs).toLowerCase()] || 'application/octet-stream',
      'Cache-Control': 'no-cache',
    });
    res.end(data);
  });
});

// --------------------------------------------------------------- rooms
/** code -> { host, guest, timer } */
const rooms = new Map();

function mintCode() {
  for (let i = 0; i < 100; i++) {
    const code = String(Math.floor(1000 + Math.random() * 9000));
    if (!rooms.has(code)) return code;
  }
  return null; // ~9000 concurrent rooms; effectively never happens
}

function send(ws, msg) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function destroyRoom(code, notify) {
  const room = rooms.get(code);
  if (!room) return;
  clearTimeout(room.timer);
  rooms.delete(code);
  for (const ws of [room.host, room.guest]) {
    if (!ws) continue;
    ws.room = null;
    if (notify) send(ws, notify);
  }
}

// ------------------------------------------------------------ signaling
const wss = new WebSocketServer({ server, maxPayload: MAX_SIGNAL_PAYLOAD });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.room = null;
  ws.badJoins = 0;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', () => ws.terminate()); // oversized frames etc.: drop, don't crash

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (typeof msg !== 'object' || msg === null) return;

    switch (msg.t) {
      case 'create': {
        if (ws.room) return;
        if (rooms.size >= MAX_ROOMS) { send(ws, { t: 'error', code: 'busy' }); return; }
        const code = mintCode();
        if (!code) { send(ws, { t: 'error', code: 'busy' }); return; }
        const room = {
          host: ws,
          guest: null,
          timer: setTimeout(() => {
            destroyRoom(code, { t: 'expired' });
          }, ROOM_TTL_MS),
        };
        rooms.set(code, room);
        ws.room = code;
        ws.info = sanitizeInfo(msg.info);
        send(ws, { t: 'created', code, ttl: ROOM_TTL_MS });
        break;
      }

      case 'join': {
        if (ws.room) return;
        const code = String(msg.code || '');
        if (!/^\d{4}$/.test(code)) { send(ws, { t: 'error', code: 'not-found' }); return; }
        const room = rooms.get(code);
        if (!room || room.guest) {
          // Brute-forcing the 4-digit space gets a socket ~5 guesses, then a new
          // TCP+WS handshake. That prices out scanning 10k codes within a TTL.
          if (++ws.badJoins >= MAX_BAD_JOINS) {
            send(ws, { t: 'error', code: 'too-many-attempts' });
            ws.close(1008, 'too many join attempts');
            return;
          }
          send(ws, { t: 'error', code: room ? 'full' : 'not-found' });
          return;
        }
        clearTimeout(room.timer); // paired rooms live until a peer leaves
        room.guest = ws;
        ws.room = code;
        ws.info = sanitizeInfo(msg.info);
        send(ws, { t: 'joined', peer: room.host.info || null });
        send(room.host, { t: 'peer-joined', peer: ws.info || null });
        break;
      }

      case 'signal': {
        const room = rooms.get(ws.room);
        if (!room) return;
        const other = room.host === ws ? room.guest : room.host;
        send(other, { t: 'signal', d: msg.d });
        break;
      }
    }
  });

  ws.on('close', () => {
    if (ws.room) destroyRoom(ws.room, { t: 'peer-left' }); // codes are single-use
  });
});

// Reap dead sockets so abandoned rooms don't linger past TTL semantics.
const reaper = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30 * 1000);
wss.on('close', () => clearInterval(reaper));

function sanitizeInfo(info) {
  if (typeof info !== 'object' || info === null) return null;
  const ua = typeof info.ua === 'string' ? info.ua.slice(0, 80) : '';
  return { ua };
}

server.listen(PORT, () => {
  console.log(`File Beacon signaling + static server on http://localhost:${PORT}`);
});
