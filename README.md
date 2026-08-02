# File Beacon

Beam files directly between two browsers. One device gets a 4-digit code, the
other enters it, and files travel **peer-to-peer over a WebRTC data channel** —
no accounts, no uploads, no file bytes on any server.

```
npm install
npm start          # http://localhost:8787
```

Open two tabs (or two devices) on that URL: click **Start sharing** in one,
enter the code in the other, then drag files onto the page or use
**Choose files to send**.

The UI is dark by default with a light mode behind the masthead toggle
(persisted in `localStorage`).

Any number of devices can have the site open at once — including on the same
Wi-Fi. Each code pairs exactly two peers in a private, single-use room; extra
devices simply create their own rooms. On a shared LAN, ICE picks the local
path, so transfers run at local network speed.

## Layout

```
server.js       Node signaling + static server (only dependency: ws)
public/         Frontend — plain HTML/CSS/JS, no build step
  index.html
  style.css
  app.js        signaling client, WebRTC transfer engine, UI state machine
  sw.js         service worker (Web Share Target only — nothing is cached)
  manifest.webmanifest
```

## Signaling flow

The WebSocket server is matchmaking only. Every message is a small JSON object:

```
A → server   {t:'create'}                 mint a room
server → A   {t:'created', code, ttl}     4-digit code, 5-minute TTL
B → server   {t:'join', code}
server → B   {t:'joined', peer}           peer = coarse device info (UA summary)
server → A   {t:'peer-joined', peer}
A ⇄ server ⇄ B   {t:'signal', d}          opaque relay of SDP offers/answers + ICE
```

The host creates an `RTCPeerConnection`, opens an ordered `RTCDataChannel`
named `files`, and sends an offer. The guest answers. ICE candidates are
relayed both ways until a direct path is found. **From the moment the data
channel opens, the WebSocket is irrelevant** — closing the server would not
interrupt an in-flight transfer.

Room rules: codes are random 4-digit numbers, held for 5 minutes awaiting a
second peer, rejected once full, and destroyed the moment either peer
disconnects (single-use). Rooms live in an in-memory `Map`; nothing is
persisted anywhere.

## Why file data never touches the server

Architecturally, there is no code path for it: the server only ever
`JSON.parse`s WebSocket text frames and relays the `d` payload of `signal`
messages (SDP/ICE strings, a few KB total per session). File bytes are sent
with `RTCDataChannel.send(ArrayBuffer)` on a connection negotiated directly
between the two browsers — encrypted with DTLS end-to-end. You can verify
this empirically: kill `server.js` mid-transfer and the transfer completes.

## Chunking & backpressure

- Files are sliced with `File.slice(offset, offset + 64 KiB)` and each slice is
  read with `.arrayBuffer()` just before sending — the sender never holds more
  than one chunk (plus the channel's internal buffer) in memory, so
  multi-hundred-MB files don't freeze the tab.
- Backpressure: before each chunk, if `dc.bufferedAmount > 1 MiB` the send loop
  awaits the `bufferedamountlow` event (`bufferedAmountLowThreshold = 256 KiB`).
  This keeps memory flat and never overruns the SCTP buffer. A 20-second stall
  on that wait fails the transfer instead of hanging forever.
- **Ordered channel, sequential queue** (documented tradeoff): an unordered
  channel with per-chunk sequence headers can reduce head-of-line blocking, but
  on a single SCTP association it does not increase throughput, and it forces
  reassembly bookkeeping on the receiver. With an ordered channel, chunks for
  the current file arrive in order between its `meta` and `done` control
  messages, so the receiver just appends buffers. Multiple files are therefore
  queued **sequentially per direction** (both peers can still send to each
  other simultaneously — each direction has its own queue).
- Receive side: chunks accumulate in an array and become a `Blob` at the end,
  which the browser may spill to disk. True streaming-to-disk via the File
  System Access API is the documented stretch goal; the current approach keeps
  Firefox/Safari compatibility.
- Completion is acknowledged (`received` control message) so the sender's
  "done" state means *delivered*, not just *sent*.

## Transfer protocol (inside the data channel)

```
JSON (text frames):   {t:'meta', id, name, size, mime}   file starts
                      {t:'done', id}                      sender finished
                      {t:'received', id}                  receiver ack → sender shows complete
                      {t:'cancel', id}                    either side aborts
Binary frames:        64 KiB ArrayBuffer chunks of the current file
```

## NAT traversal

Google's public STUN servers are configured, which handles most home/mobile
NATs. Two peers both behind **symmetric NAT** (some corporate/CGNAT networks)
need a TURN relay to connect; that's out of scope here, and the UI says so
plainly when linking times out instead of failing silently.

## Accessibility

- Everything is operable by keyboard: real `<button>`/`<input>`/`<form>`
  elements, a hidden `<input type="file">` triggered by a visible focusable
  button, visible 2px focus outlines styled to the design.
- One polite `aria-live` region announces connection changes, queue events,
  errors, and transfer progress **throttled to 25% milestones**.
- The progress ring is `role="progressbar"` with
  `aria-valuenow/-min/-max/-valuetext`; the SVG itself is `aria-hidden`.
- Status is never conveyed by color alone — every state also appears as text
  ("· done", "· failed", status line) and contrast meets WCAG AA on the paper
  background.
- Click-to-browse is the primary affordance; drag-and-drop is the secondary
  hint. `prefers-reduced-motion` disables the beacon pulse and ring animation.

## Known browser limitations

| Capability | Chrome / Edge | Firefox | Safari (desktop) | iOS Safari / Android Chrome |
|---|---|---|---|---|
| WebRTC data channels | ✅ | ✅ | ✅ | ✅ |
| `bufferedamountlow` event | ✅ | ✅ | ✅ (15.4+) | ✅ |
| Web Share **Target** (receive from share sheet) | ✅ Android, installed PWA over HTTPS | ❌ | ❌ | ✅ Android Chrome only; iOS does not support share_target |
| Auto-download of received blob | ✅ | ✅ | may require the **Save** link (popup rules) | tap **Save** link |
| SVG manifest icons | ✅ | n/a | n/a | Android ✅ / iOS prefers PNG |

Other notes:

- The share sheet flow requires the app to be **served over HTTPS and
  installed as a PWA** (Android). On localhost it registers but the OS share
  sheet won't list it.
- Very large files are limited by receiver memory until the File System Access
  API path is added (Chromium-only today).
- Corporate networks that block UDP entirely will fail at the linking step;
  the app reports this rather than spinning forever.
