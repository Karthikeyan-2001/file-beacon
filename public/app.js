/* File Beacon — client.
 * Signaling over WebSocket (tiny JSON), file bytes over an ordered
 * RTCDataChannel with bufferedAmount backpressure. Sequential send queue:
 * one ordered channel means interleaving files buys no throughput and
 * complicates reassembly, so we keep a strict FIFO per direction. */

'use strict';

// ------------------------------------------------------------ constants
const CHUNK_SIZE = 64 * 1024;              // 64 KiB slices
const HIGH_WATER = 1 * 1024 * 1024;        // pause sending above 1 MiB buffered
const LOW_WATER = 256 * 1024;              // resume below 256 KiB
const CONNECT_TIMEOUT_MS = 20 * 1000;
const STALL_TIMEOUT_MS = 20 * 1000;
const GRACE_MS = 6 * 1000; // recovery window for a transient 'disconnected'
const RTC_CONFIG = {
  iceServers: [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }],
};

// ------------------------------------------------------------------ dom
const $ = (sel) => document.querySelector(sel);
const views = {
  gate: $('#view-gate'),
  lobby: $('#view-lobby'),
  link: $('#view-link'),
  session: $('#view-session'),
  dead: $('#view-dead'),
};
const statusText = $('#status-text');
const live = $('#live');
const transfersList = $('#transfers');
const tplTransfer = $('#tpl-transfer');

// ---------------------------------------------------------------- state
let ws = null;                // signaling socket
let pc = null;                // RTCPeerConnection
let dc = null;                // RTCDataChannel
let isHost = false;
let appState = 'idle';
let pendingCandidates = [];
let connectTimer = null;
let graceTimer = null;
let ttlTimer = null;

let sendQueue = [];           // transfer items waiting to be sent
let sending = null;           // item currently being sent
let receiving = null;         // item currently being received
let transferSeq = 0;
const transfers = new Map();  // id -> item (both directions, for UI + control)
let pendingSharedFiles = [];  // files from the share sheet before we're connected

// ------------------------------------------------------------- helpers
function setState(next, label) {
  appState = next;
  document.body.dataset.state = next;
  statusText.textContent = label || {
    idle: 'Idle', hosting: 'Waiting for peer', joining: 'Joining',
    connecting: 'Linking', connected: 'Linked', disconnected: 'Disconnected',
    error: 'Error',
  }[next] || next;
  for (const [name, el] of Object.entries(views)) el.hidden = true;
  const show = {
    idle: 'gate', hosting: 'lobby', joining: 'gate', connecting: 'link',
    connected: 'session', disconnected: 'dead', error: 'dead',
  }[next];
  if (show) views[show].hidden = false;
}

let lastAnnounce = '';
function announce(text) {
  if (text === lastAnnounce) return;
  lastAnnounce = text;
  live.textContent = '';
  // Re-set on next tick so repeated announcements with different text always fire.
  requestAnimationFrame(() => { live.textContent = text; });
}

function deviceSummary(ua) {
  ua = ua || '';
  let browser = 'a browser';
  if (/edg\//i.test(ua)) browser = 'Edge';
  else if (/firefox\//i.test(ua)) browser = 'Firefox';
  else if (/chrome\//i.test(ua)) browser = 'Chrome';
  else if (/safari\//i.test(ua)) browser = 'Safari';
  let os = 'an unknown device';
  if (/windows/i.test(ua)) os = 'Windows';
  else if (/android/i.test(ua)) os = 'Android';
  else if (/iphone|ipad|ipod/i.test(ua)) os = 'iOS';
  else if (/mac os x/i.test(ua)) os = 'macOS';
  else if (/linux/i.test(ua)) os = 'Linux';
  return `${browser} on ${os}`;
}

function fmtBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
}

function fmtEta(seconds) {
  if (!isFinite(seconds) || seconds < 0) return '';
  if (seconds < 60) return `${Math.ceil(seconds)}s left`;
  return `${Math.floor(seconds / 60)}m ${Math.ceil(seconds % 60)}s left`;
}

function fmtClock(ms) {
  const s = Math.max(0, Math.floor(ms / 1000));
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
}

// ------------------------------------------------------------ signaling
function connectSocketOnce() {
  return new Promise((resolve, reject) => {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const sock = new WebSocket(`${proto}://${location.host}`);
    let settled = false;
    sock.onopen = () => { settled = true; resolve(sock); };
    sock.onerror = () => { if (!settled) { settled = true; reject(new Error('signaling-unreachable')); } };
    sock.onclose = () => { if (!settled) { settled = true; reject(new Error('signaling-unreachable')); } };
  });
}

// Some hosts (free-tier proxies included) drop the occasional WebSocket
// upgrade for no real reason. A handshake that fails once is often fine on
// the next try, so retry a few times with backoff before telling the user
// their network is the problem.
const SIGNAL_RETRY_DELAYS_MS = [0, 500, 1500];

async function openSignaling() {
  let lastErr;
  for (const delay of SIGNAL_RETRY_DELAYS_MS) {
    if (delay) await new Promise((r) => setTimeout(r, delay));
    try {
      const sock = await connectSocketOnce();
      sock.onmessage = (ev) => {
        let msg;
        try { msg = JSON.parse(ev.data); } catch { return; }
        handleSignalMessage(msg);
      };
      sock.onerror = () => {}; // post-connect transport errors surface via onclose
      sock.onclose = () => {
        // Only fatal if we were mid-pairing; once WebRTC is up the socket is optional.
        if (appState === 'hosting' || appState === 'joining' || appState === 'connecting') {
          fail('Lost the signaling connection before the peers could link.');
        }
      };
      ws = sock;
      return sock;
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr;
}

function sig(msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function handleSignalMessage(msg) {
  switch (msg.t) {
    case 'created': {
      const codeEl = $('#code-display');
      codeEl.textContent = '';
      for (const ch of msg.code) {
        const d = document.createElement('span');
        d.className = 'digit';
        d.textContent = ch;
        codeEl.appendChild(d);
      }
      codeEl.dataset.code = msg.code;
      codeEl.setAttribute('aria-label', `Room code ${msg.code.split('').join(' ')}`);
      startTtlCountdown(msg.ttl);
      setState('hosting');
      announce(`Room ready. Your code is ${msg.code.split('').join(', ')}.`);
      break;
    }
    case 'peer-joined': // host side
      $('#peer-desc').textContent = (msg.peer && msg.peer.ua) || 'a device';
      stopTtlCountdown();
      startWebRTC(true);
      break;
    case 'joined': // guest side
      $('#peer-desc').textContent = (msg.peer && msg.peer.ua) || 'a device';
      startWebRTC(false);
      break;
    case 'signal':
      onRemoteSignal(msg.d);
      break;
    case 'expired':
      cleanupPeer();
      setState('idle');
      showGateError('That room expired before anyone joined. Start a new one.');
      break;
    case 'peer-left':
      if (appState === 'connected') {
        // The data channel result is authoritative; ws notice is a fallback.
        if (!dc || dc.readyState !== 'open') peerGone();
      } else if (appState === 'connecting') {
        fail('The other device left during the handshake.');
      }
      break;
    case 'error': {
      const text = {
        'not-found': 'No room with that code. Check the digits and try again.',
        'full': 'That room already has two devices.',
        'busy': 'The server is at capacity right now. Try again in a minute.',
        'too-many-attempts': 'Too many wrong codes. Reload the page and try again.',
      }[msg.code] || 'Something went wrong on the signaling server.';
      cleanupPeer();
      setState('idle');
      showGateError(text);
      break;
    }
  }
}

// --------------------------------------------------------------- webrtc
function startWebRTC(host) {
  isHost = host;
  setState('connecting');
  announce('Peer found. Linking devices.');
  pendingCandidates = [];

  pc = new RTCPeerConnection(RTC_CONFIG);

  pc.onicecandidate = (ev) => {
    if (ev.candidate) sig({ t: 'signal', d: { candidate: ev.candidate } });
  };

  pc.onconnectionstatechange = () => {
    if (!pc) return;
    if (pc.connectionState === 'connected') {
      clearTimeout(connectTimer);
      clearTimeout(graceTimer);
      graceTimer = null;
    } else if (pc.connectionState === 'failed') {
      clearTimeout(graceTimer);
      if (appState === 'connected') peerGone();
      else fail('Could not establish a direct connection. A restrictive network (symmetric NAT) may require a TURN relay, which this demo does not include.');
    } else if (pc.connectionState === 'disconnected') {
      // 'disconnected' is often transient (a dropped packet burst, a Wi-Fi
      // handover). Give ICE a grace window to recover before giving up.
      if (graceTimer) return;
      graceTimer = setTimeout(() => {
        graceTimer = null;
        if (!pc) return;
        if (pc.connectionState === 'disconnected' || pc.connectionState === 'failed') {
          if (appState === 'connected') peerGone();
          else fail('The connection was lost during the handshake.');
        }
      }, GRACE_MS);
    }
  };

  if (host) {
    dc = pc.createDataChannel('files', { ordered: true });
    wireDataChannel();
    pc.createOffer()
      .then((offer) => pc.setLocalDescription(offer))
      .then(() => sig({ t: 'signal', d: { sdp: pc.localDescription } }))
      .catch(() => fail('Failed to create a connection offer.'));
  } else {
    pc.ondatachannel = (ev) => { dc = ev.channel; wireDataChannel(); };
  }

  connectTimer = setTimeout(() => {
    if (appState === 'connecting') {
      fail('Linking timed out. Both devices are online, but a direct path could not be found.');
    }
  }, CONNECT_TIMEOUT_MS);
}

async function onRemoteSignal(d) {
  if (!pc || !d) return;
  try {
    if (d.sdp) {
      await pc.setRemoteDescription(new RTCSessionDescription(d.sdp));
      for (const c of pendingCandidates) await pc.addIceCandidate(c).catch(() => {});
      pendingCandidates = [];
      if (d.sdp.type === 'offer') {
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        sig({ t: 'signal', d: { sdp: pc.localDescription } });
      }
    } else if (d.candidate) {
      if (pc.remoteDescription) await pc.addIceCandidate(d.candidate).catch(() => {});
      else pendingCandidates.push(d.candidate);
    }
  } catch (err) {
    fail('The WebRTC handshake failed.');
  }
}

function wireDataChannel() {
  dc.binaryType = 'arraybuffer';
  dc.bufferedAmountLowThreshold = LOW_WATER;

  dc.onopen = () => {
    clearTimeout(connectTimer);
    setState('connected');
    announce(`Connected to ${$('#peer-desc').textContent}. You can now send files.`);
    if (pendingSharedFiles.length) {
      enqueueFiles(pendingSharedFiles);
      pendingSharedFiles = [];
    }
  };

  dc.onclose = () => { if (appState === 'connected') peerGone(); };

  dc.onmessage = (ev) => {
    if (typeof ev.data === 'string') handleControl(JSON.parse(ev.data));
    else handleChunk(ev.data);
  };
}

function peerGone() {
  failActiveTransfers('The connection dropped mid-transfer.');
  cleanupPeer();
  $('#dead-text').textContent = 'The other device disconnected.';
  setState('disconnected');
  announce('The other device disconnected.');
}

function fail(text) {
  clearTimeout(connectTimer);
  failActiveTransfers(text);
  cleanupPeer();
  $('#dead-text').textContent = text;
  setState('error', 'Error');
  announce(text);
}

function cleanupPeer() {
  clearTimeout(connectTimer);
  clearTimeout(graceTimer);
  graceTimer = null;
  stopTtlCountdown();
  if (dc) { try { dc.close(); } catch {} dc = null; }
  if (pc) { try { pc.close(); } catch {} pc = null; }
  if (ws) { ws.onclose = null; try { ws.close(); } catch {} ws = null; }
  sending = null;
  receiving = null;
  sendQueue = [];
}

// --------------------------------------------------------- send pipeline
function enqueueFiles(files) {
  const list = Array.from(files).filter((f) => f.size >= 0);
  if (!list.length) return;
  if (!dc || dc.readyState !== 'open') {
    announce('Connect to a peer before sending files.');
    return;
  }
  for (const file of list) {
    const item = {
      id: `s${++transferSeq}-${Date.now()}`,
      dir: 'send',
      file,
      name: file.name,
      size: file.size,
      done: 0,
      status: 'queued',
      samples: [],
      lastMilestone: 0,
    };
    transfers.set(item.id, item);
    renderTransfer(item);
    sendQueue.push(item);
  }
  announce(`${list.length === 1 ? `Queued ${list[0].name}` : `Queued ${list.length} files`} for sending.`);
  pumpQueue();
}

async function pumpQueue() {
  if (sending || !sendQueue.length) return;
  if (!dc || dc.readyState !== 'open') return;

  const item = sendQueue.shift();
  if (item.status !== 'queued') { pumpQueue(); return; }
  sending = item;
  setItemStatus(item, 'active');
  dc.send(JSON.stringify({ t: 'meta', id: item.id, name: item.name, size: item.size, mime: item.file.type || 'application/octet-stream' }));

  try {
    let offset = 0;
    while (offset < item.size) {
      if (item.status === 'cancelled') break;
      if (!dc || dc.readyState !== 'open') throw new Error('channel-closed');
      if (dc.bufferedAmount > HIGH_WATER) await waitForDrain();
      const buf = await item.file.slice(offset, offset + CHUNK_SIZE).arrayBuffer();
      if (item.status === 'cancelled') break;
      dc.send(buf);
      offset += buf.byteLength;
      item.done = offset;
      updateProgress(item);
    }
    if (item.status !== 'cancelled') {
      dc.send(JSON.stringify({ t: 'done', id: item.id }));
      // Completion is confirmed by the receiver's 'received' ack.
    }
  } catch (err) {
    if (item.status !== 'cancelled') {
      setItemStatus(item, 'failed');
      announce(`Sending ${item.name} failed.`);
    }
  } finally {
    sending = null;
    pumpQueue();
  }
}

function waitForDrain() {
  return new Promise((resolve, reject) => {
    if (!dc || dc.readyState !== 'open') { reject(new Error('channel-closed')); return; }
    const timer = setTimeout(() => {
      dc && dc.removeEventListener('bufferedamountlow', onLow);
      reject(new Error('send-stalled'));
    }, STALL_TIMEOUT_MS);
    const onLow = () => {
      clearTimeout(timer);
      resolve();
    };
    dc.addEventListener('bufferedamountlow', onLow, { once: true });
  });
}

// ------------------------------------------------------ receive pipeline
function handleControl(msg) {
  switch (msg.t) {
    case 'meta': {
      // Protocol guard: a new file announced while one is mid-receive means the
      // streams desynced. Fail the old transfer instead of corrupting both.
      if (receiving && receiving.status === 'active') {
        receiving.chunks = [];
        setItemStatus(receiving, 'failed');
      }
      const item = {
        id: msg.id,
        dir: 'recv',
        name: String(msg.name || 'file').replace(/[/\\]/g, '_'),
        size: Number(msg.size) || 0,
        mime: msg.mime,
        chunks: [],
        done: 0,
        status: 'active',
        samples: [],
        lastMilestone: 0,
        lastChunkAt: performance.now(),
      };
      transfers.set(item.id, item);
      receiving = item;
      renderTransfer(item);
      announce(`Receiving ${item.name} (${fmtBytes(item.size)}).`);
      if (item.size === 0) finalizeReceive(item);
      break;
    }
    case 'done': {
      const item = transfers.get(msg.id);
      if (item && item.dir === 'recv' && item.status === 'active' && item.done < item.size) {
        // Sender finished but bytes are missing — ordered channel makes this a real error.
        setItemStatus(item, 'failed');
        if (receiving === item) receiving = null;
      }
      break;
    }
    case 'received': { // receiver ack -> sender marks complete
      const item = transfers.get(msg.id);
      if (item && item.dir === 'send') {
        setItemStatus(item, 'complete');
        announce(`${item.name} delivered.`);
      }
      break;
    }
    case 'cancel': {
      const item = transfers.get(msg.id);
      if (!item || item.status === 'complete') break;
      setItemStatus(item, 'cancelled');
      if (item.dir === 'recv') { item.chunks = []; if (receiving === item) receiving = null; }
      if (sending === item) item.status = 'cancelled'; // send loop checks this flag
      announce(`${item.name} was cancelled by the other device.`);
      break;
    }
  }
}

function handleChunk(buf) {
  const item = receiving;
  if (!item || item.status !== 'active') return; // late chunks after cancel: drop
  item.chunks.push(buf);
  item.done += buf.byteLength;
  item.lastChunkAt = performance.now();
  updateProgress(item);
  if (item.done >= item.size) finalizeReceive(item);
}

function finalizeReceive(item) {
  receiving = null;
  const blob = new Blob(item.chunks, { type: item.mime || 'application/octet-stream' });
  item.chunks = [];
  const url = URL.createObjectURL(blob);
  const el = item.el;
  const save = el.querySelector('.transfer-save');
  save.href = url;
  save.download = item.name;
  save.hidden = false;
  setItemStatus(item, 'complete');
  dc && dc.readyState === 'open' && dc.send(JSON.stringify({ t: 'received', id: item.id }));
  announce(`${item.name} received. Download starting.`);
  save.click(); // auto-download; the Save link stays for browsers that block this
}

// Watchdog: a receive that stops making progress fails instead of hanging.
setInterval(() => {
  if (receiving && receiving.status === 'active' &&
      performance.now() - receiving.lastChunkAt > STALL_TIMEOUT_MS) {
    const item = receiving;
    receiving = null;
    item.chunks = [];
    setItemStatus(item, 'failed');
    announce(`Receiving ${item.name} stalled and was stopped.`);
  }
}, 5000);

// -------------------------------------------------------------- transfer UI
function renderTransfer(item) {
  const node = tplTransfer.content.firstElementChild.cloneNode(true);
  node.querySelector('.transfer-name').textContent = item.name;
  node.querySelector('.transfer-dir').textContent = item.dir === 'send' ? 'Sending' : 'Receiving';
  node.querySelector('.transfer-size').textContent = fmtBytes(item.size);
  node.querySelector('.ring-wrap').setAttribute('aria-label', `${item.name} transfer progress`);

  node.querySelector('.transfer-cancel').addEventListener('click', () => cancelTransfer(item));
  node.querySelector('.transfer-retry').addEventListener('click', () => retryTransfer(item));

  item.el = node;
  transfersList.prepend(node);
  setItemStatus(item, item.status);
}

function setItemStatus(item, status) {
  item.status = status;
  if (!item.el) return;
  item.el.dataset.status = status;
  const cancellable = status === 'queued' || status === 'active';
  item.el.querySelector('.transfer-cancel').hidden = !cancellable;
  item.el.querySelector('.transfer-retry').hidden =
    !(item.dir === 'send' && (status === 'failed' || status === 'cancelled') && dc && dc.readyState === 'open');
  if (status === 'complete') {
    setRing(item, 100);
    item.el.querySelector('.transfer-stats').textContent = '';
  }
  if (status === 'failed' || status === 'cancelled') {
    item.el.querySelector('.transfer-stats').textContent = '';
  }
}

function setRing(item, pct) {
  const C = 125.664;
  item.el.querySelector('.ring-fill').style.strokeDashoffset = String(C * (1 - pct / 100));
  item.el.querySelector('.ring-pct').textContent = String(Math.floor(pct));
  const wrap = item.el.querySelector('.ring-wrap');
  wrap.setAttribute('aria-valuenow', String(Math.floor(pct)));
  wrap.setAttribute('aria-valuetext', `${Math.floor(pct)} percent`);
}

function updateProgress(item) {
  const pct = item.size ? (item.done / item.size) * 100 : 100;
  setRing(item, pct);

  // rolling 3-second window for speed
  const now = performance.now();
  item.samples.push([now, item.done]);
  while (item.samples.length > 2 && now - item.samples[0][0] > 3000) item.samples.shift();
  const [t0, b0] = item.samples[0];
  const dt = (now - t0) / 1000;
  if (dt > 0.3) {
    const speed = (item.done - b0) / dt; // bytes/sec
    const eta = speed > 0 ? (item.size - item.done) / speed : Infinity;
    item.el.querySelector('.transfer-stats').textContent =
      `${fmtBytes(speed)}/s · ${fmtEta(eta)}`;
  }

  // throttled screen-reader milestones: 25 / 50 / 75 / 100
  const milestone = Math.floor(pct / 25) * 25;
  if (milestone > item.lastMilestone && milestone < 100) {
    item.lastMilestone = milestone;
    announce(`${item.name}: ${milestone} percent ${item.dir === 'send' ? 'sent' : 'received'}.`);
  }
}

function cancelTransfer(item) {
  if (item.status !== 'queued' && item.status !== 'active') return;
  setItemStatus(item, 'cancelled');
  if (item.dir === 'recv') { item.chunks = []; if (receiving === item) receiving = null; }
  if (dc && dc.readyState === 'open') dc.send(JSON.stringify({ t: 'cancel', id: item.id }));
  announce(`${item.name} cancelled.`);
}

function retryTransfer(item) {
  if (item.dir !== 'send' || !dc || dc.readyState !== 'open') return;
  item.el.remove();
  transfers.delete(item.id);
  enqueueFiles([item.file]);
}

function failActiveTransfers(reason) {
  for (const item of transfers.values()) {
    if (item.status === 'active' || item.status === 'queued') {
      if (item.dir === 'recv') item.chunks = [];
      setItemStatus(item, 'failed');
    }
  }
  receiving = null;
}

// ------------------------------------------------------------- TTL clock
let ttlDeadline = 0;
function startTtlCountdown(ttl) {
  ttlDeadline = Date.now() + ttl;
  const tick = () => {
    const left = ttlDeadline - Date.now();
    $('#ttl').textContent = left > 0 ? `· expires in ${fmtClock(left)}` : '';
  };
  tick();
  ttlTimer = setInterval(tick, 1000);
}
function stopTtlCountdown() {
  clearInterval(ttlTimer);
  $('#ttl').textContent = '';
}

// ----------------------------------------------------------------- theme
const themeBtn = $('#btn-theme');
const themeMeta = document.querySelector('meta[name="theme-color"]');

function applyTheme(theme, persist) {
  document.documentElement.dataset.theme = theme;
  if (persist) localStorage.setItem('beacon-theme', theme);
  themeMeta.content = theme === 'dark' ? '#0C0F0D' : '#F2F4F1';
  const next = theme === 'dark' ? 'light' : 'dark';
  themeBtn.textContent = next === 'dark' ? 'Dark' : 'Light';
  themeBtn.setAttribute('aria-label', `Switch to ${next} theme`);
}

applyTheme(document.documentElement.dataset.theme || 'dark', false);

themeBtn.addEventListener('click', () => {
  const current = document.documentElement.dataset.theme;
  applyTheme(current === 'dark' ? 'light' : 'dark', true);
});

// ------------------------------------------------------------ UI wiring
function showGateError(text) {
  const el = $('#gate-error');
  el.textContent = text;
  el.hidden = !text;
}

$('#btn-host').addEventListener('click', async () => {
  showGateError('');
  setState('joining', 'Contacting server');
  try {
    await openSignaling();
    sig({ t: 'create', info: { ua: deviceSummary(navigator.userAgent) } });
  } catch {
    setState('idle');
    showGateError('Could not reach the signaling server. It may be blocked on this network.');
  }
});

$('#join-form').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  showGateError('');
  const code = $('#join-code').value.trim();
  if (!/^\d{4}$/.test(code)) {
    showGateError('Enter the 4-digit code shown on the other device.');
    $('#join-code').focus();
    return;
  }
  setState('joining', 'Joining room');
  try {
    await openSignaling();
    sig({ t: 'join', code, info: { ua: deviceSummary(navigator.userAgent) } });
  } catch {
    setState('idle');
    showGateError('Could not reach the signaling server. It may be blocked on this network.');
  }
});

$('#btn-copy').addEventListener('click', async () => {
  const code = $('#code-display').dataset.code || '';
  const btn = $('#btn-copy');
  try {
    await navigator.clipboard.writeText(code);
    btn.textContent = 'Copied';
    announce('Code copied to clipboard.');
  } catch {
    btn.textContent = 'Copy failed';
  }
  setTimeout(() => { btn.textContent = 'Copy code'; }, 1600);
});

$('#btn-cancel-host').addEventListener('click', () => {
  cleanupPeer();
  setState('idle');
});

$('#btn-leave').addEventListener('click', () => {
  cleanupPeer();
  $('#dead-text').textContent = 'You disconnected.';
  setState('disconnected', 'Disconnected');
});

$('#btn-restart').addEventListener('click', () => {
  transfersList.innerHTML = '';
  transfers.clear();
  showGateError('');
  setState('idle');
  $('#btn-host').focus();
});

// file picking (keyboard-first: a real button + hidden input)
const fileInput = $('#file-input');
$('#btn-browse').addEventListener('click', () => fileInput.click());
fileInput.addEventListener('change', () => {
  enqueueFiles(fileInput.files);
  fileInput.value = '';
});

// drag & drop: whole page is a target while connected
let dragDepth = 0;
window.addEventListener('dragenter', (ev) => {
  if (appState !== 'connected') return;
  ev.preventDefault();
  dragDepth++;
  document.body.classList.add('dragging');
});
window.addEventListener('dragover', (ev) => {
  if (appState !== 'connected') return;
  ev.preventDefault();
});
window.addEventListener('dragleave', () => {
  if (--dragDepth <= 0) { dragDepth = 0; document.body.classList.remove('dragging'); }
});
window.addEventListener('drop', (ev) => {
  if (appState !== 'connected') return;
  ev.preventDefault();
  dragDepth = 0;
  document.body.classList.remove('dragging');
  if (ev.dataTransfer && ev.dataTransfer.files.length) enqueueFiles(ev.dataTransfer.files);
});

// --------------------------------------------------- share sheet (PWA)
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('/sw.js').catch(() => {});
  navigator.serviceWorker.addEventListener('message', (ev) => {
    if (ev.data && ev.data.type === 'shared-files' && Array.isArray(ev.data.files)) {
      if (dc && dc.readyState === 'open') enqueueFiles(ev.data.files);
      else {
        pendingSharedFiles = pendingSharedFiles.concat(ev.data.files);
        announce('Files received from the share sheet. They will be sent once you connect to a peer.');
      }
    }
  });
}

// ------------------------------------------------- capability detection
(function checkSupport() {
  const problems = [];
  if (!window.RTCPeerConnection) problems.push('WebRTC is not available in this browser');
  if (!window.WebSocket) problems.push('WebSockets are not available in this browser');
  if (problems.length) {
    const el = $('#support-warning');
    el.textContent = `${problems.join(' and ')}. File Beacon needs both. Try a current version of Chrome, Firefox, or Safari, or a less restrictive network.`;
    el.hidden = false;
    $('#btn-host').disabled = true;
    $('#join-form').querySelector('button').disabled = true;
  }
})();

setState('idle');

// Exposed for automated testing / debugging only.
window.fileBeacon = { enqueueFiles, get state() { return appState; }, get transfers() { return transfers; } };
