/**
 * TikTok LIVE + Saweria -> Socket.IO bridge for the floating overlay app.
 *
 * Run:
 *   TIKTOK_USERNAME=youraccount SAWERIA_STREAM_KEY=yourkey node server/websocketServer.js
 *
 * Env vars:
 *   TIKTOK_USERNAME       TikTok username to watch, without the "@"
 *   SAWERIA_STREAM_KEY    Saweria stream key -- also used to verify webhook signatures.
 *                         Set your Saweria Webhook Integration URL to:
 *                         https://<your-railway-domain>/webhook
 *   PORT                  Port to listen on (default 3000)
 *
 * Events emitted:
 *   chat / chat_message   { id, username, message, timestamp, avatar? }
 *   donation               { id, username, amount, message, timestamp, source: 'tiktok'|'saweria' }
 *   viewerCount / viewer_count   { count }
 *   likeCount / like_count       { count }   -- FIX: total like di sesi LIVE saat ini
 *   followCount / follow_count   { count }   -- FIX: total follower baru di sesi LIVE saat ini
 *   follow                        { username } -- FIX: event per orang yang baru follow
 *   join                          { username, timestamp } -- BARU: orang yang baru masuk room live
 *   update / live_status  { isLive, title }
 *   sourceStatus           { source: 'tiktok'|'saweria', connected }
 * App -> server events:
 *   setSaweriaKey          { streamKey }    Sets the key used to verify Saweria webhooks
 *   setTiktokUsername       { username }    Connects/reconnects to a TikTok LIVE room at runtime
 *
 * NOTE (tiktok-live-connector v2.x):
 *   - The class was renamed from `WebcastPushConnection` to `TikTokLiveConnection`.
 *     We import both names defensively below so this file keeps working whether the
 *     installed version still exposes the old alias or only the new name.
 *   - `sendMessage()` was removed in v2 (not used in this file, so no impact).
 *   - Event names ('chat', 'gift', 'roomUser', 'streamEnd', 'disconnected') and their
 *     payload shapes (data.uniqueId, data.nickname, data.comment, data.giftType, etc.)
 *     are unchanged, so the handlers below did not need to change.
 *   - FIX: added 'like' (WebcastLikeMessage: totalLikeCount, likeCount) and 'social'
 *     (WebcastSocialMessage: displayType containing "follow" for new followers, "share"
 *     for shares -- we only care about follow here) listeners, both new in this file.
 *   - BARU: added 'member' (WebcastMemberMessage, fired when someone joins the room)
 *     listener -> broadcast sebagai event 'join' { username, timestamp }. Ini sebelumnya
 *     TIDAK di-listen sama sekali, makanya nama orang yang join gak pernah nyampe ke app.
 */

const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const tiktokLib = require('tiktok-live-connector');
const { createMiddleware } = require('saweria-webhook-express');

// v2.x renamed WebcastPushConnection -> TikTokLiveConnection. Support either.
const TikTokConnection = tiktokLib.TikTokLiveConnection || tiktokLib.WebcastPushConnection;

const PORT = process.env.PORT || 3000;
const TIKTOK_USERNAME = (process.env.TIKTOK_USERNAME || '').replace(/^@/, '');
const SAWERIA_STREAM_KEY = process.env.SAWERIA_STREAM_KEY || '';
const RECONNECT_DELAY_MS = 10000;

const app = express();
app.use(express.json());

const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  cors: { origin: '*' },
});

let tiktokConnection = null;
let connectedClients = 0;
let reconnectTimer = null;
let currentTiktokUsername = TIKTOK_USERNAME;
let saweriaWebhookMiddleware = null;
let saweriaKeySet = false;

// FIX: total like & follower baru di sesi LIVE yang lagi jalan sekarang.
// Di-reset ke 0 tiap kali connectToTikTok() dipanggil (artinya sesi LIVE baru).
let likeCountTotal = 0;
let followCountTotal = 0;

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

function broadcastChat(payload) {
  io.emit('chat', payload);
  io.emit('chat_message', payload);
}

function broadcastDonation(payload) {
  io.emit('donation', payload);
}

function broadcastViewerCount(count) {
  io.emit('viewerCount', { count });
  io.emit('viewer_count', { count });
}

// FIX: broadcast total like sesi ini (bukan like per klik -- itu terlalu
// ramai buat di-emit satu-satu, jadi kita jumlahkan dan kirim totalnya).
function broadcastLikeCount(count) {
  io.emit('likeCount', { count });
  io.emit('like_count', { count });
}

// FIX: broadcast total follower baru sesi ini, plus event per-orang buat
// yang mau nampilin nama follower terbaru (mis. alert).
function broadcastFollowCount(count) {
  io.emit('followCount', { count });
  io.emit('follow_count', { count });
}

function broadcastNewFollower(username) {
  io.emit('follow', { username });
}

// BARU: broadcast orang yang baru join room live. Dipisah dari 'follow'
// karena join != follow (join = sekadar masuk nonton, follow = pencet
// tombol follow).
function broadcastJoin(username) {
  io.emit('join', { username, timestamp: Date.now() });
}

function broadcastLiveStatus(isLive, title) {
  io.emit('live_status', { isLive, title: title || '' });
  io.emit('update', { isLive, title });
}

function broadcastSourceStatus(source, connected) {
  io.emit('sourceStatus', { source, connected });
}

function connectToTikTok(username) {
  currentTiktokUsername = username;

  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  // FIX: sesi LIVE baru -> mulai lagi dari 0 buat goal like/follow.
  likeCountTotal = 0;
  followCountTotal = 0;
  broadcastLikeCount(likeCountTotal);
  broadcastFollowCount(followCountTotal);

  if (!username) {
    log(
      'No TikTok username set -- running in idle mode.',
      "Set TIKTOK_USERNAME (or use the app's TikTok username field) to connect to a real LIVE room."
    );
    broadcastSourceStatus('tiktok', false);
    return;
  }

  if (tiktokConnection) {
    tiktokConnection.removeAllListeners?.();
    tiktokConnection.disconnect();
    tiktokConnection = null;
  }

  log(`Connecting to TikTok LIVE for @${username}...`);
  tiktokConnection = new TikTokConnection(username);

  tiktokConnection
    .connect()
    .then((state) => {
      log(`Connected to @${username}`, state?.roomId ? `(room ${state.roomId})` : '');
      broadcastLiveStatus(true, state?.roomInfo?.title || `${username}'s live`);
      broadcastSourceStatus('tiktok', true);
    })
    .catch((err) => {
      log('Failed to connect to TikTok LIVE:', err?.message || err);
      broadcastLiveStatus(false, '');
      broadcastSourceStatus('tiktok', false);
      scheduleReconnect(username);
    });

  tiktokConnection.on('chat', (data) => {
    // === FIX: Euler Stream punya struktur nested (user.nickname, event.msgId) ===
    const username = data.user?.nickname || data.user?.uniqueId || data.nickname || data.uniqueId || 'unknown';
    const userId = data.user?.userId || data.userId || 'u';
    const msgId = data.event?.msgId || data.msgId || Date.now();
    const avatar = data.user?.profilePicture?.urls?.[0] || data.profilePictureUrl;

    console.log('[Chat] Username:', username, '| Message:', data.comment?.substring(0, 50));

    broadcastChat({
      id: `${userId}-${msgId}`,
      username: username,
      message: data.comment || '',
      timestamp: Date.now(),
      avatar: avatar,
    });
  });

  tiktokConnection.on('gift', (data) => {
    // === FIX: Euler Stream nested structure ===
    const username = data.user?.nickname || data.user?.uniqueId || data.nickname || data.uniqueId || 'unknown';
    const userId = data.user?.userId || data.userId || 'u';

    const isStreakable = data.giftType === 1;
    if (!isStreakable || data.repeatEnd) {
      broadcastDonation({
        id: `${userId}-${Date.now()}`,
        username: username,
        amount: (data.diamondCount || 0) * (data.repeatCount || 1),
        message: data.giftName || 'sent a gift',
        timestamp: Date.now(),
        source: 'tiktok',
      });
    }
  });

  tiktokConnection.on('roomUser', (data) => {
    if (typeof data.viewerCount === 'number') {
      broadcastViewerCount(data.viewerCount);
    }
  });

  // BARU: WebcastMemberMessage -- ke-trigger tiap ada orang baru masuk
  // room live. Sebelumnya event ini gak pernah di-listen sama sekali,
  // makanya nama yang join gak pernah nyampe ke overlay app.
  // FIX: Support nested Euler Stream structure
  tiktokConnection.on('member', (data) => {
    const username = data.user?.nickname || data.user?.uniqueId || data.nickname || data.uniqueId || 'someone';
    broadcastJoin(username);
  });

  // FIX: WebcastLikeMessage. `totalLikeCount` dari library ini biasanya
  // sudah total sejak sesi konek (bukan cuma batch ini), jadi dipakai
  // langsung kalau ada; kalau versi library gak nyediain itu, fallback
  // ke akumulasi manual pakai `likeCount` (like per tap di batch ini).
  tiktokConnection.on('like', (data) => {
    if (typeof data.totalLikeCount === 'number') {
      likeCountTotal = data.totalLikeCount;
    } else {
      likeCountTotal += data.likeCount || 0;
    }
    broadcastLikeCount(likeCountTotal);
  });

  // FIX: WebcastSocialMessage dipakai buat follow DAN share -- kita cuma
  // hitung yang follow. `displayType` isinya string kayak
  // "pm_main_follow_message_viewer_2", makanya dicek pakai .includes().
  // FIX: Support nested Euler Stream structure
  tiktokConnection.on('social', (data) => {
    const displayType = String(data?.displayType || '').toLowerCase();
    if (displayType.includes('follow')) {
      followCountTotal += 1;
      broadcastFollowCount(followCountTotal);
      const username = data.user?.nickname || data.user?.uniqueId || data.nickname || data.uniqueId || 'someone';
      broadcastNewFollower(username);
    }
  });

  tiktokConnection.on('streamEnd', () => {
    log('TikTok LIVE stream ended');
    broadcastLiveStatus(false, '');
  });

  tiktokConnection.on('disconnected', () => {
    log('Disconnected from TikTok LIVE');
    broadcastSourceStatus('tiktok', false);
    scheduleReconnect(username);
  });

  tiktokConnection.on('error', (err) => {
    log('TikTok LIVE connection error:', err?.message || err);
  });
}

function scheduleReconnect(username) {
  if (username !== currentTiktokUsername) return;
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    if (username === currentTiktokUsername) connectToTikTok(username);
  }, RECONNECT_DELAY_MS);
  log(`Retrying in ${RECONNECT_DELAY_MS / 1000}s...`);
}

// ---- Saweria: webhook-based (replaces the old, unmaintained WebSocket client) ----
function setupSaweriaWebhook(streamKey) {
  const key = (streamKey || '').trim();

  if (!key) {
    saweriaWebhookMiddleware = null;
    saweriaKeySet = false;
    log('Saweria stream key cleared -- webhook verification disabled.');
    broadcastSourceStatus('saweria', false);
    return;
  }

  saweriaWebhookMiddleware = createMiddleware(key);
  saweriaKeySet = true;
  log('Saweria webhook ready. Point your Saweria Webhook Integration URL to POST /webhook on this server.');
  broadcastSourceStatus('saweria', true);
}

// FIX (debug): saweria-webhook-express membalas 403/401 SENDIRI kalau
// header 'Saweria-Callback-Signature' hilang/salah -- itu terjadi SEBELUM
// kode kita sempat log apa pun, jadi selama ini kita buta total soal apakah
// Saweria beneran ngirim request atau enggak. Middleware log kecil ini
// dipasang PALING DEPAN (sebelum verifikasi signature) supaya SETIAP
// request yang masuk ke /webhook selalu kelihatan di log, apa pun hasilnya.
app.post('/webhook', (req, res, next) => {
  log(
    'Webhook request masuk. Header signature:',
    req.headers['saweria-callback-signature'] || '(TIDAK ADA)',
    '| Content-Type:', req.headers['content-type'],
    '| IP:', req.ip
  );
  next();
}, (req, res, next) => {
  if (!saweriaWebhookMiddleware) {
    log('Webhook ditolak: stream key belum di-set di server ini.');
    return res.status(503).json({ error: 'Saweria stream key not configured on this server' });
  }
  saweriaWebhookMiddleware(req, res, next);
}, (req, res) => {
  const d = req.body || {};
  broadcastDonation({
    id: `saweria-${d.id || Date.now()}`,
    username: d.donator_name || d.donatorName || 'Anonymous',
    amount: Number(d.amount_raw ?? d.amount ?? 0),
    message: d.message || '',
    timestamp: Date.now(),
    source: 'saweria',
  });
  log('Saweria donation received:', d.donator_name || 'Anonymous', d.amount_raw ?? d.amount ?? 0);
  res.sendStatus(200);
});

io.on('connection', (socket) => {
  connectedClients += 1;
  log(`App connected (${connectedClients} client(s) now)`);

  socket.emit('connected', { message: 'Connected to server' });
  socket.emit('sourceStatus', { source: 'tiktok', connected: !!tiktokConnection });
  socket.emit('sourceStatus', { source: 'saweria', connected: saweriaKeySet });
  // FIX: klien yang baru connect (mis. buka app pas LIVE udah jalan) perlu
  // tau angka like/follow yang sudah terkumpul, bukan mulai dari 0 di layar.
  socket.emit('likeCount', { count: likeCountTotal });
  socket.emit('like_count', { count: likeCountTotal });
  socket.emit('followCount', { count: followCountTotal });
  socket.emit('follow_count', { count: followCountTotal });

  socket.on('setTiktokUsername', (data, ack) => {
    const username = (typeof data === 'string' ? data : data?.username || '').replace(/^@/, '').trim();
    try {
      connectToTikTok(username);
      if (typeof ack === 'function') ack({ success: true });
    } catch (err) {
      log('Failed to set TikTok username:', err?.message || err);
      if (typeof ack === 'function') ack({ success: false, error: err?.message });
    }
  });

  socket.on('setSaweriaKey', (data, ack) => {
    const streamKey = typeof data === 'string' ? data : data?.streamKey;
    try {
      setupSaweriaWebhook(streamKey);
      if (typeof ack === 'function') ack({ success: true });
    } catch (err) {
      log('Failed to set Saweria key:', err?.message || err);
      if (typeof ack === 'function') ack({ success: false, error: err?.message });
    }
  });

  socket.on('disconnect', () => {
    connectedClients = Math.max(0, connectedClients - 1);
    log(`App disconnected (${connectedClients} client(s) left)`);
  });
});

httpServer.listen(PORT, () => {
  connectToTikTok(TIKTOK_USERNAME);
  if (SAWERIA_STREAM_KEY) setupSaweriaWebhook(SAWERIA_STREAM_KEY);
  log(`WebSocket bridge listening on port ${PORT}`);
});

process.on('SIGINT', () => {
  if (tiktokConnection) tiktokConnection.disconnect();
  log('Shutting down...');
});
