/**
 * TikTok LIVE + Saweria -> Socket.IO bridge for the floating overlay app.
 * MULTI-USER VERSION.
 *
 * ============================================================================
 * KENAPA FILE INI DIBUAT (baca dulu sebelum pakai)
 * ============================================================================
 * Versi ASLI file ini nyimpen semua state (koneksi TikTok, Saweria key,
 * hitungan like/follow) di variabel GLOBAL:
 *
 *     let tiktokConnection = null;
 *     let saweriaWebhookMiddleware = null;
 *     let likeCountTotal = 0;
 *     let followCountTotal = 0;
 *
 * Artinya SATU server cuma bisa melayani SATU orang/SATU sesi live dalam
 * satu waktu. Kalau tunnel URL server ini dipakai bareng-bareng oleh
 * beberapa orang (misal karena semua baca URL yang sama dari file yang
 * disinkron ke GitHub), maka:
 *   - Orang B pencet "Hubungkan" TikTok -> koneksi TikTok orang A LANGSUNG
 *     DIPUTUS (connectToTikTok() eksplisit disconnect() koneksi lama).
 *   - Orang B masukin Saweria Stream Key dia -> verifikasi webhook donasi
 *     orang A jadi pakai key orang B -> donasi orang A gagal/salah alamat.
 *   - Hitungan like/follow ke-mix jadi satu angka buat semua orang.
 *
 * FIX DI FILE INI: setiap koneksi Socket.IO (= setiap orang yang buka app
 * overlay) sekarang punya SESSION SENDIRI-SENDIRI (koneksi TikTok sendiri,
 * Saweria key sendiri, hitungan like/follow sendiri). Satu server sekarang
 * bisa melayani banyak orang sekaligus tanpa saling menimpa.
 *
 * ----------------------------------------------------------------------------
 * PERUBAHAN PENTING YANG PERLU DIKETAHUI PEMAKAI:
 * ----------------------------------------------------------------------------
 * 1. Webhook Saweria SEKARANG per-user. Sebelumnya URL webhook di Saweria
 *    diisi:
 *        https://<domain>/webhook
 *    Sekarang HARUS diisi dengan stream key disisipkan di URL-nya:
 *        https://<domain>/webhook/<SAWERIA_STREAM_KEY_KAMU>
 *    Ini supaya server tau donasi itu punya sesi/orang yang mana. Endpoint
 *    lama "/webhook" (tanpa key) masih ada tapi cuma balas error yang
 *    ngingetin buat pakai URL baru ini.
 * 2. Env var TIKTOK_USERNAME / SAWERIA_STREAM_KEY saat start server SUDAH
 *    TIDAK DIPAKAI lagi (karena sekarang setiap sesi diatur lewat app,
 *    bukan lewat env saat server nyala duluan sebelum ada yang connect).
 *    Cukup jalankan tanpa env itu:
 *        node server/websocketServer.multiuser.js
 *    Tiap orang tetap isi username TikTok & Saweria key dari app mereka
 *    masing-masing seperti biasa.
 * 3. Semua event yang dikirim ke app (chat, donation, viewerCount, dst)
 *    SEKARANG cuma dikirim ke socket pemiliknya (io.to(socket.id).emit),
 *    bukan disiarkan ke semua orang yang connect (io.emit) seperti versi
 *    lama. Jadi app A tidak akan lagi kebanjiran event punya app B.
 *
 * Env vars:
 *   PORT                  Port to listen on (default 3000)
 *
 * Events emitted (ke socket pemiliknya saja):
 *   chat / chat_message   { id, username, message, timestamp, avatar? }
 *   donation               { id, username, amount, message, timestamp, source: 'tiktok'|'saweria' }
 *   viewerCount / viewer_count   { count }
 *   likeCount / like_count       { count }
 *   followCount / follow_count   { count }
 *   follow                        { username }
 *   join                          { username, timestamp }
 *   update / live_status  { isLive, title }
 *   sourceStatus           { source: 'tiktok'|'saweria', connected }
 * App -> server events:
 *   setSaweriaKey          { streamKey }
 *   setTiktokUsername       { username }
 */

const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const tiktokLib = require('tiktok-live-connector');
const { createMiddleware } = require('saweria-webhook-express');

const TikTokConnection = tiktokLib.TikTokLiveConnection || tiktokLib.WebcastPushConnection;

const PORT = process.env.PORT || 3000;
const RECONNECT_DELAY_MS = 10000;

const app = express();
app.use(express.json());

const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  cors: { origin: '*' },
});

function log(...args) {
  console.log(`[${new Date().toISOString()}]`, ...args);
}

// ============================================================================
// SESSION MANAGEMENT — inti dari perbaikan multi-user
// ============================================================================
// sessions: socket.id -> Session object (state per orang yang connect)
const sessions = new Map();
// sessionsByStreamKey: saweria streamKey -> socket.id (buat routing webhook)
const sessionsByStreamKey = new Map();

class Session {
  constructor(socket) {
    this.socket = socket;
    this.tiktokConnection = null;
    this.currentTiktokUsername = '';
    this.reconnectTimer = null;
    this.saweriaKey = '';
    this.saweriaMiddleware = null;
    this.saweriaKeySet = false;
    // FIX: total like & follower baru untuk SESI LIVE milik user ini saja.
    this.likeCountTotal = 0;
    this.followCountTotal = 0;
  }
}

function getSession(socketId) {
  return sessions.get(socketId);
}

// ---- Broadcast helpers: sekarang menyasar SATU socket (pemilik sesi), ----
// ---- bukan io.emit() ke semua orang seperti versi lama. ----
function emitTo(session, event, payload) {
  session.socket.emit(event, payload);
}

function broadcastChat(session, payload) {
  emitTo(session, 'chat', payload);
  emitTo(session, 'chat_message', payload);
}

function broadcastDonation(session, payload) {
  emitTo(session, 'donation', payload);
}

function broadcastViewerCount(session, count) {
  emitTo(session, 'viewerCount', { count });
  emitTo(session, 'viewer_count', { count });
}

function broadcastLikeCount(session, count) {
  emitTo(session, 'likeCount', { count });
  emitTo(session, 'like_count', { count });
}

function broadcastFollowCount(session, count) {
  emitTo(session, 'followCount', { count });
  emitTo(session, 'follow_count', { count });
}

function broadcastNewFollower(session, username) {
  emitTo(session, 'follow', { username });
}

function broadcastJoin(session, username) {
  emitTo(session, 'join', { username, timestamp: Date.now() });
}

function broadcastLiveStatus(session, isLive, title) {
  emitTo(session, 'live_status', { isLive, title: title || '' });
  emitTo(session, 'update', { isLive, title });
}

function broadcastSourceStatus(session, source, connected) {
  emitTo(session, 'sourceStatus', { source, connected });
}

// ============================================================================
// TikTok LIVE connection — sekarang per session, bukan global
// ============================================================================
function connectToTikTok(session, username) {
  session.currentTiktokUsername = username;

  if (session.reconnectTimer) {
    clearTimeout(session.reconnectTimer);
    session.reconnectTimer = null;
  }

  // Sesi LIVE baru -> mulai lagi dari 0 buat goal like/follow MILIK SESI INI.
  session.likeCountTotal = 0;
  session.followCountTotal = 0;
  broadcastLikeCount(session, session.likeCountTotal);
  broadcastFollowCount(session, session.followCountTotal);

  if (!username) {
    log(`[${session.socket.id}] No TikTok username set -- idle mode.`);
    broadcastSourceStatus(session, 'tiktok', false);
    return;
  }

  if (session.tiktokConnection) {
    session.tiktokConnection.removeAllListeners?.();
    session.tiktokConnection.disconnect();
    session.tiktokConnection = null;
  }

  log(`[${session.socket.id}] Connecting to TikTok LIVE for @${username}...`);
  const connection = new TikTokConnection(username);
  session.tiktokConnection = connection;

  connection
    .connect()
    .then((state) => {
      log(`[${session.socket.id}] Connected to @${username}`, state?.roomId ? `(room ${state.roomId})` : '');
      broadcastLiveStatus(session, true, state?.roomInfo?.title || `${username}'s live`);
      broadcastSourceStatus(session, 'tiktok', true);
    })
    .catch((err) => {
      log(`[${session.socket.id}] Failed to connect to TikTok LIVE:`, err?.message || err);
      broadcastLiveStatus(session, false, '');
      broadcastSourceStatus(session, 'tiktok', false);
      scheduleReconnect(session, username);
    });

  connection.on('chat', (data) => {
    const username = data.user?.nickname || data.user?.uniqueId || data.nickname || data.uniqueId || 'unknown';
    const userId = data.user?.userId || data.userId || 'u';
    const msgId = data.event?.msgId || data.msgId || Date.now();
    const avatar = data.user?.profilePicture?.urls?.[0] || data.profilePictureUrl;

    broadcastChat(session, {
      id: `${userId}-${msgId}`,
      username: username,
      message: data.comment || '',
      timestamp: Date.now(),
      avatar: avatar,
    });
  });

  connection.on('gift', (data) => {
    const username = data.user?.nickname || data.user?.uniqueId || data.nickname || data.uniqueId || 'unknown';
    const userId = data.user?.userId || data.userId || 'u';

    const isStreakable = data.giftType === 1;
    if (!isStreakable || data.repeatEnd) {
      broadcastDonation(session, {
        id: `${userId}-${Date.now()}`,
        username: username,
        amount: (data.diamondCount || 0) * (data.repeatCount || 1),
        message: data.giftName || 'sent a gift',
        timestamp: Date.now(),
        source: 'tiktok',
      });
    }
  });

  connection.on('roomUser', (data) => {
    if (typeof data.viewerCount === 'number') {
      broadcastViewerCount(session, data.viewerCount);
    }
  });

  connection.on('member', (data) => {
    const username = data.user?.nickname || data.user?.uniqueId || data.nickname || data.uniqueId || 'someone';
    broadcastJoin(session, username);
  });

  connection.on('like', (data) => {
    if (typeof data.totalLikeCount === 'number') {
      session.likeCountTotal = data.totalLikeCount;
    } else {
      session.likeCountTotal += data.likeCount || 0;
    }
    broadcastLikeCount(session, session.likeCountTotal);
  });

  connection.on('social', (data) => {
    const displayType = String(data?.displayType || '').toLowerCase();
    if (displayType.includes('follow')) {
      session.followCountTotal += 1;
      broadcastFollowCount(session, session.followCountTotal);
      const username = data.user?.nickname || data.user?.uniqueId || data.nickname || data.uniqueId || 'someone';
      broadcastNewFollower(session, username);
    }
  });

  connection.on('streamEnd', () => {
    log(`[${session.socket.id}] TikTok LIVE stream ended`);
    broadcastLiveStatus(session, false, '');
  });

  connection.on('disconnected', () => {
    log(`[${session.socket.id}] Disconnected from TikTok LIVE`);
    broadcastSourceStatus(session, 'tiktok', false);
    scheduleReconnect(session, username);
  });

  connection.on('error', (err) => {
    log(`[${session.socket.id}] TikTok LIVE connection error:`, err?.message || err);
  });
}

function scheduleReconnect(session, username) {
  if (username !== session.currentTiktokUsername) return;
  if (session.reconnectTimer) return;
  session.reconnectTimer = setTimeout(() => {
    session.reconnectTimer = null;
    if (username === session.currentTiktokUsername) connectToTikTok(session, username);
  }, RECONNECT_DELAY_MS);
  log(`[${session.socket.id}] Retrying in ${RECONNECT_DELAY_MS / 1000}s...`);
}

// ============================================================================
// Saweria webhook — sekarang di-route per stream key lewat URL
// ============================================================================
// FIX UTAMA MULTI-USER: dulu URL webhook cuma "/webhook" satu-satunya untuk
// SEMUA orang, jadi server gak bisa tau donasi ini punya sesi yang mana
// kalau ada lebih dari satu orang connect. Sekarang stream key disisipkan
// di URL: "/webhook/<streamKey>" -- karena streamKey sudah unik per orang,
// ini otomatis jadi "alamat" khusus tiap sesi tanpa perlu ubah apa pun di
// app overlay-nya sendiri (cuma URL yang didaftarkan ke Saweria yang beda).
function setupSaweriaWebhook(session, streamKey) {
  const key = (streamKey || '').trim();

  // Bersihkan pendaftaran key lama punya sesi ini (kalau ganti key)
  if (session.saweriaKey && sessionsByStreamKey.get(session.saweriaKey) === session.socket.id) {
    sessionsByStreamKey.delete(session.saweriaKey);
  }

  if (!key) {
    session.saweriaMiddleware = null;
    session.saweriaKeySet = false;
    session.saweriaKey = '';
    log(`[${session.socket.id}] Saweria stream key cleared.`);
    broadcastSourceStatus(session, 'saweria', false);
    return;
  }

  session.saweriaKey = key;
  session.saweriaMiddleware = createMiddleware(key);
  session.saweriaKeySet = true;
  sessionsByStreamKey.set(key, session.socket.id);

  log(`[${session.socket.id}] Saweria webhook ready. Set Saweria Webhook Integration URL to: POST /webhook/${key}`);
  broadcastSourceStatus(session, 'saweria', true);
}

app.post('/webhook/:key', (req, res, next) => {
  log(
    `Webhook masuk untuk key=${req.params.key}. Signature:`,
    req.headers['saweria-callback-signature'] || '(TIDAK ADA)',
    '| IP:', req.ip
  );
  next();
}, (req, res, next) => {
  const socketId = sessionsByStreamKey.get(req.params.key);
  const session = socketId ? getSession(socketId) : null;

  if (!session || !session.saweriaMiddleware) {
    log(`Webhook ditolak: tidak ada sesi aktif untuk key=${req.params.key} (app-nya belum "Hubungkan" atau sudah disconnect).`);
    return res.status(404).json({ error: 'No active session for this stream key' });
  }

  req._otterSession = session;
  session.saweriaMiddleware(req, res, next);
}, (req, res) => {
  const session = req._otterSession;
  const d = req.body || {};
  broadcastDonation(session, {
    id: `saweria-${d.id || Date.now()}`,
    username: d.donator_name || d.donatorName || 'Anonymous',
    amount: Number(d.amount_raw ?? d.amount ?? 0),
    message: d.message || '',
    timestamp: Date.now(),
    source: 'saweria',
  });
  log(`[${session.socket.id}] Saweria donation received:`, d.donator_name || 'Anonymous', d.amount_raw ?? d.amount ?? 0);
  res.sendStatus(200);
});

// Endpoint lama tanpa key -- dibiarkan supaya kasih pesan jelas, bukan 404 polos.
app.post('/webhook', (req, res) => {
  res.status(400).json({
    error: 'URL webhook ini sudah tidak dipakai. Gunakan /webhook/<SAWERIA_STREAM_KEY_KAMU> di pengaturan Webhook Integration Saweria.',
  });
});

// ============================================================================
// Socket.IO connection lifecycle -- bikin & bersihin session per orang
// ============================================================================
io.on('connection', (socket) => {
  const session = new Session(socket);
  sessions.set(socket.id, session);
  log(`App connected: ${socket.id} (${sessions.size} client(s) now)`);

  socket.emit('connected', { message: 'Connected to server' });
  socket.emit('sourceStatus', { source: 'tiktok', connected: !!session.tiktokConnection });
  socket.emit('sourceStatus', { source: 'saweria', connected: session.saweriaKeySet });
  socket.emit('likeCount', { count: session.likeCountTotal });
  socket.emit('like_count', { count: session.likeCountTotal });
  socket.emit('followCount', { count: session.followCountTotal });
  socket.emit('follow_count', { count: session.followCountTotal });

  socket.on('setTiktokUsername', (data, ack) => {
    const username = (typeof data === 'string' ? data : data?.username || '').replace(/^@/, '').trim();
    try {
      connectToTikTok(session, username);
      if (typeof ack === 'function') ack({ success: true });
    } catch (err) {
      log(`[${socket.id}] Failed to set TikTok username:`, err?.message || err);
      if (typeof ack === 'function') ack({ success: false, error: err?.message });
    }
  });

  socket.on('setSaweriaKey', (data, ack) => {
    const streamKey = typeof data === 'string' ? data : data?.streamKey;
    try {
      setupSaweriaWebhook(session, streamKey);
      if (typeof ack === 'function') {
        ack({
          success: true,
          // FIX: kasih tau app-nya URL webhook yang perlu didaftarkan ke Saweria,
          // supaya pemakai gampang copy-paste tanpa perlu ngerti detail path-nya.
          webhookPath: session.saweriaKey ? `/webhook/${session.saweriaKey}` : null,
        });
      }
    } catch (err) {
      log(`[${socket.id}] Failed to set Saweria key:`, err?.message || err);
      if (typeof ack === 'function') ack({ success: false, error: err?.message });
    }
  });

  socket.on('disconnect', () => {
    if (session.tiktokConnection) {
      session.tiktokConnection.removeAllListeners?.();
      session.tiktokConnection.disconnect();
    }
    if (session.reconnectTimer) clearTimeout(session.reconnectTimer);
    if (session.saweriaKey && sessionsByStreamKey.get(session.saweriaKey) === socket.id) {
      sessionsByStreamKey.delete(session.saweriaKey);
    }
    sessions.delete(socket.id);
    log(`App disconnected: ${socket.id} (${sessions.size} client(s) left)`);
  });
});

httpServer.listen(PORT, () => {
  log(`WebSocket bridge (multi-user) listening on port ${PORT}`);
});

process.on('SIGINT', () => {
  for (const session of sessions.values()) {
    if (session.tiktokConnection) session.tiktokConnection.disconnect();
  }
  log('Shutting down...');
  process.exit(0);
});
