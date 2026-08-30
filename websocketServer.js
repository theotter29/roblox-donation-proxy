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
    broadcastChat({
      id: `${data.userId || 'u'}-${data.msgId || Date.now()}`,
      username: data.nickname || data.uniqueId || 'unknown',
      message: data.comment || '',
      timestamp: Date.now(),
      avatar: data.profilePictureUrl,
    });
  });

  tiktokConnection.on('gift', (data) => {
    const isStreakable = data.giftType === 1;
    if (!isStreakable || data.repeatEnd) {
      broadcastDonation({
        id: `${data.userId || 'u'}-${Date.now()}`,
        username: data.nickname || data.uniqueId || 'unknown',
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

app.post('/webhook', (req, res, next) => {
  if (!saweriaWebhookMiddleware) {
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
