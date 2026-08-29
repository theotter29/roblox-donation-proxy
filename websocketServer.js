/**
 * TikTok LIVE + Saweria -> Socket.IO bridge for the floating overlay app.
 *
 * Run:
 *   TIKTOK_USERNAME=youraccount SAWERIA_STREAM_KEY=yourkey node server/websocketServer.js
 *
 * Env vars:
 *   TIKTOK_USERNAME     TikTok username to watch, without the "@" (required to go live;
 *                       server runs in idle mode without it, useful for testing the app UI)
 *   SAWERIA_STREAM_KEY  Optional -- Saweria stream key (from your Saweria alert widget URL,
 *                       the part after ?streamKey=). Can also be set/changed at runtime from
 *                       the app (Home screen -> Donation Sources), no restart needed.
 *   PORT                Port to listen on (default 3000 -- matches the app's default
 *                       "WebSocket Server URL" field)
 *
 * Events emitted (each broadcast under two names for compatibility with both
 * the main screen's WebSocketContext and floatingOverlayService, which listen
 * for slightly different event names):
 *   chat / chat_message   { id, username, message, timestamp, avatar? }
 *   donation               { id, username, amount, message, timestamp, source: 'tiktok'|'saweria' }
 *   viewerCount / viewer_count   { count }
 *   update / live_status  { isLive, title }
 *   sourceStatus           { source: 'saweria', connected }
 *
 * App -> server events:
 *   setSaweriaKey      { streamKey }   Connects/reconnects the Saweria listener at runtime.
 *   setTiktokUsername  { username }    Connects/reconnects to a TikTok LIVE room at runtime
 *                                      (no server restart needed -- overrides TIKTOK_USERNAME).
 */

const http = require('http');
const { Server } = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const { Client: SaweriaClient } = require('saweria');

const PORT = process.env.PORT || 3000;
const TIKTOK_USERNAME = (process.env.TIKTOK_USERNAME || '').replace(/^@/, '');
const SAWERIA_STREAM_KEY = process.env.SAWERIA_STREAM_KEY || '';
const RECONNECT_DELAY_MS = 10000;

const httpServer = http.createServer();
const io = new Server(httpServer, {
  cors: { origin: '*' },
});

let tiktokConnection = null;
let saweriaClient = null;
let connectedClients = 0;
let reconnectTimer = null;
let currentTiktokUsername = TIKTOK_USERNAME;

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
  io.emit('update', { isLive, title });
  io.emit('live_status', { isLive, title: title || '' });
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
      'Set TIKTOK_USERNAME (or use the app\'s TikTok username field) to connect to a real LIVE room.'
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
  tiktokConnection = new WebcastPushConnection(username);

  tiktokConnection
    .connect()
    .then((state) => {
      log(
        `Connected to @${username}`,
        state?.roomId ? `(room ${state.roomId})` : ''
      );
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
    // giftType 1 gifts can be sent as a "streak" (combo); only broadcast
    // once the streak finishes so the overlay doesn't spam per-tap updates
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
}

function scheduleReconnect(username) {
  // Don't retry a stale username if the user has since switched accounts
  if (username !== currentTiktokUsername) return;
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    if (username === currentTiktokUsername) connectToTikTok(username);
  }, RECONNECT_DELAY_MS);
  log(`Retrying in ${RECONNECT_DELAY_MS / 1000}s...`);
}

function connectToSaweria(streamKey) {
  const key = (streamKey || '').trim();

  if (saweriaClient) {
    try {
      saweriaClient.removeAllListeners?.();
    } catch (e) {
      // ignore
    }
    saweriaClient = null;
  }

  if (!key) {
    log('Saweria stream key cleared -- not listening for Saweria donations.');
    broadcastSourceStatus('saweria', false);
    return;
  }

  log('Connecting to Saweria donation stream...');
  saweriaClient = new SaweriaClient();
  saweriaClient.setStreamKey(key);

  saweriaClient.on('donations', (donations) => {
    (donations || []).forEach((d) => {
      broadcastDonation({
        id: `saweria-${d.id || Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
        username: d.donator_name || d.donatorName || 'Anonymous',
        amount: Number(d.amount_raw ?? d.amount ?? 0),
        message: d.message || '',
        timestamp: Date.now(),
        source: 'saweria',
      });
    });
  });

  // The npm "saweria" client connects lazily on first listener registration;
  // report as connected once we've wired it up (it retries internally).
  broadcastSourceStatus('saweria', true);
}

io.on('connection', (socket) => {
  connectedClients += 1;
  log(`App connected (${connectedClients} client(s) now)`);

  socket.emit('connected', { message: 'Connected to server' });
  // Let a freshly-connected app know current source status right away
  socket.emit('sourceStatus', { source: 'tiktok', connected: !!tiktokConnection });
  socket.emit('sourceStatus', { source: 'saweria', connected: !!saweriaClient });

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
      connectToSaweria(streamKey);
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
  log(`WebSocket bridge listening on port ${PORT}`);
  connectToTikTok(TIKTOK_USERNAME);
  if (SAWERIA_STREAM_KEY) connectToSaweria(SAWERIA_STREAM_KEY);
});

process.on('SIGINT', () => {
  log('Shutting down...');
  if (tiktokConnection) tiktokConnection.disconnect();
  httpServer.close(() => process.exit(0));
});
