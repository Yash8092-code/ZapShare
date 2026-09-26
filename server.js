import express from 'express';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import os from 'os';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

// Ephemeral rooms in-memory only (zero disk storage, zero persistence)
const rooms = new Map();

// Helper to get local IPv4 address for QR pairing across phone & PC
function getLocalIp() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return 'localhost';
}

const localIp = getLocalIp();

// Serve static assets
app.use(express.static(path.join(__dirname, 'public')));

// Network info API for QR codes and client awareness
app.get('/api/info', (req, res) => {
  const host = req.get('host');
  const isCloud = process.env.VERCEL || (host && !host.startsWith('localhost') && !host.startsWith('127.') && !host.startsWith('192.168.'));
  const proto = req.get('x-forwarded-proto') || (isCloud ? 'https' : 'http');
  const fullUrl = isCloud && host ? `${proto}://${host}` : `http://${localIp}:${PORT}`;

  res.json({
    localIp,
    port: PORT,
    fullUrl
  });
});

// Secure dynamic ICE / TURN configuration endpoint
app.get('/api/ice-servers', (req, res) => {
  const defaultIceServers = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:stun2.l.google.com:19302' },
    { urls: 'stun:stun3.l.google.com:19302' },
    { urls: 'stun:stun4.l.google.com:19302' }
  ];

  // Optional secure authenticated TURN servers from environment (Cloudflare, Metered, coturn, etc.)
  if (process.env.TURN_URL && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL) {
    const urls = process.env.TURN_URL.split(',').map(u => u.trim());
    defaultIceServers.push({
      urls,
      username: process.env.TURN_USERNAME,
      credential: process.env.TURN_CREDENTIAL
    });
  }

  res.json({ iceServers: defaultIceServers });
});

// WebSocket Signaling Logic
wss.on('connection', (ws) => {
  let userPin = null;
  let userRole = null; // 'sender' | 'receiver'

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      switch (msg.type) {
        case 'create-room': {
          const pin = msg.pin;
          userPin = pin;
          userRole = 'sender';

          // Clear previous timeout if any
          if (rooms.has(pin)) {
            const existing = rooms.get(pin);
            clearTimeout(existing.timeout);
          }

          rooms.set(pin, {
            pin,
            sender: ws,
            receiver: null,
            created: Date.now(),
            fileMeta: msg.fileMeta || null,
            timeout: setTimeout(() => {
              rooms.delete(pin);
            }, 10 * 60 * 1000) // 10 minute room expiry
          });

          ws.send(JSON.stringify({ type: 'room-created', pin }));
          break;
        }

        case 'join-room': {
          const pin = msg.pin;
          const room = rooms.get(pin);

          if (!room || !room.sender || room.sender.readyState !== WebSocket.OPEN) {
            ws.send(JSON.stringify({
              type: 'error',
              code: 'ROOM_NOT_FOUND',
              message: 'Invalid or expired 6-digit PIN. Please re-check or generate a new code.'
            }));
            return;
          }

          userPin = pin;
          userRole = 'receiver';
          room.receiver = ws;

          // Notify sender that receiver has arrived
          room.sender.send(JSON.stringify({
            type: 'peer-joined',
            role: 'receiver',
            senderInfo: msg.clientInfo || { device: 'Unknown Device' }
          }));

          // Notify receiver with sender's file metadata if available
          ws.send(JSON.stringify({
            type: 'room-joined',
            pin,
            fileMeta: room.fileMeta
          }));
          break;
        }

        case 'update-meta': {
          if (userPin && rooms.has(userPin)) {
            const room = rooms.get(userPin);
            room.fileMeta = msg.fileMeta;
            if (room.receiver && room.receiver.readyState === WebSocket.OPEN) {
              room.receiver.send(JSON.stringify({
                type: 'meta-updated',
                fileMeta: msg.fileMeta
              }));
            }
          }
          break;
        }

        case 'signal': {
          // Transparently relay SDP offer/answer or ICE candidate
          if (!userPin || !rooms.has(userPin)) return;
          const room = rooms.get(userPin);
          const target = userRole === 'sender' ? room.receiver : room.sender;

          if (target && target.readyState === WebSocket.OPEN) {
            target.send(JSON.stringify({
              type: 'signal',
              senderRole: userRole,
              data: msg.data
            }));
          }
          break;
        }

        case 'transfer-action': {
          // Accept, decline, or cancel notification
          if (!userPin || !rooms.has(userPin)) return;
          const room = rooms.get(userPin);
          const target = userRole === 'sender' ? room.receiver : room.sender;

          if (target && target.readyState === WebSocket.OPEN) {
            target.send(JSON.stringify({
              type: 'transfer-action',
              action: msg.action,
              reason: msg.reason
            }));
          }
          break;
        }

        case 'transfer-complete': {
          if (userPin && rooms.has(userPin)) {
            const room = rooms.get(userPin);
            // Purge room instantly after transfer complete (Zero retention)
            clearTimeout(room.timeout);
            rooms.delete(userPin);
          }
          break;
        }
      }
    } catch (err) {
      console.error('Error handling WS message:', err);
    }
  });

  ws.on('close', () => {
    if (userPin && rooms.has(userPin)) {
      const room = rooms.get(userPin);
      if (userRole === 'sender') {
        if (room.receiver && room.receiver.readyState === WebSocket.OPEN) {
          room.receiver.send(JSON.stringify({
            type: 'peer-disconnected',
            role: 'sender'
          }));
        }
        clearTimeout(room.timeout);
        rooms.delete(userPin);
      } else if (userRole === 'receiver') {
        if (room.sender && room.sender.readyState === WebSocket.OPEN) {
          room.sender.send(JSON.stringify({
            type: 'peer-disconnected',
            role: 'receiver'
          }));
        }
        room.receiver = null;
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`\n======================================================`);
  console.log(`⚡ ZapShare — Direct P2P File Transfer`);
  console.log(`🚀 Local URL:   http://localhost:${PORT}`);
  console.log(`📱 Network URL: http://${localIp}:${PORT}`);
  console.log(`🛡️  Zero logs, Zero cloud retention, Direct P2P SCTP`);
  console.log(`======================================================\n`);
});
