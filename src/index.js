require('dotenv').config();
const http = require('http');
const express = require('express');
const cors = require('cors');
const { Server } = require('socket.io');

const { initFirebase, testWrite } = require('./firebase/admin');
const { registerSocketEvents, scanQueueForMatch } = require('./socket/events');
const { startCleanupJobs } = require('./services/sessionCleanup');
const userRoutes = require('./routes/user');
const reportRoutes = require('./routes/reports');

const PORT = process.env.PORT || 3000;

// ─── Init Firebase ────────────────────────────────────────────────
const db = initFirebase();

// Run startup Firestore test write (Fix 5)
testWrite();

// ─── Express App ──────────────────────────────────────────────────
const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json());

app.get('/health', (req, res) => res.json({ status: 'ok', timestamp: Date.now() }));
app.use('/api/user', userRoutes);
app.use('/api/reports', reportRoutes);

// ─── HTTP + Socket.io ─────────────────────────────────────────────
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
  transports: ['websocket', 'polling'],
  allowEIO3: true,
  pingTimeout: 30000,
  pingInterval: 10000,
});

io.on('connection', (socket) => {
  const remoteAddr = socket.handshake.address;
  console.log(`[Socket] ✅ New connection: socketId=${socket.id} from ${remoteAddr}`);
  registerSocketEvents(io, socket, db);

  // Trigger a match scan on new connection in case someone is waiting
  setTimeout(() => scanQueueForMatch(io, db), 500);
});

// ─── Cron Jobs ────────────────────────────────────────────────────
startCleanupJobs(io);

// ─── Start Server ─────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`[Kindred] Server running on port ${PORT}`);
});
