const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../firebase/admin');
const { enqueue, dequeue, findMatch } = require('../services/matchmaking');
const { processReport, checkBan, sanitizeMessage, updateVibeScore } = require('../services/moderation');
const { scheduleSessionDeletion, deleteSessionNow } = require('../services/sessionCleanup');

// ─── In-memory state ─────────────────────────────────────────────
// Map: deviceId -> socketId
const deviceSockets = new Map();
// Map: sessionId -> { user1DeviceId, user2DeviceId }
const activeSessions = new Map();
// Map: deviceId -> reconnect timer
const reconnectTimers = new Map();
// Set: all connected socket IDs (for online counter — Fix 3)
const connectedSocketIds = new Set();

// ─── Bot Profile (Fix 4) ─────────────────────────────────────────
const BOT_DEVICE_ID = '__bot__';
const BOT_PROFILE = {
  name: 'TestBot',
  deviceId: BOT_DEVICE_ID,
  gender: null,
  aboutTags: ['Developer', 'Tester'],
  interestTags: [],
  lookingForTags: ['Deep Conversation'],
  vibe: 'trusted',
  ratingAvg: 5.0,
  ratingCount: 999,
  recentReports: [],
  isBot: true,
};

// ─── Helpers ─────────────────────────────────────────────────────

async function getPartnerProfile(deviceId) {
  // Bot never needs a Firestore lookup
  if (deviceId === BOT_DEVICE_ID) return BOT_PROFILE;

  const db = getDb();
  try {
    const [userDoc, repDoc] = await Promise.all([
      db.collection('users').doc(deviceId).get(),
      db.collection('reputation').doc(deviceId).get(),
    ]);

    const user = userDoc.exists ? userDoc.data() : {};
    const rep  = repDoc.exists  ? repDoc.data()  : {};

    const ratingCount = rep.ratingCount || 0;
    const ratingSum   = rep.ratingSum   || 0;

    return {
      deviceId,
      gender: user.showGender ? user.gender : null,
      aboutTags: user.aboutTags || [],
      interestTags: user.interestTags || [],
      lookingForTags: user.lookingForTags || [],
      vibe: rep.vibeScore || 'neutral',
      ratingAvg: ratingCount > 0 ? Math.round((ratingSum / ratingCount) * 10) / 10 : null,
      ratingCount,
      isBot: false,
    };
  } catch (err) {
    console.error(`[getPartnerProfile] Firestore read failed for ${deviceId}:`, err.message);
    return { deviceId, aboutTags: [], interestTags: [], lookingForTags: [], vibe: 'neutral', isBot: false };
  }
}

async function getRecentReportsForProfile(deviceId) {
  if (deviceId === BOT_DEVICE_ID) return [];
  const db = getDb();
  try {
    const since = Date.now() - 24 * 60 * 60 * 1000;
    const snap  = await db
      .collection('reputation')
      .doc(deviceId)
      .collection('reports')
      .where('timestamp', '>=', since)
      .get();

    const grouped = {};
    snap.forEach((doc) => {
      const { category } = doc.data();
      grouped[category] = (grouped[category] || 0) + 1;
    });
    return Object.entries(grouped).map(([category, count]) => ({ category, count }));
  } catch (err) {
    console.error(`[getRecentReportsForProfile] Firestore read failed for ${deviceId}:`, err.message);
    return [];
  }
}

function emitOnlineCount(io) {
  io.emit('online_count', { count: connectedSocketIds.size });
}

/**
 * Get the current online count (for REST endpoint).
 */
function getOnlineCount() {
  return connectedSocketIds.size;
}

// ─── Main Socket Registration ────────────────────────────────────

function registerSocketEvents(io, socket, db) {
  if (!db) db = getDb(); // fallback

  // Track connection for online counter (Fix 3)
  connectedSocketIds.add(socket.id);
  emitOnlineCount(io);
  // Send current count immediately to this new socket
  socket.emit('online_count', { count: connectedSocketIds.size });

  // ─── JOIN QUEUE ──────────────────────────────────────────────
  socket.on('join_queue', async ({ deviceId, profile }) => {
    if (!deviceId) return;

    console.log(`[Queue] join_queue received — deviceId: ${deviceId}`);

    deviceSockets.set(deviceId, socket.id);
    console.log(`[Socket] deviceId=${deviceId} mapped to socketId=${socket.id}`);

    // Check ban (Bug 5: try/catch)
    try {
      const banStatus = await checkBan(deviceId);
      if (banStatus.banned) {
        socket.emit('banned', { permanent: banStatus.permanent, expiresAt: banStatus.expiresAt });
        console.log(`[Queue] ${deviceId} is banned — rejected from queue`);
        return;
      }
    } catch (err) {
      console.error('[join_queue] Ban check failed:', err.message);
    }

    // Save/update profile in Firestore (Bug 5: try/catch with log)
    try {
      await db.collection('users').doc(deviceId).set(
        {
          gender:         profile.gender         || null,
          showGender:     profile.showGender      || false,
          aboutTags:      profile.aboutTags       || [],
          interestTags:   profile.interestTags    || [],
          lookingForTags: profile.lookingForTags  || [],
          lastSeen: Date.now(),
        },
        { merge: true }
      );
      console.log(`[Queue] Firestore profile written for deviceId=${deviceId}`);
    } catch (err) {
      console.error('[join_queue] Firestore profile write failed:', err.message);
    }

    // Change 5: Ensure reputation document exists for this device
    try {
      const repRef = db.collection('reputation').doc(deviceId);
      const repDoc = await repRef.get();
      if (!repDoc.exists) {
        await repRef.set({
          ratingSum: 0,
          ratingCount: 0,
          vibeScore: 'neutral',
          bans: [],
        });
        console.log(`[Queue] Reputation doc created for deviceId=${deviceId}`);
      }
    } catch (err) {
      console.error('[join_queue] Reputation doc creation failed:', err.message);
    }

    enqueue({ socketId: socket.id, deviceId, profile, socket });

    const matchService = require('../services/matchmaking');
    const queueSize = matchService._getQueue().length;
    console.log(`[Queue] ${deviceId} joined queue. Queue size: ${queueSize}`);

    // Wait messages
    const wait30 = setTimeout(() => {
      socket.emit('wait_message', { type: '30s' });
    }, 30000);

    const wait2min = setTimeout(() => {
      socket.emit('wait_message', { type: '2min' });
    }, 120000);

    const timeout5min = setTimeout(() => {
      dequeue(deviceId);
      socket.emit('queue_timeout', {});
    }, 300000);

    socket._waitTimers = { wait30, wait2min, timeout5min };

    // Bug 2: Bot timer — if still unmatched after 60s, pair with bot
    const botTimer = setTimeout(async () => {
      const matchService = require('../services/matchmaking');
      const stillInQueue = matchService._getQueue().find((e) => e.deviceId === deviceId);
      if (!stillInQueue) return; // already matched

      console.log(`[Bot] ⚡ No match found for ${deviceId} after 60s — pairing with bot`);

      // Remove from queue and cancel timers
      dequeue(deviceId);
      clearSocketTimers(socket);

      // Create bot session
      const sessionId = uuidv4();
      activeSessions.set(sessionId, {
        user1DeviceId: deviceId,
        user2DeviceId: BOT_DEVICE_ID,
      });

      try {
        await db.collection('sessions').doc(sessionId).set({
          user1DeviceId: deviceId,
          user2DeviceId: BOT_DEVICE_ID,
          startedAt: Date.now(),
          status: 'active',
          deleteAt: null,
        });
      } catch (err) {
        console.error('[Bot] Firestore session write failed:', err.message);
      }

      socket.emit('match_found', {
        sessionId,
        partnerProfile: BOT_PROFILE,
      });
      console.log(`[Bot] match_found emitted to ${deviceId} with bot profile`);
    }, 60000);

    socket._botTimer = botTimer;

    // Try matching immediately
    attemptMatch(deviceId, io, db);
    // Run again after 2s to catch second user joining
    setTimeout(() => attemptMatch(deviceId, io, db), 2000);
  });

  // ─── LEAVE QUEUE ────────────────────────────────────────────
  socket.on('leave_queue', ({ deviceId }) => {
    if (!deviceId) return;
    dequeue(deviceId);
    clearSocketTimers(socket);
  });

  // ─── SEND MESSAGE ────────────────────────────────────────────
  socket.on('send_message', async ({ sessionId, text, senderDeviceId }) => {
    if (!sessionId || !text || !senderDeviceId) return;

    const session = activeSessions.get(sessionId);
    if (!session) return;

    const clean     = sanitizeMessage(text.trim().slice(0, 2000));
    const msgId     = uuidv4();
    const timestamp = Date.now();

    // Store in Firestore
    try {
      await db.collection('messages').doc(sessionId).collection('msgs').doc(msgId).set({
        senderDeviceId,
        text: clean,
        timestamp,
      });
    } catch (err) {
      console.error('[send_message] Firestore write failed:', err.message);
    }

    // Relay to partner (skip if partner is bot — Fix 4)
    const partnerDeviceId =
      session.user1DeviceId === senderDeviceId ? session.user2DeviceId : session.user1DeviceId;

    if (partnerDeviceId !== BOT_DEVICE_ID) {
      const partnerSocketId = deviceSockets.get(partnerDeviceId);
      if (partnerSocketId) {
        io.to(partnerSocketId).emit('message', { msgId, text: clean, senderDeviceId, timestamp });
      }
    }
    // Echo back to sender
    socket.emit('message', { msgId, text: clean, senderDeviceId, timestamp });
  });

  // ─── TYPING ──────────────────────────────────────────────────
  socket.on('typing', ({ sessionId, deviceId }) => {
    const session = activeSessions.get(sessionId);
    if (!session) return;
    const partnerDeviceId =
      session.user1DeviceId === deviceId ? session.user2DeviceId : session.user1DeviceId;
    if (partnerDeviceId === BOT_DEVICE_ID) return; // bot ignores typing
    const partnerSocketId = deviceSockets.get(partnerDeviceId);
    if (partnerSocketId) io.to(partnerSocketId).emit('partner_typing', {});
  });

  // ─── RATE PARTNER ────────────────────────────────────────────
  socket.on('rate_partner', async ({ sessionId, rating, raterDeviceId }) => {
    if (!sessionId || !rating || !raterDeviceId) return;
    const session = activeSessions.get(sessionId);
    if (!session) return;

    const targetDeviceId =
      session.user1DeviceId === raterDeviceId ? session.user2DeviceId : session.user1DeviceId;

    if (targetDeviceId === BOT_DEVICE_ID) {
      // Bot always accepts ratings gracefully — just acknowledge
      socket.emit('rating_saved', { rating });
      return;
    }

    try {
      const repRef = db.collection('reputation').doc(targetDeviceId);
      const repDoc = await repRef.get();
      const rep    = repDoc.exists ? repDoc.data() : {};

      const ratingKey = `rating_${sessionId}_${raterDeviceId}`;
      const prevRating = rep[ratingKey] || null;

      let ratingSum   = rep.ratingSum   || 0;
      let ratingCount = rep.ratingCount || 0;

      if (prevRating !== null) {
        ratingSum = ratingSum - prevRating + rating;
      } else {
        ratingSum  += rating;
        ratingCount += 1;
      }

      await repRef.set({ ratingSum, ratingCount, [ratingKey]: rating }, { merge: true });
      await updateVibeScore(targetDeviceId);
      socket.emit('rating_saved', { rating });
    } catch (err) {
      console.error('[rate_partner] Firestore error:', err.message);
    }
  });

  // ─── REPORT PARTNER ──────────────────────────────────────────
  socket.on('report_partner', async ({ sessionId, category, reporterDeviceId }) => {
    if (!sessionId || !category || !reporterDeviceId) return;
    const session = activeSessions.get(sessionId);
    if (!session) return;

    const targetDeviceId =
      session.user1DeviceId === reporterDeviceId ? session.user2DeviceId : session.user1DeviceId;

    if (targetDeviceId === BOT_DEVICE_ID) {
      socket.emit('report_result', { reported: true }); // bot absorbs reports silently
      return;
    }

    try {
      const result = await processReport({ sessionId, reporterDeviceId, targetDeviceId, category });

      // Change 4: Update vibeScore after every report
      await updateVibeScore(targetDeviceId);

      // Change 4: Fetch updated recent reports and emit them back to reporter
      const updatedReports = await getRecentReportsForProfile(targetDeviceId);
      socket.emit('report_result', { ...result, updatedReports });

      if (result.banned) {
        const targetSocketId = deviceSockets.get(targetDeviceId);
        if (targetSocketId) {
          io.to(targetSocketId).emit('banned', { permanent: result.permanent, expiresAt: result.expiresAt });
        }
      }
    } catch (err) {
      console.error('[report_partner] Firestore error:', err.message);
    }
  });

  // ─── BLOCK PARTNER ───────────────────────────────────────────
  socket.on('block_partner', async ({ sessionId, blockerDeviceId }) => {
    if (!sessionId || !blockerDeviceId) return;
    const session = activeSessions.get(sessionId);
    if (!session) return;

    const targetDeviceId =
      session.user1DeviceId === blockerDeviceId ? session.user2DeviceId : session.user1DeviceId;

    if (targetDeviceId !== BOT_DEVICE_ID) {
      try {
        await db
          .collection('blocks')
          .doc(blockerDeviceId)
          .collection('blocked')
          .doc(targetDeviceId)
          .set({ createdAt: Date.now() });
      } catch (err) {
        console.error('[block_partner] Firestore error:', err.message);
      }
    }
    socket.emit('block_confirmed', {});
  });

  // ─── FIND NEXT ───────────────────────────────────────────────
  socket.on('find_next', async ({ sessionId, deviceId }) => {
    if (!sessionId || !deviceId) return;
    await endSession(sessionId, deviceId, io, db, 'find_next');
    // Re-queue with existing profile
    try {
      const userDoc = await db.collection('users').doc(deviceId).get();
      const profile = userDoc.exists ? userDoc.data() : {};
      enqueue({ socketId: socket.id, deviceId, profile, socket });
      attemptMatch(deviceId, io, db);
    } catch (err) {
      console.error('[find_next] Firestore error:', err.message);
    }
  });

  // ─── END CHAT ────────────────────────────────────────────────
  socket.on('end_chat', async ({ sessionId, deviceId }) => {
    if (!sessionId || !deviceId) return;
    await endSession(sessionId, deviceId, io, db, 'end_chat');
    socket.emit('session_ended', {});
  });

  // ─── RECONNECT SESSION ───────────────────────────────────────
  socket.on('reconnect_session', ({ sessionId, deviceId }) => {
    if (!sessionId || !deviceId) return;
    deviceSockets.set(deviceId, socket.id);

    const timer = reconnectTimers.get(deviceId);
    if (timer) {
      clearTimeout(timer);
      reconnectTimers.delete(deviceId);
    }

    const session = activeSessions.get(sessionId);
    if (!session) return;

    const partnerDeviceId =
      session.user1DeviceId === deviceId ? session.user2DeviceId : session.user1DeviceId;

    if (partnerDeviceId !== BOT_DEVICE_ID) {
      const partnerSocketId = deviceSockets.get(partnerDeviceId);
      if (partnerSocketId) {
        io.to(partnerSocketId).emit('partner_reconnected', {});
      }
    }
  });

  // ─── DISCONNECT ──────────────────────────────────────────────
  socket.on('disconnect', async () => {
    // Remove from connected set (Fix 3)
    connectedSocketIds.delete(socket.id);
    emitOnlineCount(io);

    // Find deviceId for this socket
    let disconnectedDeviceId = null;
    for (const [deviceId, socketId] of deviceSockets.entries()) {
      if (socketId === socket.id) {
        disconnectedDeviceId = deviceId;
        break;
      }
    }

    if (!disconnectedDeviceId) return;

    // Remove from queue if searching
    dequeue(disconnectedDeviceId);
    clearSocketTimers(socket);

    // Check active sessions
    for (const [sessionId, session] of activeSessions.entries()) {
      if (
        session.user1DeviceId === disconnectedDeviceId ||
        session.user2DeviceId === disconnectedDeviceId
      ) {
        const partnerDeviceId =
          session.user1DeviceId === disconnectedDeviceId
            ? session.user2DeviceId
            : session.user1DeviceId;

        // Don't notify bot on disconnect
        if (partnerDeviceId !== BOT_DEVICE_ID) {
          const partnerSocketId = deviceSockets.get(partnerDeviceId);
          if (partnerSocketId) {
            io.to(partnerSocketId).emit('partner_disconnected', {});
          }

          // Give 60s for reconnect
          const timer = setTimeout(async () => {
            reconnectTimers.delete(disconnectedDeviceId);
            const partnerSockId = deviceSockets.get(partnerDeviceId);
            if (partnerSockId) {
              io.to(partnerSockId).emit('partner_left', {
                deleteAt: Date.now() + 5 * 60 * 1000,
              });
            }
            await scheduleSessionDeletion(sessionId, 60 * 1000);
            activeSessions.delete(sessionId);
          }, 60000);

          reconnectTimers.set(disconnectedDeviceId, timer);
        } else {
          // Bot session — just clean up immediately (bot never reconnects)
          try {
            await scheduleSessionDeletion(sessionId, 5 * 60 * 1000);
          } catch (err) {
            console.error('[disconnect] Bot session cleanup error:', err.message);
          }
          activeSessions.delete(sessionId);
        }
        break;
      }
    }

    deviceSockets.delete(disconnectedDeviceId);
  });
}

// ─── Match Helpers ───────────────────────────────────────────────

async function attemptMatch(deviceId, io, db) {
  scanQueueForMatch(io, db);
}

async function scanQueueForMatch(io, db) {
  const matchService = require('../services/matchmaking');
  const queueEntries = matchService._getQueue ? matchService._getQueue() : [];

  console.log(`[Matchmaking] scanQueueForMatch — queue size: ${queueEntries.length}`);

  for (const entry of queueEntries) {
    console.log(`[Matchmaking] Attempting match for ${entry.deviceId}...`);
    const match = await matchService.findMatch(entry, db);
    if (match) {
      console.log(`[Matchmaking] ✅ Match found: ${entry.deviceId} <-> ${match.deviceId}`);
      matchService.dequeue(entry.deviceId);
      matchService.dequeue(match.deviceId);

      clearSocketTimers(entry.socket);
      clearSocketTimers(match.socket);

      const sessionId = uuidv4();

      try {
        const [entryProfile, matchProfile, entryReports, matchReports] = await Promise.all([
          getPartnerProfile(entry.deviceId),
          getPartnerProfile(match.deviceId),
          getRecentReportsForProfile(entry.deviceId),
          getRecentReportsForProfile(match.deviceId),
        ]);

        activeSessions.set(sessionId, {
          user1DeviceId: entry.deviceId,
          user2DeviceId: match.deviceId,
        });

        await db.collection('sessions').doc(sessionId).set({
          user1DeviceId:    entry.deviceId,
          user2DeviceId:    match.deviceId,
          user1LookingFor:  entry.profile.lookingForTags || [],
          user2LookingFor:  match.profile.lookingForTags || [],
          startedAt: Date.now(),
          status:    'active',
          deleteAt:  null,
        });

        io.to(entry.socket.id).emit('match_found', {
          sessionId,
          partnerProfile: { ...matchProfile, recentReports: matchReports },
        });
        io.to(match.socket.id).emit('match_found', {
          sessionId,
          partnerProfile: { ...entryProfile, recentReports: entryReports },
        });
        console.log(`[Matchmaking] match_found emitted — sessionId: ${sessionId}`);
      } catch (err) {
        console.error('[scanQueueForMatch] Error creating session:', err.message);
      }

      break; // one match per scan
    } else {
      console.log(`[Matchmaking] No match found yet for ${entry.deviceId}`);
    }
  }
}

async function endSession(sessionId, leavingDeviceId, io, db, reason) {
  const session = activeSessions.get(sessionId);
  if (!session) return;

  const partnerDeviceId =
    session.user1DeviceId === leavingDeviceId ? session.user2DeviceId : session.user1DeviceId;

  const DELETE_DELAY = 5 * 60 * 1000;

  // Don't notify bot (Fix 4)
  if (partnerDeviceId !== BOT_DEVICE_ID) {
    const partnerSocketId = deviceSockets.get(partnerDeviceId);
    if (partnerSocketId) {
      io.to(partnerSocketId).emit('partner_left', { deleteAt: Date.now() + DELETE_DELAY });
    }
  }

  try {
    await db.collection('sessions').doc(sessionId).set(
      { status: 'ending', endedAt: Date.now() },
      { merge: true }
    );
    await scheduleSessionDeletion(sessionId, DELETE_DELAY);
  } catch (err) {
    console.error('[endSession] Firestore error:', err.message);
  }

  activeSessions.delete(sessionId);
}

function clearSocketTimers(socket) {
  if (socket._waitTimers) {
    clearTimeout(socket._waitTimers.wait30);
    clearTimeout(socket._waitTimers.wait2min);
    clearTimeout(socket._waitTimers.timeout5min);
    socket._waitTimers = null;
  }
  // Clear bot timer (Fix 4)
  if (socket._botTimer) {
    clearTimeout(socket._botTimer);
    socket._botTimer = null;
  }
}

module.exports = { registerSocketEvents, scanQueueForMatch, getOnlineCount };
