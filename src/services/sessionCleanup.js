const cron = require('node-cron');
const { getDb } = require('../firebase/admin');

/**
 * Run every minute. Clean up expired sessions.
 * - Sessions where deleteAt <= now get messages + session deleted.
 */
function startCleanupJobs(io) {
  cron.schedule('* * * * *', async () => {
    try {
      const db = getDb();
      const now = Date.now();

      const expiredSessions = await db
        .collection('sessions')
        .where('status', '!=', 'deleted')
        .where('deleteAt', '<=', now)
        .get();

      for (const sessionDoc of expiredSessions.docs) {
        const sessionId = sessionDoc.id;
        try {
          // Delete messages subcollection
          const msgs = await db.collection('messages').doc(sessionId).collection('msgs').get();
          const batch = db.batch();
          msgs.forEach((msg) => batch.delete(msg.ref));
          batch.update(sessionDoc.ref, { status: 'deleted' });
          await batch.commit();

          console.log(`[Cleanup] Session ${sessionId} deleted.`);
        } catch (err) {
          console.error(`[Cleanup] Error deleting session ${sessionId}:`, err.message);
        }
      }
    } catch (err) {
      console.error('[Cleanup] Cron error:', err.message);
    }
  });

  console.log('[Cleanup] Session cleanup job started.');
}

/**
 * Schedule deletion of a session after a delay (ms).
 * Sets the deleteAt timestamp in Firestore.
 */
async function scheduleSessionDeletion(sessionId, delayMs) {
  const db = getDb();
  const deleteAt = Date.now() + delayMs;
  await db.collection('sessions').doc(sessionId).set({ deleteAt, status: 'ending' }, { merge: true });
}

/**
 * Immediately delete a session (both users skipped all tags).
 */
async function deleteSessionNow(sessionId) {
  const db = getDb();
  const msgs = await db.collection('messages').doc(sessionId).collection('msgs').get();
  const batch = db.batch();
  msgs.forEach((msg) => batch.delete(msg.ref));
  const sessionRef = db.collection('sessions').doc(sessionId);
  batch.update(sessionRef, { status: 'deleted', deleteAt: Date.now() });
  await batch.commit();
}

module.exports = { startCleanupJobs, scheduleSessionDeletion, deleteSessionNow };
