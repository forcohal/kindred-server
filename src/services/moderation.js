const { getDb } = require('../firebase/admin');

// URL pattern to block in messages
const URL_PATTERN = /((https?:\/\/|www\.)[^\s]+|[a-zA-Z0-9-]+\.(com|net|org|io|co|app|dev|xyz|me|info|edu|gov|ly|gg|to|link)[^\s]*)/gi;

/**
 * Compute total weighted report score for a device ID in the past 24h (all categories combined).
 */
async function getTotalReportWeight(deviceId) {
  const db = getDb();
  const since = Date.now() - 24 * 60 * 60 * 1000;
  const snap = await db
    .collection('reputation')
    .doc(deviceId)
    .collection('reports')
    .where('timestamp', '>=', since)
    .get();

  let totalWeight = 0;
  snap.forEach((doc) => {
    totalWeight += doc.data().weight || 1;
  });
  return totalWeight;
}

/**
 * Process a new report. Returns ban info if a ban is triggered.
 */
async function processReport({ sessionId, reporterDeviceId, targetDeviceId, category }) {
  const db = getDb();

  // Determine report weight based on session quality (simplified: default 1)
  const weight = 1;

  // Check if already reported this session
  const existingReportSnap = await db
    .collection('reputation')
    .doc(targetDeviceId)
    .collection('reports')
    .where('sessionId', '==', sessionId)
    .where('reporterDeviceId', '==', reporterDeviceId)
    .get();

  if (!existingReportSnap.empty) {
    return { alreadyReported: true };
  }

  // Store report
  await db.collection('reputation').doc(targetDeviceId).collection('reports').add({
    category,
    weight,
    sessionId,
    reporterDeviceId,
    timestamp: Date.now(),
  });

  // Recalculate total weighted score across all categories
  const score = await getTotalReportWeight(targetDeviceId);

  if (score >= 5) {
    // Issue ban
    const banResult = await issueBan(targetDeviceId, category);
    return { banned: true, ...banResult };
  }

  return { reported: true };
}

/**
 * Issue a ban to a user. Returns ban details.
 */
async function issueBan(deviceId, reason) {
  const db = getDb();
  const repRef = db.collection('reputation').doc(deviceId);
  const repDoc = await repRef.get();
  const repData = repDoc.exists ? repDoc.data() : {};

  const bans = repData.bans || [];
  const banCount = bans.length;

  let permanent = false;
  let expiresAt = null;

  if (banCount >= 2) {
    // 3rd ban = permanent
    permanent = true;
  } else {
    // 1st or 2nd ban = 24h
    expiresAt = Date.now() + 24 * 60 * 60 * 1000;
  }

  const newBan = { reason, issuedAt: Date.now(), expiresAt, permanent };
  bans.push(newBan);

  await repRef.set({ bans }, { merge: true });

  return { permanent, expiresAt };
}

/**
 * Check if a device is currently banned.
 */
async function checkBan(deviceId) {
  const db = getDb();
  const repDoc = await db.collection('reputation').doc(deviceId).get();
  if (!repDoc.exists) return { banned: false };

  const bans = repDoc.data().bans || [];
  const now = Date.now();

  for (const ban of bans) {
    if (ban.permanent) return { banned: true, permanent: true };
    if (ban.expiresAt && ban.expiresAt > now) {
      return { banned: true, expiresAt: ban.expiresAt };
    }
  }

  return { banned: false };
}

/**
 * Get recent reports (last 24h) for a device, grouped by category.
 */
async function getRecentReports(deviceId) {
  const db = getDb();
  const since = Date.now() - 24 * 60 * 60 * 1000;
  const snap = await db
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
}

/**
 * Block URLs from message text.
 */
function sanitizeMessage(text) {
  return text.replace(URL_PATTERN, '[link removed]');
}

/**
 * Compute vibe score from rating data.
 */
function computeVibeScore(ratingSum, ratingCount) {
  if (ratingCount === 0) return 'neutral';
  const avg = ratingSum / ratingCount;
  if (avg >= 4.0) return 'trusted';
  if (avg >= 2.5) return 'neutral';
  return 'flagged';
}

/**
 * Update vibe score for a device based on current reputation.
 * Reads latest ratingSum/ratingCount from Firestore, computes vibeScore,
 * and saves it back. Creates the document with defaults if it doesn't exist.
 */
async function updateVibeScore(deviceId) {
  const db = getDb();
  const repRef = db.collection('reputation').doc(deviceId);
  const repDoc = await repRef.get();

  let ratingSum = 0;
  let ratingCount = 0;

  if (!repDoc.exists) {
    // Create default reputation document
    await repRef.set({
      ratingSum: 0,
      ratingCount: 0,
      vibeScore: 'neutral',
      bans: [],
    });
  } else {
    const data = repDoc.data();
    ratingSum   = data.ratingSum   || 0;
    ratingCount = data.ratingCount || 0;
  }

  const vibe = computeVibeScore(ratingSum, ratingCount);
  await repRef.set({ vibeScore: vibe }, { merge: true });
}

module.exports = {
  processReport,
  checkBan,
  getRecentReports,
  sanitizeMessage,
  updateVibeScore,
  computeVibeScore,
  issueBan,
};
