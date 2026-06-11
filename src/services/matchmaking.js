/**
 * In-memory matchmaking queue.
 * Each entry: { socketId, deviceId, profile, joinedAt, socket }
 */
const queue = [];

/**
 * Compute tag overlap score between two users.
 * Primary: lookingForTags, Secondary: interestTags.
 */
function computeScore(a, b) {
  const lookingForA = a.profile.lookingForTags || [];
  const lookingForB = b.profile.lookingForTags || [];
  const interestsA = a.profile.interestTags || [];
  const interestsB = b.profile.interestTags || [];

  const lookingForOverlap = lookingForA.filter((t) => lookingForB.includes(t)).length;
  const interestOverlap = interestsA.filter((t) => interestsB.includes(t)).length;

  return lookingForOverlap * 10 + interestOverlap;
}

/**
 * Check if two users have blocked each other.
 */
async function areBlocked(deviceIdA, deviceIdB, db) {
  const [aBlockedB, bBlockedA] = await Promise.all([
    db.collection('blocks').doc(deviceIdA).collection('blocked').doc(deviceIdB).get(),
    db.collection('blocks').doc(deviceIdB).collection('blocked').doc(deviceIdA).get(),
  ]);
  return aBlockedB.exists || bBlockedA.exists;
}

/**
 * Add user to the matchmaking queue.
 */
function enqueue(entry) {
  // Avoid duplicate device IDs
  const existing = queue.findIndex((e) => e.deviceId === entry.deviceId);
  if (existing !== -1) queue.splice(existing, 1);
  queue.push({ ...entry, joinedAt: Date.now() });
}

/**
 * Remove user from the matchmaking queue.
 */
function dequeue(deviceId) {
  const idx = queue.findIndex((e) => e.deviceId === deviceId);
  if (idx !== -1) queue.splice(idx, 1);
}

/**
 * Find best match for a given entry.
 * Any non-blocked user is always a valid match.
 * Tag score only determines priority — not eligibility.
 */
async function findMatch(entry, db) {
  let bestMatch = null;
  let bestScore = -1;

  for (const candidate of queue) {
    if (candidate.deviceId === entry.deviceId) continue;

    const blocked = await areBlocked(entry.deviceId, candidate.deviceId, db);
    if (blocked) continue;

    const score = computeScore(entry, candidate);

    if (score > bestScore) {
      bestScore = score;
      bestMatch = candidate;
    }
  }

  return bestMatch;
}

/**
 * Get queue length.
 */
function queueLength() {
  return queue.length;
}

function _getQueue() {
  return [...queue];
}

module.exports = { enqueue, dequeue, findMatch, queueLength, _getQueue };
