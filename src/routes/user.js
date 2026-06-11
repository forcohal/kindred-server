const express = require('express');
const router = express.Router();
const { getDb } = require('../firebase/admin');
const { checkBan } = require('../services/moderation');
const { getRecentReports } = require('../services/moderation');
const { getOnlineCount } = require('../socket/events');

// POST /api/user/register — save or update user profile
router.post('/register', async (req, res) => {
  try {
    const { deviceId, gender, showGender, aboutTags, interestTags, lookingForTags } = req.body;
    if (!deviceId) return res.status(400).json({ error: 'deviceId required' });

    const db = getDb();
    await db.collection('users').doc(deviceId).set(
      {
        gender: gender || null,
        showGender: showGender || false,
        aboutTags: aboutTags || [],
        interestTags: interestTags || [],
        lookingForTags: lookingForTags || [],
        updatedAt: Date.now(),
      },
      { merge: true }
    );

    // Check ban
    const banStatus = await checkBan(deviceId);

    res.json({ success: true, banned: banStatus.banned, banDetails: banStatus });
  } catch (err) {
    console.error('[POST /user/register]', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/user/online-count — returns live connected socket count (Fix 3)
// IMPORTANT: must be declared before /:deviceId routes to avoid Express matching 'online-count' as a deviceId
router.get('/online-count', (req, res) => {
  res.json({ count: getOnlineCount() });
});

// GET /api/user/:deviceId/profile — get full reputation profile
router.get('/:deviceId/profile', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const db = getDb();

    const [userDoc, repDoc] = await Promise.all([
      db.collection('users').doc(deviceId).get(),
      db.collection('reputation').doc(deviceId).get(),
    ]);

    const user = userDoc.exists ? userDoc.data() : {};
    const rep = repDoc.exists ? repDoc.data() : {};
    const recentReports = await getRecentReports(deviceId);

    const ratingCount = rep.ratingCount || 0;
    const ratingSum = rep.ratingSum || 0;

    res.json({
      gender: user.gender,
      showGender: user.showGender || false,
      aboutTags: user.aboutTags || [],
      interestTags: user.interestTags || [],
      lookingForTags: user.lookingForTags || [],
      ratingAvg: ratingCount > 0 ? Math.round((ratingSum / ratingCount) * 10) / 10 : null,
      ratingCount,
      vibeScore: rep.vibeScore || 'neutral',
      recentReports,
    });
  } catch (err) {
    console.error('[GET /user/:deviceId/profile]', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/user/:deviceId/ban — check ban status
router.get('/:deviceId/ban', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const banStatus = await checkBan(deviceId);
    res.json(banStatus);
  } catch (err) {
    console.error('[GET /user/:deviceId/ban]', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
