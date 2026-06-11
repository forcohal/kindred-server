const express = require('express');
const router = express.Router();
const { getRecentReports } = require('../services/moderation');

// GET /api/reports/:deviceId — get recent (24h) reports for a device
router.get('/:deviceId', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const reports = await getRecentReports(deviceId);
    res.json({ reports });
  } catch (err) {
    console.error('[GET /reports/:deviceId]', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
