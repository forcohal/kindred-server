const admin = require('firebase-admin');

let db;

function initFirebase() {
  if (admin.apps.length === 0) {
    try {
      const projectId    = process.env.FIREBASE_PROJECT_ID;
      const clientEmail  = process.env.FIREBASE_CLIENT_EMAIL;
      const privateKey   = (process.env.FIREBASE_PRIVATE_KEY || '').replace(/\\n/g, '\n');

      // Validate credentials before attempting init
      if (!projectId || !clientEmail || !privateKey) {
        console.error('[Firebase] ❌ Missing credentials in environment variables:');
        console.error('  FIREBASE_PROJECT_ID  :', projectId    ? '✅ SET' : '❌ MISSING');
        console.error('  FIREBASE_CLIENT_EMAIL:', clientEmail  ? '✅ SET' : '❌ MISSING');
        console.error('  FIREBASE_PRIVATE_KEY :', privateKey   ? '✅ SET' : '❌ MISSING');
        throw new Error('Firebase credentials incomplete — check .env file');
      }

      admin.initializeApp({
        credential: admin.credential.cert({ projectId, clientEmail, privateKey }),
      });

      console.log(`[Firebase] ✅ Initialized for project: ${projectId}`);
    } catch (err) {
      console.error('[Firebase] ❌ Initialization failed:', err.message);
      throw err;
    }
  }

  db = admin.firestore();
  return db;
}

function getDb() {
  if (!db) throw new Error('Firebase not initialized. Call initFirebase() first.');
  return db;
}

/**
 * Write a test document on startup to verify Firestore connectivity.
 * Check Firebase Console → Firestore → _test/startup to confirm.
 */
async function testWrite() {
  try {
    const testDb = getDb();
    await testDb.collection('_test').doc('startup').set({
      timestamp: Date.now(),
      server: 'kindred',
      message: 'Firestore is connected and working ✅',
    });
    console.log('[Firebase] ✅ Firestore test write succeeded — check _test/startup in console');
  } catch (err) {
    console.error('[Firebase] ❌ Firestore test write FAILED:', err.message);
    console.error('[Firebase] Check service account permissions and Firestore rules');
  }
}

module.exports = { initFirebase, getDb, testWrite };
