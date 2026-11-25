const fs = require('node:fs');
const path = require('node:path');
const { pool } = require('../config/db');

async function runMigrations() {
  const schemaPath = path.resolve(__dirname, 'schema.sql');
  const sql = fs.readFileSync(schemaPath, 'utf8');
  const client = await pool.connect();
  try {
    await client.query(sql);
    console.log('[Migrate] Schema and Row-Level Security policies applied successfully.');
  } finally {
    client.release();
  }
}

if (require.main === module) {
  runMigrations()
    .then(() => pool.end())
    .catch((err) => {
      console.error('[Migrate] Failed:', err);
      process.exit(1);
    });
}

module.exports = { runMigrations };
