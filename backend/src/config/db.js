const { Pool } = require('pg');
const config = require('./env');

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const pool = new Pool({
  connectionString: config.databaseUrl,
  max: config.pgPool.max,
  min: config.pgPool.min,
  idleTimeoutMillis: config.pgPool.idleTimeoutMillis,
  connectionTimeoutMillis: config.pgPool.connectionTimeoutMillis,
  ssl: config.pgPool.ssl ? { rejectUnauthorized: false } : false,
});

pool.on('error', (err) => {
  console.error('[pg.Pool] Unexpected idle client error:', err.message);
});

/**
 * Returns live connection pool utilization metrics for monitoring & viva verification.
 */
function getPoolMetrics() {
  return {
    totalCount: pool.totalCount,
    idleCount: pool.idleCount,
    waitingCount: pool.waitingCount,
    maxConnections: config.pgPool.max,
    minConnections: config.pgPool.min,
    idleTimeoutMillis: config.pgPool.idleTimeoutMillis,
    connectionTimeoutMillis: config.pgPool.connectionTimeoutMillis,
  };
}

/**
 * Executes a database operation inside a strict tenant-isolated transaction.
 * Uses PostgreSQL transaction-local `app.current_tenant_id` so pooled connections
 * never leak tenant context across requests.
 */
async function withTenantTransaction(tenantId, callback) {
  if (!tenantId || !UUID_REGEX.test(tenantId)) {
    const err = new Error('Invalid or missing tenant UUID for scoped transaction');
    err.statusCode = 400;
    throw err;
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query("SELECT set_config('app.current_tenant_id', $1, true)", [tenantId]);
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Executes a privileged system transaction (used strictly for auth lookup, seeding,
 * and cross-tenant EXPLAIN ANALYZE benchmarks). Never call with unvalidated user filters.
 */
async function withSystemTransaction(callback) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query("SELECT set_config('app.bypass_rls', 'on', true)");
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

async function checkDatabaseHealth() {
  const start = Date.now();
  await pool.query('SELECT 1');
  return {
    status: 'up',
    latencyMs: Date.now() - start,
    pool: getPoolMetrics(),
  };
}

module.exports = {
  pool,
  UUID_REGEX,
  getPoolMetrics,
  withTenantTransaction,
  withSystemTransaction,
  checkDatabaseHealth,
};
