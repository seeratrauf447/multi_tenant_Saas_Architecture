const { withSystemTransaction } = require('../config/db');
const orderRepository = require('../repositories/orderRepository');

async function runNPlusOneBenchmark(tenantId, limit = 25) {
  const unoptimized = await orderRepository.listOrdersUnoptimizedNPlus1(tenantId, limit);
  const optimized = await orderRepository.listOrdersOptimized(tenantId, limit);

  const speedup =
    optimized.durationMs > 0
      ? Number((unoptimized.durationMs / optimized.durationMs).toFixed(2))
      : 1;

  return {
    tenantId,
    limit,
    rowCount: optimized.count,
    unoptimized: {
      strategy: unoptimized.strategy,
      queryCount: unoptimized.queryCount,
      durationMs: unoptimized.durationMs,
    },
    optimized: {
      strategy: optimized.strategy,
      queryCount: optimized.queryCount,
      durationMs: optimized.durationMs,
    },
    speedupFactor: speedup,
    queriesSaved: unoptimized.queryCount - optimized.queryCount,
  };
}

/**
 * Runs EXPLAIN (ANALYZE, BUFFERS) on the tenant-scoped order timeline query
 * BEFORE and AFTER the composite index (tenant_id, created_at DESC).
 * Uses a PostgreSQL SAVEPOINT around DROP INDEX so the index is immediately
 * restored cleanly without risking an un-indexed production table.
 */
async function runExplainAnalyzeBenchmark(tenantId) {
  return withSystemTransaction(async (client) => {
    const querySql = `
      EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
      SELECT id, status, total_amount, created_at
      FROM orders
      WHERE tenant_id = $1
      ORDER BY created_at DESC
      LIMIT 50
    `;

    const textQuerySql = `
      EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
      SELECT id, status, total_amount, created_at
      FROM orders
      WHERE tenant_id = $1
      ORDER BY created_at DESC
      LIMIT 50
    `;

    // Ensure composite index exists first for the "after" baseline
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_orders_tenant_created_at
      ON orders (tenant_id, created_at DESC)
    `);
    await client.query('ANALYZE orders');

    // Measure WITH composite index
    const { rows: afterJsonRows } = await client.query(querySql, [tenantId]);
    const { rows: afterTextRows } = await client.query(textQuerySql, [tenantId]);
    const afterPlan = afterJsonRows[0]['QUERY PLAN'][0];

    // Temporarily drop composite index inside a savepoint to measure BEFORE indexing
    await client.query('SAVEPOINT before_drop_index');
    let beforePlan;
    let beforeTextRows;
    try {
      await client.query('DROP INDEX IF EXISTS idx_orders_tenant_created_at');
      // Force planner to evaluate sequential/unindexed scan on current stats
      const { rows: bJson } = await client.query(querySql, [tenantId]);
      const { rows: bText } = await client.query(textQuerySql, [tenantId]);
      beforePlan = bJson[0]['QUERY PLAN'][0];
      beforeTextRows = bText;
    } finally {
      await client.query('ROLLBACK TO SAVEPOINT before_drop_index');
    }

    const { rows: totalCountRows } = await client.query('SELECT COUNT(*)::int AS total FROM orders');
    const beforeMs = Number(beforePlan['Execution Time'] || 0);
    const afterMs = Number(afterPlan['Execution Time'] || 0);

    return {
      tenantId,
      totalOrdersInTable: totalCountRows[0].total,
      compositeIndex: 'idx_orders_tenant_created_at ON orders (tenant_id, created_at DESC)',
      beforeIndexing: {
        nodeType: beforePlan.Plan['Node Type'],
        planningTimeMs: beforePlan['Planning Time'],
        executionTimeMs: beforeMs,
        planText: beforeTextRows.map((r) => r['QUERY PLAN']).join('\n'),
      },
      afterIndexing: {
        nodeType: afterPlan.Plan['Node Type'],
        planningTimeMs: afterPlan['Planning Time'],
        executionTimeMs: afterMs,
        planText: afterTextRows.map((r) => r['QUERY PLAN']).join('\n'),
      },
      improvementFactor: afterMs > 0 ? Number((beforeMs / afterMs).toFixed(2)) : 1,
    };
  });
}

module.exports = {
  runNPlusOneBenchmark,
  runExplainAnalyzeBenchmark,
};
