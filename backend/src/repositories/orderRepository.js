const { performance } = require('node:perf_hooks');
const { withTenantTransaction } = require('../config/db');

async function createQueuedOrder(tenantId, userId, items, totalAmount) {
  return withTenantTransaction(tenantId, async (client) => {
    const { rows: orderRows } = await client.query(
      `INSERT INTO orders (tenant_id, user_id, status, total_amount)
       VALUES ($1, $2, 'QUEUED', $3)
       RETURNING id, tenant_id, user_id, status, total_amount::float8 AS total_amount, created_at`,
      [tenantId, userId, totalAmount]
    );
    const order = orderRows[0];

    const insertedItems = [];
    for (const item of items) {
      const { rows: itemRows } = await client.query(
        `INSERT INTO order_items (
           tenant_id, order_id, product_id, product_name, sku, quantity, unit_price, line_total
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING id, product_id, product_name, sku, quantity, unit_price::float8 AS unit_price, line_total::float8 AS line_total`,
        [
          tenantId,
          order.id,
          item.productId,
          item.productName,
          item.sku,
          item.quantity,
          item.unitPrice,
          item.lineTotal,
        ]
      );
      insertedItems.push(itemRows[0]);
    }

    return {
      ...order,
      items: insertedItems,
    };
  });
}

async function attachKafkaMetadata(tenantId, orderId, { topic, partition, offset }) {
  return withTenantTransaction(tenantId, async (client) => {
    const { rows } = await client.query(
      `UPDATE orders
       SET kafka_topic = $1, kafka_partition = $2, kafka_offset = $3, updated_at = NOW()
       WHERE id = $4 AND tenant_id = $5
       RETURNING id, status, kafka_topic, kafka_partition, kafka_offset`,
      [topic, partition, String(offset), orderId, tenantId]
    );
    return rows[0] || null;
  });
}

async function updateOrderStatus(client, tenantId, orderId, status, failureReason = null) {
  const { rows } = await client.query(
    `UPDATE orders
     SET status = $1, failure_reason = $2, updated_at = NOW()
     WHERE id = $3 AND tenant_id = $4
     RETURNING id, tenant_id, status, failure_reason, total_amount::float8 AS total_amount, updated_at`,
    [status, failureReason, orderId, tenantId]
  );
  return rows[0] || null;
}

async function getOrderById(tenantId, orderId) {
  return withTenantTransaction(tenantId, async (client) => {
    const { rows } = await client.query(
      `SELECT
         o.id,
         o.tenant_id,
         o.user_id,
         o.status,
         o.total_amount::float8 AS total_amount,
         o.kafka_topic,
         o.kafka_partition,
         o.kafka_offset,
         o.failure_reason,
         o.created_at,
         o.updated_at,
         COALESCE(
           json_agg(
             json_build_object(
               'id', oi.id,
               'productId', oi.product_id,
               'productName', oi.product_name,
               'sku', oi.sku,
               'quantity', oi.quantity,
               'unitPrice', oi.unit_price::float8,
               'lineTotal', oi.line_total::float8
             )
           ) FILTER (WHERE oi.id IS NOT NULL),
           '[]'::json
         ) AS items
       FROM orders o
       LEFT JOIN order_items oi
         ON oi.order_id = o.id AND oi.tenant_id = o.tenant_id
       WHERE o.tenant_id = $1 AND o.id = $2
       GROUP BY o.id`,
      [tenantId, orderId]
    );
    return rows[0] || null;
  });
}

/**
 * Optimized Query (Lecture 17):
 * Uses a single SQL LEFT JOIN + json_agg with the composite index (tenant_id, created_at DESC)
 * to fetch N orders and all their line items in 1 database round-trip.
 */
async function listOrdersOptimized(tenantId, limit = 25) {
  const start = performance.now();
  const orders = await withTenantTransaction(tenantId, async (client) => {
    const { rows } = await client.query(
      `WITH recent_orders AS (
         SELECT
           id, tenant_id, user_id, status, total_amount,
           kafka_topic, kafka_partition, kafka_offset, failure_reason,
           created_at, updated_at
         FROM orders
         WHERE tenant_id = $1
         ORDER BY created_at DESC
         LIMIT $2
       )
       SELECT
         o.id,
         o.tenant_id,
         o.user_id,
         o.status,
         o.total_amount::float8 AS total_amount,
         o.kafka_topic,
         o.kafka_partition,
         o.kafka_offset,
         o.failure_reason,
         o.created_at,
         o.updated_at,
         COALESCE(
           json_agg(
             json_build_object(
               'id', oi.id,
               'productId', oi.product_id,
               'productName', oi.product_name,
               'sku', oi.sku,
               'quantity', oi.quantity,
               'unitPrice', oi.unit_price::float8,
               'lineTotal', oi.line_total::float8
             )
           ) FILTER (WHERE oi.id IS NOT NULL),
           '[]'::json
         ) AS items
       FROM recent_orders o
       LEFT JOIN order_items oi
         ON oi.order_id = o.id AND oi.tenant_id = o.tenant_id
       GROUP BY
         o.id, o.tenant_id, o.user_id, o.status, o.total_amount,
         o.kafka_topic, o.kafka_partition, o.kafka_offset, o.failure_reason,
         o.created_at, o.updated_at
       ORDER BY o.created_at DESC`,
      [tenantId, limit]
    );
    return rows;
  });

  return {
    strategy: 'OPTIMIZED_SINGLE_JOIN',
    queryCount: 1,
    durationMs: Number((performance.now() - start).toFixed(3)),
    count: orders.length,
    orders,
  };
}

/**
 * Un-optimized N+1 Query (Lecture 17 Benchmark Baseline):
 * Executes 1 query for orders + N individual queries for each order's line items.
 * Intentionally preserved for side-by-side latency & query-count benchmarking.
 */
async function listOrdersUnoptimizedNPlus1(tenantId, limit = 25) {
  const start = performance.now();
  let queryCount = 0;

  const orders = await withTenantTransaction(tenantId, async (client) => {
    const { rows: orderRows } = await client.query(
      `SELECT
         id, tenant_id, user_id, status, total_amount::float8 AS total_amount,
         kafka_topic, kafka_partition, kafka_offset, failure_reason,
         created_at, updated_at
       FROM orders
       WHERE tenant_id = $1
       ORDER BY created_at DESC
       LIMIT $2`,
      [tenantId, limit]
    );
    queryCount += 1;

    const hydrated = [];
    for (const order of orderRows) {
      const { rows: itemRows } = await client.query(
        `SELECT
           id,
           product_id AS "productId",
           product_name AS "productName",
           sku,
           quantity,
           unit_price::float8 AS "unitPrice",
           line_total::float8 AS "lineTotal"
         FROM order_items
         WHERE tenant_id = $1 AND order_id = $2`,
        [tenantId, order.id]
      );
      queryCount += 1;
      hydrated.push({
        ...order,
        items: itemRows,
      });
    }
    return hydrated;
  });

  return {
    strategy: 'UNOPTIMIZED_N_PLUS_1',
    queryCount,
    durationMs: Number((performance.now() - start).toFixed(3)),
    count: orders.length,
    orders,
  };
}

module.exports = {
  createQueuedOrder,
  attachKafkaMetadata,
  updateOrderStatus,
  getOrderById,
  listOrdersOptimized,
  listOrdersUnoptimizedNPlus1,
};
