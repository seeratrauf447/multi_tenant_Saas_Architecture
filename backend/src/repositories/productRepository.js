const { withTenantTransaction } = require('../config/db');

async function listProducts(tenantId) {
  return withTenantTransaction(tenantId, async (client) => {
    const { rows } = await client.query(
      `SELECT id, tenant_id, sku, name, unit_price::float8 AS unit_price, stock, created_at
       FROM products
       WHERE tenant_id = $1
       ORDER BY created_at ASC`,
      [tenantId]
    );
    return rows;
  });
}

async function createProduct(tenantId, { sku, name, unitPrice, stock }) {
  return withTenantTransaction(tenantId, async (client) => {
    const { rows } = await client.query(
      `INSERT INTO products (tenant_id, sku, name, unit_price, stock)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, tenant_id, sku, name, unit_price::float8 AS unit_price, stock, created_at`,
      [tenantId, sku, name, unitPrice, stock]
    );
    return rows[0];
  });
}

async function getProductsByIds(tenantId, productIds) {
  return withTenantTransaction(tenantId, async (client) => {
    const { rows } = await client.query(
      `SELECT id, tenant_id, sku, name, unit_price::float8 AS unit_price, stock
       FROM products
       WHERE tenant_id = $1 AND id = ANY($2::uuid[])`,
      [tenantId, productIds]
    );
    return rows;
  });
}

async function decrementStockAtomic(client, tenantId, productId, quantity) {
  const { rows } = await client.query(
    `UPDATE products
     SET stock = stock - $1
     WHERE id = $2 AND tenant_id = $3 AND stock >= $1
     RETURNING id, sku, name, stock`,
    [quantity, productId, tenantId]
  );
  return rows[0] || null;
}

module.exports = {
  listProducts,
  createProduct,
  getProductsByIds,
  decrementStockAtomic,
};
