const bcrypt = require('bcryptjs');
const { pool, withSystemTransaction } = require('../config/db');

const TENANTS = [
  {
    id: '11111111-1111-4111-8111-111111111111',
    slug: 'acme-corp',
    name: 'Acme Corp (Tenant A)',
    plan: 'ENTERPRISE',
    user: {
      id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      email: 'admin@acme-corp.io',
      fullName: 'Alice Carter (Acme Admin)',
      password: 'TenantA!Password123',
      role: 'ADMIN',
    },
    products: [
      {
        id: 'a1000000-0000-4000-8000-000000000001',
        sku: 'ACME-K8S-NODE',
        name: 'Managed Kubernetes Node Pack',
        unitPrice: 249.0,
        stock: 500,
      },
      {
        id: 'a1000000-0000-4000-8000-000000000002',
        sku: 'ACME-EDGE-GW',
        name: 'Dedicated Edge API Gateway',
        unitPrice: 129.5,
        stock: 350,
      },
      {
        id: 'a1000000-0000-4000-8000-000000000003',
        sku: 'ACME-KAFKA-STR',
        name: 'Dedicated Kafka Partition Add-on',
        unitPrice: 79.0,
        stock: 800,
      },
    ],
  },
  {
    id: '22222222-2222-4222-8222-222222222222',
    slug: 'globex-inc',
    name: 'Globex Inc (Tenant B)',
    plan: 'BUSINESS',
    user: {
      id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
      email: 'admin@globex-inc.io',
      fullName: 'Bob Vance (Globex Admin)',
      password: 'TenantB!Password123',
      role: 'ADMIN',
    },
    products: [
      {
        id: 'b2000000-0000-4000-8000-000000000001',
        sku: 'GLX-PG-CLUSTER',
        name: 'High-IOPS Postgres Cluster',
        unitPrice: 399.0,
        stock: 200,
      },
      {
        id: 'b2000000-0000-4000-8000-000000000002',
        sku: 'GLX-WAF-SHIELD',
        name: 'Zero-Trust WAF Shield',
        unitPrice: 149.0,
        stock: 450,
      },
      {
        id: 'b2000000-0000-4000-8000-000000000003',
        sku: 'GLX-OBS-TRACE',
        name: 'Distributed Tracing Retention (1TB)',
        unitPrice: 95.0,
        stock: 600,
      },
    ],
  },
];

async function seedDatabase({ bulkOrderCount = 0 } = {}) {
  await withSystemTransaction(async (client) => {
    for (const tenant of TENANTS) {
      await client.query(
        `INSERT INTO tenants (id, slug, name, plan)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, plan = EXCLUDED.plan`,
        [tenant.id, tenant.slug, tenant.name, tenant.plan]
      );

      const passwordHash = await bcrypt.hash(tenant.user.password, 10);
      await client.query(
        `INSERT INTO users (id, tenant_id, email, password_hash, full_name, role)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (tenant_id, email) DO UPDATE SET password_hash = EXCLUDED.password_hash`,
        [
          tenant.user.id,
          tenant.id,
          tenant.user.email,
          passwordHash,
          tenant.user.fullName,
          tenant.user.role,
        ]
      );

      for (const product of tenant.products) {
        await client.query(
          `INSERT INTO products (id, tenant_id, sku, name, unit_price, stock)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (tenant_id, sku) DO UPDATE
             SET name = EXCLUDED.name, unit_price = EXCLUDED.unit_price`,
          [product.id, tenant.id, product.sku, product.name, product.unitPrice, product.stock]
        );
      }

      // Seed 25 realistic initial orders + order items per tenant for immediate N+1 benchmark demo
      const { rows: existingOrders } = await client.query(
        'SELECT COUNT(*)::int AS cnt FROM orders WHERE tenant_id = $1',
        [tenant.id]
      );

      if (existingOrders[0].cnt === 0) {
        for (let i = 0; i < 25; i += 1) {
          const p1 = tenant.products[i % tenant.products.length];
          const p2 = tenant.products[(i + 1) % tenant.products.length];
          const qty1 = (i % 3) + 1;
          const qty2 = (i % 2) + 1;
          const total = Number((qty1 * p1.unitPrice + qty2 * p2.unitPrice).toFixed(2));
          const createdAt = new Date(Date.now() - (25 - i) * 3600 * 1000);

          const { rows: orderRows } = await client.query(
            `INSERT INTO orders (tenant_id, user_id, status, total_amount, kafka_topic, kafka_partition, kafka_offset, created_at, updated_at)
             VALUES ($1, $2, 'CONFIRMED', $3, 'tenant.orders', $4, $5, $6, $6)
             RETURNING id`,
            [tenant.id, tenant.user.id, total, i % 3, String(i), createdAt]
          );

          const orderId = orderRows[0].id;
          await client.query(
            `INSERT INTO order_items (tenant_id, order_id, product_id, product_name, sku, quantity, unit_price, line_total)
             VALUES
               ($1, $2, $3, $4, $5, $6, $7, $8),
               ($1, $2, $9, $10, $11, $12, $13, $14)`,
            [
              tenant.id,
              orderId,
              p1.id,
              p1.name,
              p1.sku,
              qty1,
              p1.unitPrice,
              qty1 * p1.unitPrice,
              p2.id,
              p2.name,
              p2.sku,
              qty2,
              p2.unitPrice,
              qty2 * p2.unitPrice,
            ]
          );
        }
      }
    }

    // Optional bulk seed for EXPLAIN ANALYZE indexing benchmark (e.g. 50,000 rows)
    if (bulkOrderCount > 0) {
      console.log(`[Seed] Generating ${bulkOrderCount} synthetic orders for EXPLAIN ANALYZE...`);
      await client.query(
        `INSERT INTO orders (tenant_id, user_id, status, total_amount, created_at, updated_at)
         SELECT
           CASE WHEN g % 2 = 0
             THEN '11111111-1111-4111-8111-111111111111'::uuid
             ELSE '22222222-2222-4222-8222-222222222222'::uuid
           END,
           CASE WHEN g % 2 = 0
             THEN 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'::uuid
             ELSE 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'::uuid
           END,
           'CONFIRMED',
           ROUND((50 + (random() * 900))::numeric, 2),
           NOW() - (random() * interval '365 days'),
           NOW()
         FROM generate_series(1, $1) AS g`,
        [bulkOrderCount]
      );
      await client.query('ANALYZE orders');
    }
  });

  console.log('[Seed] Seeded Tenant A (acme-corp) and Tenant B (globex-inc) successfully.');
}

if (require.main === module) {
  const bulk = Number.parseInt(process.argv[2] || '0', 10);
  seedDatabase({ bulkOrderCount: Number.isNaN(bulk) ? 0 : bulk })
    .then(() => pool.end())
    .catch((err) => {
      console.error('[Seed] Failed:', err);
      process.exit(1);
    });
}

module.exports = { seedDatabase, TENANTS };
