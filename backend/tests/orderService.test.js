const test = require('node:test');
const assert = require('node:assert/strict');
const { createOrderService } = require('../src/services/orderService');

const TENANT_A = {
  id: '11111111-1111-4111-8111-111111111111',
  slug: 'acme-corp',
  name: 'Acme Corp (Tenant A)',
};

const USER_A = {
  userId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
  email: 'admin@acme-corp.io',
};

const PRODUCT_A1 = {
  id: 'a1000000-0000-4000-8000-000000000001',
  tenant_id: TENANT_A.id,
  sku: 'ACME-K8S-NODE',
  name: 'Managed Kubernetes Node Pack',
  unit_price: 249,
  stock: 10,
};

const PRODUCT_B1_ID = 'b2000000-0000-4000-8000-000000000001';

test('placeOrder persists QUEUED order and publishes OrderCreated with tenantId key', async () => {
  let publishedEvent = null;
  let attachedMeta = null;

  const orderService = createOrderService({
    productsRepo: {
      async getProductsByIds(tenantId, ids) {
        assert.equal(tenantId, TENANT_A.id);
        return ids.includes(PRODUCT_A1.id) ? [PRODUCT_A1] : [];
      },
    },
    ordersRepo: {
      async createQueuedOrder(tenantId, userId, items, totalAmount) {
        assert.equal(tenantId, TENANT_A.id);
        assert.equal(userId, USER_A.userId);
        assert.equal(totalAmount, 498);
        return {
          id: '99999999-9999-4999-8999-999999999999',
          created_at: '2026-11-25T00:00:00.000Z',
          items,
        };
      },
      async attachKafkaMetadata(tenantId, orderId, meta) {
        attachedMeta = { tenantId, orderId, meta };
      },
    },
    producer: {
      async publishOrderCreated(event) {
        publishedEvent = event;
        return {
          topic: 'tenant.orders',
          partition: 1,
          offset: '42',
          messageKey: event.tenantId,
          publishedAt: '2026-11-25T00:00:01.000Z',
        };
      },
    },
  });

  const res = await orderService.placeOrder({
    tenant: TENANT_A,
    user: USER_A,
    items: [{ productId: PRODUCT_A1.id, quantity: 2 }],
  });

  assert.equal(res.accepted, true);
  assert.equal(res.status, 'QUEUED');
  assert.equal(res.totalAmount, 498);
  assert.equal(res.kafka.topic, 'tenant.orders');
  assert.equal(res.kafka.partition, 1);
  assert.equal(res.kafka.messageKey, TENANT_A.id);
  assert.equal(publishedEvent.eventType, 'OrderCreated');
  assert.equal(publishedEvent.tenantId, TENANT_A.id);
  assert.equal(attachedMeta.orderId, res.orderId);
});

test('placeOrder rejects ordering a product belonging to another tenant (404)', async () => {
  const orderService = createOrderService({
    productsRepo: {
      async getProductsByIds() {
        // Scoped query returns empty because PRODUCT_B1 belongs to Tenant B
        return [];
      },
    },
    ordersRepo: {},
    producer: {},
  });

  await assert.rejects(
    () =>
      orderService.placeOrder({
        tenant: TENANT_A,
        user: USER_A,
        items: [{ productId: PRODUCT_B1_ID, quantity: 1 }],
      }),
    (err) => err.statusCode === 404
  );
});

test('placeOrder rejects insufficient stock with 409 Conflict', async () => {
  const orderService = createOrderService({
    productsRepo: {
      async getProductsByIds() {
        return [PRODUCT_A1]; // stock = 10
      },
    },
    ordersRepo: {},
    producer: {},
  });

  await assert.rejects(
    () =>
      orderService.placeOrder({
        tenant: TENANT_A,
        user: USER_A,
        items: [{ productId: PRODUCT_A1.id, quantity: 25 }],
      }),
    (err) => err.statusCode === 409
  );
});
