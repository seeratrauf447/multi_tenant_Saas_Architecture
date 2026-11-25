const productRepository = require('../repositories/productRepository');
const orderRepository = require('../repositories/orderRepository');
const { defaultKafkaProducer } = require('./kafkaProducer');

function createOrderService({
  productsRepo = productRepository,
  ordersRepo = orderRepository,
  producer = defaultKafkaProducer,
} = {}) {
  /**
   * Validates order items within the active tenant boundary, saves a QUEUED order,
   * and publishes an OrderCreated event to Kafka partitioned by tenant_id.
   */
  async function placeOrder({ tenant, user, items }) {
    const productIds = [...new Set(items.map((item) => item.productId))];
    const catalogProducts = await productsRepo.getProductsByIds(tenant.id, productIds);

    const productMap = new Map(catalogProducts.map((p) => [p.id, p]));
    const normalizedItems = [];
    let totalAmount = 0;

    for (const requested of items) {
      const product = productMap.get(requested.productId);
      if (!product) {
        const err = new Error(
          `Product ${requested.productId} not found in tenant "${tenant.slug}" catalog.`
        );
        err.statusCode = 404;
        throw err;
      }

      if (product.stock < requested.quantity) {
        const err = new Error(
          `Insufficient stock for "${product.name}" (available: ${product.stock}, requested: ${requested.quantity}).`
        );
        err.statusCode = 409;
        throw err;
      }

      const lineTotal = Number((product.unit_price * requested.quantity).toFixed(2));
      totalAmount += lineTotal;
      normalizedItems.push({
        productId: product.id,
        productName: product.name,
        sku: product.sku,
        quantity: requested.quantity,
        unitPrice: product.unit_price,
        lineTotal,
      });
    }

    totalAmount = Number(totalAmount.toFixed(2));

    const order = await ordersRepo.createQueuedOrder(
      tenant.id,
      user.userId,
      normalizedItems,
      totalAmount
    );

    const eventPayload = {
      eventType: 'OrderCreated',
      eventVersion: '1.0',
      orderId: order.id,
      tenantId: tenant.id,
      tenantSlug: tenant.slug,
      tenantName: tenant.name,
      customerEmail: user.email,
      totalAmount,
      items: normalizedItems.map((i) => ({
        productId: i.productId,
        name: i.productName,
        sku: i.sku,
        quantity: i.quantity,
        unitPrice: i.unitPrice,
      })),
      createdAt: order.created_at,
    };

    const kafkaMeta = await producer.publishOrderCreated(eventPayload);
    await ordersRepo.attachKafkaMetadata(tenant.id, order.id, kafkaMeta);

    return {
      accepted: true,
      orderId: order.id,
      tenantId: tenant.id,
      tenantSlug: tenant.slug,
      status: 'QUEUED',
      totalAmount,
      items: order.items,
      kafka: kafkaMeta,
    };
  }

  return { placeOrder };
}

const defaultOrderService = createOrderService();

module.exports = {
  createOrderService,
  defaultOrderService,
};
