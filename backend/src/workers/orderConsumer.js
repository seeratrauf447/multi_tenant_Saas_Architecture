const { performance } = require('node:perf_hooks');
const { kafka, ensureOrdersTopicExists } = require('../config/kafka');
const config = require('../config/env');
const { withTenantTransaction, pool } = require('../config/db');
const productRepository = require('../repositories/productRepository');
const orderRepository = require('../repositories/orderRepository');
const invoiceRepository = require('../repositories/invoiceRepository');
const { invokeInvoiceWorker } = require('../services/serverlessClient');

let consumerInstance = null;

async function handleOrderCreatedMessage({ topic, partition, message }) {
  const start = performance.now();
  const messageKey = message.key ? message.key.toString() : 'null';
  const offset = message.offset;

  let event;
  try {
    event = JSON.parse(message.value.toString());
  } catch (err) {
    console.error(
      `[Kafka Consumer] Invalid JSON payload at partition=${partition} offset=${offset}:`,
      err.message
    );
    return;
  }

  const { orderId, tenantId, tenantName, customerEmail, items = [] } = event;
  console.log(
    `[Kafka Consumer] Consumed OrderCreated | topic=${topic} partition=${partition} offset=${offset} key=${messageKey} orderId=${orderId}`
  );

  if (!tenantId || !orderId) {
    console.warn(`[Kafka Consumer] Missing tenantId/orderId at offset=${offset}, skipping.`);
    return;
  }

  let finalStatus = 'CONFIRMED';
  let failureReason = null;

  try {
    await withTenantTransaction(tenantId, async (client) => {
      const { rows: currentOrderRows } = await client.query(
        'SELECT id, status FROM orders WHERE id = $1 AND tenant_id = $2 FOR UPDATE',
        [orderId, tenantId]
      );

      if (currentOrderRows.length === 0) {
        throw new Error(`Order ${orderId} not found in tenant ${tenantId}`);
      }

      if (currentOrderRows[0].status === 'CONFIRMED') {
        console.log(`[Kafka Consumer] Order ${orderId} already CONFIRMED (idempotent skip).`);
        return;
      }

      for (const item of items) {
        const updated = await productRepository.decrementStockAtomic(
          client,
          tenantId,
          item.productId,
          item.quantity
        );
        if (!updated) {
          const stockErr = new Error(
            `Insufficient inventory for SKU ${item.sku || item.productId}`
          );
          stockErr.isStockFailure = true;
          throw stockErr;
        }
      }

      await orderRepository.updateOrderStatus(client, tenantId, orderId, 'CONFIRMED', null);
    });
  } catch (err) {
    if (err.isStockFailure) {
      finalStatus = 'FAILED_OUT_OF_STOCK';
      failureReason = err.message;
      await withTenantTransaction(tenantId, async (client) => {
        await orderRepository.updateOrderStatus(
          client,
          tenantId,
          orderId,
          finalStatus,
          failureReason
        );
      });
    } else {
      console.error(`[Kafka Consumer] Error processing order ${orderId}:`, err.message);
      throw err;
    }
  }

  // Trigger Serverless Async Worker for PDF Invoice + Email Dispatch on CONFIRMED orders
  if (finalStatus === 'CONFIRMED') {
    try {
      const invoiceResult = await invokeInvoiceWorker({
        orderId,
        tenantId,
        tenantName: tenantName || tenantId,
        customerEmail: customerEmail || 'billing@tenant.io',
        items,
      });

      await withTenantTransaction(tenantId, async (client) => {
        await invoiceRepository.saveInvoice(client, tenantId, invoiceResult);
      });

      console.log(
        `[Serverless Invoice] orderId=${orderId} invoice=${invoiceResult.invoiceNumber} startType=${invoiceResult.workerTelemetry.startType} latency=${invoiceResult.workerTelemetry.totalLatencyMs}ms`
      );
    } catch (workerErr) {
      console.error(
        `[Serverless Invoice] Failed for orderId=${orderId}:`,
        workerErr.message
      );
    }
  }

  const processingMs = Number((performance.now() - start).toFixed(3));
  await withTenantTransaction(tenantId, async (client) => {
    await invoiceRepository.logKafkaEvent(client, {
      tenantId,
      orderId,
      eventType: 'OrderCreated',
      topic,
      partition,
      offset,
      messageKey,
      status: finalStatus,
      processingMs,
      consumerGroup: config.kafka.consumerGroup,
    });
  });

  console.log(
    `[Kafka Consumer] Committed offset=${offset} partition=${partition} status=${finalStatus} processingMs=${processingMs}ms`
  );
}

async function startOrderConsumer() {
  await ensureOrdersTopicExists(3);

  const consumer = kafka.consumer({
    groupId: config.kafka.consumerGroup,
  });

  await consumer.connect();
  await consumer.subscribe({
    topic: config.kafka.ordersTopic,
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      await handleOrderCreatedMessage({ topic, partition, message });
    },
  });

  consumerInstance = consumer;
  console.log(
    `[Kafka Consumer] Listening on topic="${config.kafka.ordersTopic}" group="${config.kafka.consumerGroup}" brokers="${config.kafka.brokers.join(',')}"`
  );
  return consumer;
}

async function stopOrderConsumer() {
  if (consumerInstance) {
    await consumerInstance.disconnect();
    consumerInstance = null;
  }
}

if (require.main === module) {
  startOrderConsumer().catch((err) => {
    console.error('[Kafka Consumer] Fatal error:', err);
    process.exit(1);
  });

  const shutdown = async () => {
    console.log('\n[Kafka Consumer] Shutting down...');
    await stopOrderConsumer().catch(() => {});
    await pool.end().catch(() => {});
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

module.exports = {
  startOrderConsumer,
  stopOrderConsumer,
  handleOrderCreatedMessage,
};
