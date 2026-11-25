const { Partitioners } = require('kafkajs');
const { kafka, ensureOrdersTopicExists } = require('../config/kafka');
const config = require('../config/env');

function createKafkaProducer({ kafkaClient = kafka, topic = config.kafka.ordersTopic } = {}) {
  const producer = kafkaClient.producer({
    allowAutoTopicCreation: true,
    idempotent: false,
    createPartitioner: Partitioners.DefaultPartitioner,
  });

  let connected = false;
  let connectingPromise = null;

  async function ensureConnected() {
    if (connected) return;
    if (!connectingPromise) {
      connectingPromise = (async () => {
        await ensureOrdersTopicExists(3);
        await producer.connect();
        connected = true;
      })().finally(() => {
        connectingPromise = null;
      });
    }
    await connectingPromise;
  }

  async function publishOrderCreated(eventPayload) {
    await ensureConnected();

    const messageKey = String(eventPayload.tenantId);
    const serialized = JSON.stringify(eventPayload);

    const recordMetadata = await producer.send({
      topic,
      messages: [
        {
          key: messageKey,
          value: serialized,
          headers: {
            'x-event-type': 'OrderCreated',
            'x-tenant-id': String(eventPayload.tenantId),
            'x-order-id': String(eventPayload.orderId),
          },
        },
      ],
    });

    const firstMeta = recordMetadata[0] || {};
    return {
      topic: firstMeta.topicName || topic,
      partition: firstMeta.partition ?? 0,
      offset: String(firstMeta.baseOffset ?? '0'),
      messageKey,
      publishedAt: new Date().toISOString(),
    };
  }

  async function disconnect() {
    if (connected) {
      await producer.disconnect();
      connected = false;
    }
  }

  return {
    publishOrderCreated,
    disconnect,
    isConnected: () => connected,
  };
}

const defaultKafkaProducer = createKafkaProducer();

module.exports = {
  createKafkaProducer,
  defaultKafkaProducer,
};
