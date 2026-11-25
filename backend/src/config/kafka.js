const { Kafka, logLevel } = require('kafkajs');
const config = require('./env');

function buildKafkaConfig() {
  const kafkaConfig = {
    clientId: config.kafka.clientId,
    brokers: config.kafka.brokers,
    logLevel: logLevel.WARN,
    retry: {
      initialRetryTime: 250,
      retries: 5,
    },
  };

  if (config.kafka.ssl) {
    if (config.kafka.caCertBase64) {
      kafkaConfig.ssl = {
        rejectUnauthorized: true,
        ca: [Buffer.from(config.kafka.caCertBase64, 'base64').toString('utf8')],
      };
    } else {
      kafkaConfig.ssl = true;
    }
  }

  if (
    config.kafka.saslMechanism &&
    config.kafka.saslUsername &&
    config.kafka.saslPassword
  ) {
    kafkaConfig.sasl = {
      mechanism: config.kafka.saslMechanism,
      username: config.kafka.saslUsername,
      password: config.kafka.saslPassword,
    };
  }

  return kafkaConfig;
}

const kafka = new Kafka(buildKafkaConfig());

/**
 * Ensures the partitioned orders topic exists (3 partitions by default) so
 * tenant_id message keys distribute across partitions while preserving per-tenant ordering.
 */
async function ensureOrdersTopicExists(numPartitions = 3) {
  const admin = kafka.admin();
  try {
    await admin.connect();
    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes(config.kafka.ordersTopic)) {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: config.kafka.ordersTopic,
            numPartitions,
            replicationFactor: 1,
          },
        ],
      });
      console.log(
        `[Kafka] Created topic "${config.kafka.ordersTopic}" with ${numPartitions} partitions`
      );
    }
  } catch (err) {
    console.warn(
      `[Kafka] Topic auto-creation skipped (${err.message}). If using managed Kafka (e.g. Aiven), ensure "${config.kafka.ordersTopic}" exists.`
    );
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

module.exports = {
  kafka,
  buildKafkaConfig,
  ensureOrdersTopicExists,
};
