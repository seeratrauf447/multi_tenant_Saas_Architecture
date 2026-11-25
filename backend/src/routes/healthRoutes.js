const express = require('express');
const { checkDatabaseHealth } = require('../config/db');
const { defaultKafkaProducer } = require('../services/kafkaProducer');
const config = require('../config/env');

const router = express.Router();

router.get('/', async (_req, res) => {
  let dbHealth;
  try {
    dbHealth = await checkDatabaseHealth();
  } catch (err) {
    dbHealth = { status: 'down', error: err.message };
  }

  return res.status(dbHealth.status === 'up' ? 200 : 503).json({
    service: 'multitenant-saas-api',
    status: dbHealth.status === 'up' ? 'healthy' : 'degraded',
    timestamp: new Date().toISOString(),
    database: dbHealth,
    kafka: {
      brokers: config.kafka.brokers,
      topic: config.kafka.ordersTopic,
      consumerGroup: config.kafka.consumerGroup,
      producerConnected: defaultKafkaProducer.isConnected(),
    },
    serverless: {
      mode: config.serverlessInvoiceUrl ? 'remote-http' : 'local-adapter',
      url: config.serverlessInvoiceUrl || '/api/invoice-worker',
    },
  });
});

module.exports = router;
