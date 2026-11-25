const app = require('./app');
const config = require('./config/env');
const { pool } = require('./config/db');
const { defaultKafkaProducer } = require('./services/kafkaProducer');
const { startOrderConsumer, stopOrderConsumer } = require('./workers/orderConsumer');

const server = app.listen(config.port, () => {
  console.log(`[API] Multi-Tenant SaaS Platform listening on http://localhost:${config.port}`);
});

if (config.runEmbeddedConsumer) {
  startOrderConsumer().catch((err) => {
    console.error('[API] Embedded Kafka consumer failed to start:', err.message);
  });
}

async function shutdown() {
  console.log('\n[API] Shutting down gracefully...');
  server.close(async () => {
    await stopOrderConsumer().catch(() => {});
    await defaultKafkaProducer.disconnect().catch(() => {});
    await pool.end().catch(() => {});
    process.exit(0);
  });
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
