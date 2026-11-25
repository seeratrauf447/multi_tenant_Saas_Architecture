const path = require('node:path');
const dotenv = require('dotenv');

// Load .env from repository root first, then backend/.env if present
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

const parseBool = (val, fallback = false) => {
  if (val === undefined || val === '') return fallback;
  return String(val).toLowerCase() === 'true';
};

const parseIntEnv = (val, fallback) => {
  const parsed = Number.parseInt(val, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
};

const config = {
  port: parseIntEnv(process.env.PORT, 4000),
  nodeEnv: process.env.NODE_ENV || 'development',
  corsOrigin: process.env.CORS_ORIGIN || '*',
  serveFrontend: parseBool(process.env.SERVE_FRONTEND, false),
  runEmbeddedConsumer: parseBool(process.env.RUN_EMBEDDED_CONSUMER, false),

  databaseUrl:
    process.env.DATABASE_URL ||
    'postgresql://saas_user:saas_pass@localhost:5432/multitenant_saas',
  pgPool: {
    max: parseIntEnv(process.env.PG_POOL_MAX, 20),
    min: parseIntEnv(process.env.PG_POOL_MIN, 2),
    idleTimeoutMillis: parseIntEnv(process.env.PG_IDLE_TIMEOUT_MS, 30000),
    connectionTimeoutMillis: parseIntEnv(process.env.PG_CONNECTION_TIMEOUT_MS, 5000),
    ssl: parseBool(process.env.PG_SSL, false),
  },

  jwtSecret: process.env.JWT_SECRET || 'dev-only-secret-change-in-production',
  jwtExpiresIn: process.env.JWT_EXPIRES_IN || '8h',

  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || 'multitenant-saas-platform',
    brokers: (process.env.KAFKA_BROKERS || 'localhost:19092')
      .split(',')
      .map((b) => b.trim())
      .filter(Boolean),
    ordersTopic: process.env.KAFKA_TOPIC_ORDERS || 'tenant.orders',
    consumerGroup: process.env.KAFKA_CONSUMER_GROUP || 'order-processor-group',
    ssl: parseBool(process.env.KAFKA_SSL, false),
    saslMechanism: process.env.KAFKA_SASL_MECHANISM || '',
    saslUsername: process.env.KAFKA_SASL_USERNAME || '',
    saslPassword: process.env.KAFKA_SASL_PASSWORD || '',
    caCertBase64: process.env.KAFKA_CA_CERT_BASE64 || '',
  },

  serverlessInvoiceUrl: process.env.SERVERLESS_INVOICE_URL || '',
};

module.exports = config;
