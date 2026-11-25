const { withTenantTransaction } = require('../config/db');

async function saveInvoice(client, tenantId, invoice) {
  const { rows } = await client.query(
    `INSERT INTO invoices (
       tenant_id, order_id, invoice_number, subtotal, tax_amount, total_amount,
       pdf_base64, email_status, cold_start, init_duration_ms,
       execution_duration_ms, total_latency_ms, worker_instance_id
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
     ON CONFLICT (order_id) DO UPDATE SET
       pdf_base64 = EXCLUDED.pdf_base64,
       cold_start = EXCLUDED.cold_start,
       init_duration_ms = EXCLUDED.init_duration_ms,
       execution_duration_ms = EXCLUDED.execution_duration_ms,
       total_latency_ms = EXCLUDED.total_latency_ms,
       worker_instance_id = EXCLUDED.worker_instance_id
     RETURNING id, order_id, invoice_number, total_amount::float8 AS total_amount, cold_start, total_latency_ms::float8 AS total_latency_ms, created_at`,
    [
      tenantId,
      invoice.orderId,
      invoice.invoiceNumber,
      invoice.subtotal,
      invoice.taxAmount,
      invoice.totalAmount,
      invoice.pdfBase64,
      invoice.emailDispatch?.status || 'DISPATCHED',
      Boolean(invoice.workerTelemetry?.coldStart),
      Number(invoice.workerTelemetry?.initDurationMs || 0),
      Number(invoice.workerTelemetry?.executionDurationMs || 0),
      Number(invoice.workerTelemetry?.totalLatencyMs || 0),
      invoice.workerTelemetry?.instanceId || 'local',
    ]
  );
  return rows[0];
}

async function getInvoiceByOrderId(tenantId, orderId) {
  return withTenantTransaction(tenantId, async (client) => {
    const { rows } = await client.query(
      `SELECT
         id, tenant_id, order_id, invoice_number,
         subtotal::float8 AS subtotal,
         tax_amount::float8 AS tax_amount,
         total_amount::float8 AS total_amount,
         pdf_base64, email_status, cold_start,
         init_duration_ms::float8 AS init_duration_ms,
         execution_duration_ms::float8 AS execution_duration_ms,
         total_latency_ms::float8 AS total_latency_ms,
         worker_instance_id, created_at
       FROM invoices
       WHERE tenant_id = $1 AND order_id = $2
       LIMIT 1`,
      [tenantId, orderId]
    );
    return rows[0] || null;
  });
}

async function listRecentInvoices(tenantId, limit = 20) {
  return withTenantTransaction(tenantId, async (client) => {
    const { rows } = await client.query(
      `SELECT
         id, tenant_id, order_id, invoice_number,
         subtotal::float8 AS subtotal,
         tax_amount::float8 AS tax_amount,
         total_amount::float8 AS total_amount,
         email_status, cold_start,
         init_duration_ms::float8 AS init_duration_ms,
         execution_duration_ms::float8 AS execution_duration_ms,
         total_latency_ms::float8 AS total_latency_ms,
         worker_instance_id, created_at
       FROM invoices
       WHERE tenant_id = $1
       ORDER BY created_at DESC
       LIMIT $2`,
      [tenantId, limit]
    );
    return rows;
  });
}

async function logKafkaEvent(client, eventLog) {
  const { rows } = await client.query(
    `INSERT INTO kafka_event_logs (
       tenant_id, order_id, event_type, topic, partition, kafka_offset,
       message_key, status, processing_ms, consumer_group
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
     RETURNING *`,
    [
      eventLog.tenantId,
      eventLog.orderId,
      eventLog.eventType,
      eventLog.topic,
      eventLog.partition,
      String(eventLog.offset),
      eventLog.messageKey,
      eventLog.status,
      eventLog.processingMs,
      eventLog.consumerGroup,
    ]
  );
  return rows[0];
}

async function listKafkaEvents(tenantId, limit = 30) {
  return withTenantTransaction(tenantId, async (client) => {
    const { rows } = await client.query(
      `SELECT
         id, tenant_id, order_id, event_type, topic, partition,
         kafka_offset, message_key, status,
         processing_ms::float8 AS processing_ms,
         consumer_group, created_at
       FROM kafka_event_logs
       WHERE tenant_id = $1
       ORDER BY created_at DESC
       LIMIT $2`,
      [tenantId, limit]
    );
    return rows;
  });
}

module.exports = {
  saveInvoice,
  getInvoiceByOrderId,
  listRecentInvoices,
  logKafkaEvent,
  listKafkaEvents,
};
