CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1. Tenant Registry
CREATE TABLE IF NOT EXISTS tenants (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  slug TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  plan TEXT NOT NULL DEFAULT 'ENTERPRISE',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Tenant-Scoped Users
CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  email TEXT NOT NULL,
  password_hash TEXT NOT NULL,
  full_name TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'ADMIN',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, email)
);

-- 3. Tenant-Scoped Product Inventory
CREATE TABLE IF NOT EXISTS products (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  sku TEXT NOT NULL,
  name TEXT NOT NULL,
  unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
  stock INTEGER NOT NULL CHECK (stock >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, sku)
);

-- 4. Tenant-Scoped Orders
CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  user_id UUID REFERENCES users(id) ON DELETE SET NULL,
  status TEXT NOT NULL DEFAULT 'QUEUED',
  total_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
  kafka_topic TEXT,
  kafka_partition INTEGER,
  kafka_offset TEXT,
  failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. Tenant-Scoped Order Line Items
CREATE TABLE IF NOT EXISTS order_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id UUID NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
  product_name TEXT NOT NULL,
  sku TEXT NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  unit_price NUMERIC(12, 2) NOT NULL,
  line_total NUMERIC(12, 2) NOT NULL
);

-- 6. Tenant-Scoped Serverless Invoices & Cold-Start Telemetry
CREATE TABLE IF NOT EXISTS invoices (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  order_id UUID NOT NULL UNIQUE REFERENCES orders(id) ON DELETE CASCADE,
  invoice_number TEXT NOT NULL,
  subtotal NUMERIC(12, 2) NOT NULL,
  tax_amount NUMERIC(12, 2) NOT NULL,
  total_amount NUMERIC(12, 2) NOT NULL,
  pdf_base64 TEXT NOT NULL,
  email_status TEXT NOT NULL,
  cold_start BOOLEAN NOT NULL,
  init_duration_ms NUMERIC(10, 3) NOT NULL,
  execution_duration_ms NUMERIC(10, 3) NOT NULL,
  total_latency_ms NUMERIC(10, 3) NOT NULL,
  worker_instance_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 7. Tenant-Scoped Kafka Consumer Event Logs
CREATE TABLE IF NOT EXISTS kafka_event_logs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  order_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  topic TEXT NOT NULL,
  partition INTEGER NOT NULL,
  kafka_offset TEXT NOT NULL,
  message_key TEXT NOT NULL,
  status TEXT NOT NULL,
  processing_ms NUMERIC(10, 3) NOT NULL,
  consumer_group TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Performance Tuning Indexes (Lecture 17)
CREATE INDEX IF NOT EXISTS idx_orders_tenant_created_at
  ON orders (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_order_items_tenant_order
  ON order_items (tenant_id, order_id);

CREATE INDEX IF NOT EXISTS idx_products_tenant_id
  ON products (tenant_id);

CREATE INDEX IF NOT EXISTS idx_kafka_logs_tenant_created
  ON kafka_event_logs (tenant_id, created_at DESC);

-- PostgreSQL Row-Level Security (RLS) Defense-in-Depth (Lecture 18)
DO $$
DECLARE
  tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['users', 'products', 'orders', 'order_items', 'invoices', 'kafka_event_logs']
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation_policy ON %I', tbl);
    EXECUTE format(
      'CREATE POLICY tenant_isolation_policy ON %I
       USING (
         current_setting(''app.bypass_rls'', true) = ''on''
         OR tenant_id = NULLIF(current_setting(''app.current_tenant_id'', true), '''')::uuid
       )
       WITH CHECK (
         current_setting(''app.bypass_rls'', true) = ''on''
         OR tenant_id = NULLIF(current_setting(''app.current_tenant_id'', true), '''')::uuid
       )',
      tbl
    );
  END LOOP;
END $$;
