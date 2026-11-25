# CloudFlow B2B — Multi-Tenant Event-Driven SaaS Platform

> **CSC337: Advanced Web Technologies — Lab Assignment 06**
> Topics: Multi-Tenant SaaS Isolation, PostgreSQL Performance Tuning (`N+1` Elimination, `EXPLAIN ANALYZE`, `pg.Pool`), Apache Kafka Event-Driven Orders (`kafkajs`), and Serverless Cold-Start vs Warm-Start Analysis.

---

## 1. System Architecture

```mermaid
flowchart LR
    Client["React SaaS Console (Tenant A / Tenant B)"] -->|"JWT + x-tenant-id / Subdomain"| API["Express API + Tenant Resolver Middleware"]
    API -->|"SET LOCAL app.current_tenant_id (RLS) + pg.Pool"| PG[("PostgreSQL 16 (Composite Index + RLS)")]
    API -->|"202 Accepted | OrderCreated (key = tenant_id)"| Kafka[["Apache Kafka / Redpanda (Topic: tenant.orders, 3 Partitions)"]]
    Kafka -->|"Consumer Group: order-processor-group"| Worker["KafkaJS Order Consumer"]
    Worker -->|"Atomic Stock Decrement + Offset Log"| PG
    Worker -->|"POST /api/invoice-worker (Async PDF & Email)"| Serverless["Vercel Serverless Function (/api/invoice-worker)"]
    Serverless -->|"PDF Base64 + Cold/Warm Telemetry"| Worker
```

### Key Architectural Guarantees
1. **Multi-Tenant Isolation (Lecture 18):**
   - **Shared Database with Tenant Discriminator (`tenant_id`) + PostgreSQL Row-Level Security (`FORCE ROW LEVEL SECURITY`)**.
   - **Tenant Resolution Middleware** ([`tenantResolver.js`](backend/src/middleware/tenantResolver.js)) extracts tenant context from **JWT claim** (`req.user.tenantId`), **`x-tenant-id` header** (UUID or slug), and **subdomain** (`acme-corp.localhost`), enforcing strict consistency (`403 Cross-tenant boundary violation` on mismatch) and storing context in `AsyncLocalStorage`.
   - Every `pg.Pool` checkout wraps queries in `BEGIN; SELECT set_config('app.current_tenant_id', $1, true); ... COMMIT;` ([`db.js`](backend/src/config/db.js)) so pooled connections never leak tenant state across requests.
2. **Database Performance Tuning (Lecture 17):**
   - **N+1 Elimination:** [`orderRepository.js`](backend/src/repositories/orderRepository.js) compares an intentional `UNOPTIMIZED_N_PLUS_1` baseline (`1 + N` queries) against `OPTIMIZED_SINGLE_JOIN` (`LEFT JOIN` + `json_agg` in `1` SQL round trip).
   - **Composite Index:** `idx_orders_tenant_created_at ON orders (tenant_id, created_at DESC)` eliminates full-table sequential scans and in-memory sorts on tenant order timelines.
   - **Connection Pooling:** Explicit `pg.Pool` tuning (`PG_POOL_MAX=20`, `PG_POOL_MIN=2`, `PG_IDLE_TIMEOUT_MS=30000`) with live pool metrics exposed at `GET /api/v1/diagnostics/pool`.
3. **Event-Driven Orders with Apache Kafka (Lecture 18):**
   - `POST /api/v1/orders` persists a `QUEUED` order, publishes `OrderCreated` to `tenant.orders` using **`tenant_id` as the Kafka message key** (preserving per-tenant sequential ordering within a partition), and responds immediately with **`202 Accepted`**.
   - [`orderConsumer.js`](backend/src/workers/orderConsumer.js) runs consumer group `order-processor-group`, atomically decrements inventory, commits offset telemetry to `kafka_event_logs`, and triggers the serverless invoice worker.
4. **Serverless Async Worker & Cold-Start Analysis (Lecture 17):**
   - [`api/invoice-worker.js`](api/invoice-worker.js) is a zero-dependency Vercel Serverless Function that generates a real PDF 1.4 tax invoice, dispatches a simulated email receipt, and tracks `coldStart`, `initDurationMs`, `executionDurationMs`, and `totalLatencyMs`.

---

## 2. Test Accounts (Tenant A & Tenant B)

| Tenant | `tenant_id` (UUID) | Slug (`x-tenant-id`) | Email | Password |
| :--- | :--- | :--- | :--- | :--- |
| **Tenant A (Acme Corp)** | `11111111-1111-4111-8111-111111111111` | `acme-corp` | `admin@acme-corp.io` | `TenantA!Password123` |
| **Tenant B (Globex Inc)** | `22222222-2222-4222-8222-222222222222` | `globex-inc` | `admin@globex-inc.io` | `TenantB!Password123` |

---

## 3. Local Setup & Running

### Prerequisites
- Node.js 20+
- Docker Desktop (for local PostgreSQL 16 + Redpanda Kafka broker + Redpanda Console), **or** cloud PostgreSQL (Neon/Supabase) + Cloud Kafka (Aiven).

### Step 1: Configure Environment
```bash
cp .env.example .env
```

### Step 2: Start PostgreSQL & Kafka (Redpanda)
```bash
docker compose up -d
```
- PostgreSQL runs on `localhost:5432`
- Kafka Broker runs on `localhost:19092`
- Redpanda Console UI runs on `http://localhost:8080`

### Step 3: Install Dependencies, Migrate, and Seed
```bash
npm run install:all
npm run migrate
npm run seed
```

### Step 4: Run API, Kafka Consumer, and Frontend
Open three terminals:
```bash
# Terminal 1: API Server (http://localhost:4000)
npm run dev:api

# Terminal 2: Kafka Order Consumer Group (live offset logs)
npm run dev:worker

# Terminal 3: React Operations Console (http://localhost:5173)
npm run dev:web
```

---

## 4. Benchmarking & `EXPLAIN ANALYZE` Guide

### Run Benchmarks via CLI
```bash
# 1. Seeds 50,000 synthetic orders (if < 50k) and runs EXPLAIN (ANALYZE, BUFFERS) Before vs After Composite Index
npm run benchmark:explain

# 2. Compares N+1 queries (1 + 25 = 26 queries) vs Single JOIN (1 query)
npm run benchmark:n1

# 3. Measures Serverless Cold Start (isolated process boot) vs Warm Start invocations
npm run benchmark:coldstart
```

### SQL Query Under `EXPLAIN ANALYZE`
```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, status, total_amount, created_at
FROM orders
WHERE tenant_id = '11111111-1111-4111-8111-111111111111'
ORDER BY created_at DESC
LIMIT 50;
```

- **Before Composite Index** (`idx_orders_tenant_created_at` dropped inside a savepoint): PostgreSQL performs a `Seq Scan on orders` + `Sort (top-N heapsort)` scanning all tenant and cross-tenant rows.
- **After Composite Index** (`CREATE INDEX idx_orders_tenant_created_at ON orders (tenant_id, created_at DESC)`): PostgreSQL performs a `Limit -> Index Scan using idx_orders_tenant_created_at on orders`, jumping directly to the active tenant's newest 50 rows.

*(Run `npm run benchmark:explain` with PostgreSQL running or use Tab 2 in the UI to capture your live environment's execution plan and latency figures.)*

### Observed Serverless Cold-Start vs Warm-Start Benchmark (Local Node.js Runtime)
Measured via `npm run benchmark:coldstart`:

| Invocation | Start Type | Init Duration | Execution Duration | Total Latency |
| :--- | :--- | :--- | :--- | :--- |
| Run #0 (Fresh Node Process) | `COLD_START` | `0.597 ms` (in-process) | `0.717 ms` | `126.165 ms` (incl. process boot) |
| Run #1–#5 Avg (Warm Module) | `WARM_START` | `0 ms` | `0.02 ms` | `0.02 ms` |

---

## 5. Cloud Deployment Guide (Vercel + Render/Railway + Aiven)

1. **PostgreSQL:** Provision a managed Postgres DB on **Neon**, **Supabase**, or **Render PostgreSQL** (`PG_SSL=true`). Run `npm run migrate && npm run seed` against `DATABASE_URL`.
2. **Cloud Kafka (Aiven for Apache Kafka):**
   - Create a free **Aiven for Apache Kafka** service and a topic `tenant.orders` with as many partitions as the plan allows (**3 locally, 2 on the Aiven free tier**).
   - Set `KAFKA_BROKERS`, `KAFKA_SSL=true`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` (and `KAFKA_CA_CERT_BASE64` if using Aiven's CA certificate).
3. **Serverless Worker + Frontend on Vercel:**
   - Deploy the repository root to **Vercel** (`vercel.json` builds `frontend/dist` and exposes `/api/invoice-worker` as a Node.js Serverless Function).
   - Set `VITE_API_BASE_URL` on Vercel to your Render/Railway backend URL.
4. **Backend API + Consumer on Render / Railway:**
   - Deploy `backend/` as a Web Service (`npm install && npm start`), with `RUN_EMBEDDED_CONSUMER=true` (for single-service free tier) and `SERVERLESS_INVOICE_URL=https://<your-vercel-domain>/api/invoice-worker`.

---

### 5.1 Deployment Runbook — Render (API) + Neon (Postgres) + Aiven (Kafka) + Vercel (Web + Serverless)

Deploy in this order: every step produces a URL that the next step consumes. Secrets live in the platform dashboards, **never** in `.env` committed to git.

#### Step 1 — Cloud PostgreSQL (Neon / Supabase / Render PostgreSQL)

1. Create a free database and copy the connection string, keeping `?sslmode=require`.
2. Apply the schema and seed **50,000 synthetic orders**, otherwise the EXPLAIN `Seq Scan` baseline is too small to demonstrate:

```bash
DATABASE_URL="postgresql://user:pass@host/db?sslmode=require" PG_SSL=true npm run migrate
DATABASE_URL="postgresql://user:pass@host/db?sslmode=require" PG_SSL=true npm run seed 50000
```

#### Step 2 — Cloud Kafka (Aiven for Apache Kafka, free tier)

> **Upstash Kafka is not an option:** it was deprecated on 11 Sep 2024 and discontinued on 11 Mar 2025. Use Aiven's free tier instead — 250 KiB/s throughput, 3-day retention, and **5 topics with 2 partitions each** (one free Kafka service per organisation, no credit card).

1. Create the free Kafka service (region group only; the cloud/region is chosen for you) and copy the **Service URI** host:port into `KAFKA_BROKERS`.
2. Create topic `tenant.orders` manually in the console with **2 partitions** — the free plan caps a topic at 2 and restricts topic config, so the app's auto-creation (which requests 3) will be skipped. The local Docker demo keeps 3 partitions; the message key is still `tenant_id`, so a tenant's events stay ordered inside one partition either way.
3. Under *Users & ACLs*, add a user with **SCRAM-SHA-256** credentials → `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD`.
4. Set `KAFKA_SSL=true`, `KAFKA_SASL_MECHANISM=scram-sha-256`, and `KAFKA_CA_CERT_BASE64` to the base64 of Aiven's CA PEM (shown under *CA certificate*):

```bash
base64 -w0 aiven-ca.pem                 # Linux / macOS
git bash: base64 -w0 aiven-ca.pem       # Windows (PowerShell: [Convert]::ToBase64String([IO.File]::ReadAllBytes('aiven-ca.pem')))
```

#### Step 3 — Backend API + Kafka consumer (Render)

[`render.yaml`](render.yaml) in the repository root defines this service: root dir `backend`, health check `/api/v1/health`, and `RUN_EMBEDDED_CONSUMER=true` so the KafkaJS consumer runs inside the API process (one free-tier service). Render → **New +** → **Blueprint** → select this repository → fill the `sync: false` secrets.

#### Step 4 — Serverless invoice worker + React console (Vercel)

- Import the same repository. [`vercel.json`](vercel.json) builds `frontend/dist` and exposes `api/invoice-worker.js` at `/api/invoice-worker`.
- Set `VITE_API_BASE_URL` **before the first build** — Vite inlines it at build time. Use the API origin only: no `/api/v1`, no trailing slash.
- Copy `https://<app>.vercel.app/api/invoice-worker` into Render's `SERVERLESS_INVOICE_URL`, then **redeploy the API** so the worker URL takes effect.
- Set Render's `CORS_ORIGIN` to the exact Vercel origin, or the browser console will be blocked.

#### Environment variable matrix

| Variable | Render — `cloudflow-api` | Vercel — frontend build | Vercel — `/api/invoice-worker` | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `NODE_ENV` | `production` | — | — | |
| `PORT` | *(injected by Render)* | — | — | Never hardcode |
| `SERVE_FRONTEND` | `false` | — | — | Frontend is on Vercel |
| `RUN_EMBEDDED_CONSUMER` | `true` | — | — | Consumer inside the API process |
| `DATABASE_URL` | cloud PG string + `sslmode=require` | — | — | Secret |
| `PG_SSL` | `true` | — | — | Required for Neon/Supabase/Render PG |
| `PG_POOL_MAX` / `PG_POOL_MIN` | `20` / `2` | — | — | Optional; these are the defaults |
| `PG_IDLE_TIMEOUT_MS` / `PG_CONNECTION_TIMEOUT_MS` | `30000` / `5000` | — | — | Optional |
| `JWT_SECRET` | long random string | — | — | Rotating it invalidates all sessions |
| `JWT_EXPIRES_IN` | `8h` | — | — | |
| `CORS_ORIGIN` | `https://<web>.vercel.app` | — | — | Exact origin; comma-separated for several |
| `KAFKA_BROKERS` | `host:9092` | — | — | Secret |
| `KAFKA_TOPIC_ORDERS` | `tenant.orders` | — | — | 3 partitions local, 2 on Aiven free |
| `KAFKA_CONSUMER_GROUP` | `order-processor-group` | — | — | |
| `KAFKA_SSL` | `true` | — | — | Managed brokers |
| `KAFKA_SASL_MECHANISM` | `scram-sha-256` | — | — | Aiven SCRAM user |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | broker credentials | — | — | Secrets |
| `KAFKA_CA_CERT_BASE64` | required on Aiven | — | — | Base64 of the CA PEM (`base64 -w0`) |
| `SERVERLESS_INVOICE_URL` | `https://<web>.vercel.app/api/invoice-worker` | — | — | Blank = local in-process adapter |
| `VITE_API_BASE_URL` | — | `https://<api>.onrender.com` | — | Build-time only; no `/api/v1` |
| *(function itself)* | — | — | no env vars needed | Zero-dependency handler |

#### Post-deploy smoke test

1. `GET https://<api>/api/v1/health` → `200` with `database.status = "up"` and `serverless.mode` set.
2. Sign in as Tenant A → place an order → `202` → row reaches `CONFIRMED` → invoice PDF downloads.
3. **Test Cross-Tenant Spoof (403)** → `Cross-tenant boundary violation`.
4. Tab 2 → run N+1 and EXPLAIN ANALYZE against the cloud database.
5. Render free tier sleeps after ~15 minutes idle: open the app a couple of minutes before the viva, and place one throwaway order so the consumer is confirmed alive.

---

## 6. Live Viva Verification Checklist

1. **Tenant Isolation:** Sign in as Tenant A (`acme-corp`), click **Test Cross-Tenant Spoof (403)** in the header (or send `x-tenant-id: 22222222-2222-4222-8222-222222222222` with Tenant A's JWT) to demonstrate `403 Cross-tenant boundary violation`. Also verify Tenant A's order UUID returns `404` when queried as Tenant B.
2. **Kafka Event Flow & Offsets:** Watch Terminal 2 (`npm run dev:worker`), place an order in the UI, observe immediate **`202 Accepted`** with `partition` and `offset`, and see the consumer log `Committed offset=... status=CONFIRMED` + stock decrement + PDF invoice availability.
3. **N+1 & `EXPLAIN ANALYZE`:** Run both live in **Tab 2 (N+1, EXPLAIN & pg.Pool)** or via `npm run benchmark:n1` and `npm run benchmark:explain`.
4. **Serverless Cold vs Warm Start:** Trigger **Tab 4 (Serverless Cold vs Warm)** or `npm run benchmark:coldstart` and download the generated `.pdf` invoice.
