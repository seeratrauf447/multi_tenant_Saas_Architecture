import React, { useEffect, useState, useCallback } from 'react';
import { apiRequest, downloadInvoicePdf } from '../api/client';

export default function OrdersConsole({ session }) {
  const [products, setProducts] = useState([]);
  const [orders, setOrders] = useState([]);
  const [selectedProductId, setSelectedProductId] = useState('');
  const [quantity, setQuantity] = useState(1);
  const [submitting, setSubmitting] = useState(false);
  const [lastDispatch, setLastDispatch] = useState(null);
  const [error, setError] = useState(null);
  const [queryMeta, setQueryMeta] = useState(null);

  const loadData = useCallback(async () => {
    if (!session) return;
    try {
      const [prodRes, ordRes] = await Promise.all([
        apiRequest('/api/v1/products', {
          token: session.token,
          tenantHeader: session.tenant.slug,
        }),
        apiRequest('/api/v1/orders?limit=15', {
          token: session.token,
          tenantHeader: session.tenant.slug,
        }),
      ]);
      setProducts(prodRes.data.products || []);
      if (prodRes.data.products?.length && !selectedProductId) {
        setSelectedProductId(prodRes.data.products[0].id);
      }
      setOrders(ordRes.data.orders || []);
      setQueryMeta({
        strategy: ordRes.data.strategy,
        queryCount: ordRes.data.queryCount,
        durationMs: ordRes.data.durationMs,
      });
    } catch (err) {
      setError(err.message);
    }
  }, [session, selectedProductId]);

  useEffect(() => {
    setSelectedProductId('');
    setLastDispatch(null);
    setError(null);
    loadData();
  }, [session?.tenant?.id]);

  // Poll every 2.5s while any order is still QUEUED so consumer completion appears live
  useEffect(() => {
    const hasQueued = orders.some((o) => o.status === 'QUEUED');
    if (!hasQueued) return undefined;
    const timer = setInterval(loadData, 2500);
    return () => clearInterval(timer);
  }, [orders, loadData]);

  async function handlePlaceOrder(e) {
    e.preventDefault();
    if (!selectedProductId) return;
    setSubmitting(true);
    setError(null);
    try {
      const { status, data } = await apiRequest('/api/v1/orders', {
        method: 'POST',
        token: session.token,
        tenantHeader: session.tenant.slug,
        body: {
          items: [{ productId: selectedProductId, quantity: Number(quantity) }],
        },
      });
      setLastDispatch({ httpStatus: status, ...data });
      await loadData();
    } catch (err) {
      setError(err.message);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="grid-2">
      <div className="panel">
        <h3 style={{ marginTop: 0 }}>Place Async Order (Kafka Producer)</h3>
        <form onSubmit={handlePlaceOrder} className="form-row">
          <select
            value={selectedProductId}
            onChange={(e) => setSelectedProductId(e.target.value)}
            style={{ flex: 1, minWidth: 200 }}
          >
            {products.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name} (${p.unit_price}) — Stock: {p.stock}
              </option>
            ))}
          </select>
          <input
            type="number"
            min="1"
            max="50"
            value={quantity}
            onChange={(e) => setQuantity(e.target.value)}
            style={{ width: 76 }}
          />
          <button className="btn btn-primary" type="submit" disabled={submitting || !selectedProductId}>
            {submitting ? 'Publishing...' : 'Submit Order'}
          </button>
        </form>

        {lastDispatch && (
          <div className="alert alert-ok mono">
            HTTP {lastDispatch.httpStatus} Accepted · Order {lastDispatch.orderId.slice(0, 8)}...
            <br />
            Topic: {lastDispatch.kafka.topic} · Partition: {lastDispatch.kafka.partition} · Offset:{' '}
            {lastDispatch.kafka.offset} · Key: {lastDispatch.kafka.messageKey}
          </div>
        )}

        {error && <div className="alert alert-err">{error}</div>}

        <h4 style={{ marginBottom: 8 }}>Tenant Catalog ({session.tenant.slug})</h4>
        <table>
          <thead>
            <tr>
              <th>SKU</th>
              <th>Product</th>
              <th>Price</th>
              <th>Stock</th>
            </tr>
          </thead>
          <tbody>
            {products.map((p) => (
              <tr key={p.id}>
                <td className="mono">{p.sku}</td>
                <td>{p.name}</td>
                <td className="mono">${p.unit_price.toFixed(2)}</td>
                <td className="mono">{p.stock}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <h3 style={{ marginTop: 0 }}>
            Tenant Orders {queryMeta && <span className="badge badge-info">{queryMeta.queryCount} SQL query · {queryMeta.durationMs}ms</span>}
          </h3>
          <button className="btn" onClick={loadData}>Refresh</button>
        </div>

        <table>
          <thead>
            <tr>
              <th>Order ID</th>
              <th>Status</th>
              <th>Items</th>
              <th>Total</th>
              <th>Kafka P/O</th>
              <th>Invoice</th>
            </tr>
          </thead>
          <tbody>
            {orders.map((o) => (
              <tr key={o.id}>
                <td className="mono">{o.id.slice(0, 8)}</td>
                <td>
                  <span
                    className={`badge ${
                      o.status === 'CONFIRMED'
                        ? 'badge-ok'
                        : o.status === 'QUEUED'
                        ? 'badge-warn'
                        : 'badge-err'
                    }`}
                  >
                    {o.status}
                  </span>
                </td>
                <td>{o.items?.length || 0}</td>
                <td className="mono">${Number(o.total_amount).toFixed(2)}</td>
                <td className="mono">
                  {o.kafka_partition !== null && o.kafka_partition !== undefined
                    ? `P${o.kafka_partition}:O${o.kafka_offset}`
                    : '—'}
                </td>
                <td>
                  {o.status === 'CONFIRMED' ? (
                    <button
                      className="btn btn-success"
                      style={{ padding: '4px 8px', fontSize: '0.75rem' }}
                      onClick={() =>
                        downloadInvoicePdf(o.id, {
                          token: session.token,
                          tenantHeader: session.tenant.slug,
                        }).catch((err) => setError(err.message))
                      }
                    >
                      PDF
                    </button>
                  ) : (
                    '—'
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
