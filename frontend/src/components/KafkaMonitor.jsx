import React, { useEffect, useState, useCallback } from 'react';
import { apiRequest } from '../api/client';

export default function KafkaMonitor({ session }) {
  const [events, setEvents] = useState([]);
  const [error, setError] = useState(null);

  const loadEvents = useCallback(async () => {
    try {
      const { data } = await apiRequest('/api/v1/diagnostics/kafka-events?limit=30', {
        token: session.token,
        tenantHeader: session.tenant.slug,
      });
      setEvents(data.events || []);
    } catch (err) {
      setError(err.message);
    }
  }, [session]);

  useEffect(() => {
    loadEvents();
    const timer = setInterval(loadEvents, 4000);
    return () => clearInterval(timer);
  }, [loadEvents]);

  return (
    <div className="panel">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h3 style={{ marginTop: 0 }}>Kafka Consumer Group Telemetry (tenant.orders)</h3>
        <button className="btn" onClick={loadEvents}>Refresh Offsets</button>
      </div>
      {error && <div className="alert alert-err">{error}</div>}
      <table>
        <thead>
          <tr>
            <th>Topic</th>
            <th>Partition</th>
            <th>Offset</th>
            <th>Message Key (tenant_id)</th>
            <th>Order ID</th>
            <th>Status</th>
            <th>Processing</th>
          </tr>
        </thead>
        <tbody>
          {events.length === 0 ? (
            <tr>
              <td colSpan="7" style={{ color: 'var(--muted)' }}>
                No consumer events logged yet for {session.tenant.slug}. Place an order to trigger Kafka!
              </td>
            </tr>
          ) : (
            events.map((ev) => (
              <tr key={ev.id}>
                <td className="mono">{ev.topic}</td>
                <td className="mono">{ev.partition}</td>
                <td className="mono">{ev.kafka_offset}</td>
                <td className="mono">{ev.message_key.slice(0, 12)}...</td>
                <td className="mono">{ev.order_id.slice(0, 8)}</td>
                <td>
                  <span className={`badge ${ev.status === 'CONFIRMED' ? 'badge-ok' : 'badge-err'}`}>
                    {ev.status}
                  </span>
                </td>
                <td className="mono">{ev.processing_ms}ms</td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
