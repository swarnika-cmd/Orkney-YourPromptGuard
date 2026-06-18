import { useState, useEffect } from 'react';

interface KpiData {
  total_spend: number;
  blocked_injections: number;
  avg_overhead_ms: number;
}

interface AnalyticsData {
  p95_latency: number;
  total_cost: number;
  avg_faithfulness: number;
}

interface TenantSpend {
  tenant_id: string;
  total_cost: number;
}

interface SecurityLog {
  request_id: string;
  tenant_id: string;
  timestamp: number;
  model: string;
  latency_ms: number;
  http_status: number;
  violations_triggered: string;
  pii_mappings: Record<string, string>;
}

export default function App() {
  const [window, setWindow] = useState<'1h' | '24h' | '30d'>('24h');
  const [kpis, setKpis] = useState<KpiData>({ total_spend: 0, blocked_injections: 0, avg_overhead_ms: 0 });
  const [analytics, setAnalytics] = useState<AnalyticsData>({ p95_latency: 0, total_cost: 0, avg_faithfulness: 0 });
  const [tenants, setTenants] = useState<TenantSpend[]>([]);
  const [logs, setLogs] = useState<SecurityLog[]>([]);
  const [selectedLog, setSelectedLog] = useState<SecurityLog | null>(null);
  const [lastRefreshed, setLastRefreshed] = useState<string>('');

  const fetchDashboardData = async () => {
    try {
      // 1. Fetch Overall KPIs
      const kpiRes = await fetch('/api/analytics/summary');
      if (kpiRes.ok) {
        const kpiJson = await kpiRes.json();
        setKpis(kpiJson);
      }

      // 2. Fetch Lookback-specific Aggregates (faithfulness, p95 latency)
      const analyticsRes = await fetch(`/api/analytics?window=${window}`);
      if (analyticsRes.ok) {
        const analyticsJson = await analyticsRes.json();
        setAnalytics(analyticsJson);
      }

      // 3. Fetch Tenant Spend Charting
      const tenantRes = await fetch(`/api/analytics/tenants?window=${window}`);
      if (tenantRes.ok) {
        const tenantJson = await tenantRes.json();
        setTenants(tenantJson);
      }

      // 4. Fetch Security/Vulnerability Logs
      const logsRes = await fetch('/api/security/logs');
      if (logsRes.ok) {
        const logsJson = await logsRes.json();
        setLogs(logsJson);
      }

      setLastRefreshed(new Date().toLocaleTimeString());
    } catch (err) {
      console.error('Failed to fetch dashboard data:', err);
    }
  };

  // Poll every 5 seconds, also trigger on lookback window change
  useEffect(() => {
    fetchDashboardData();
    const interval = setInterval(fetchDashboardData, 5000);
    return () => clearInterval(interval);
  }, [window]);

  // Map backend violation name to friendly name & CSS class
  const getViolationDetails = (violationStr: string) => {
    const v = violationStr.toUpperCase();
    if (v.includes('BUDGET_EXCEEDED') || v.includes('BUDGET_429_BREACH')) {
      return { friendly: 'BUDGET_429_BREACH', className: 'budget_429_breach' };
    }
    if (v.includes('MALICIOUS_INJECTION')) {
      return { friendly: 'MALICIOUS_INJECTION', className: 'malicious_injection' };
    }
    return { friendly: 'PII_REDACTED', className: 'pii_redacted' };
  };

  // Format date helper
  const formatDate = (epoch: number) => {
    return new Date(epoch * 1000).toLocaleString();
  };

  // Find max cost in tenants list for scaling custom charts
  const maxTenantCost = tenants.reduce((max, t) => (t.total_cost > max ? t.total_cost : max), 0.000001);

  return (
    <div className="dashboard-container">
      {/* HEADER SECTION */}
      <header className="dashboard-header">
        <div className="header-title-area">
          <h1>ShieldWall Gateway <span className="badge">Control Room</span></h1>
          <p className="header-subtitle">Real-time security auditing, data masking & LLM budget gate dashboard</p>
        </div>
        <div className="header-controls">
          <div className="refresh-indicator">
            <span className="refresh-dot"></span>
            Live Updates (Last sync: {lastRefreshed || 'connecting...'})
          </div>
        </div>
      </header>

      {/* LOOKBACK FILTER NAV */}
      <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1.5rem' }}>
        {(['1h', '24h', '30d'] as const).map((w) => (
          <button
            key={w}
            onClick={() => setWindow(w)}
            style={{
              padding: '0.5rem 1rem',
              borderRadius: '8px',
              border: '1px solid var(--border-color)',
              background: window === w ? 'var(--color-accent-blue)' : 'rgba(255, 255, 255, 0.02)',
              color: window === w ? '#000000' : 'var(--text-primary)',
              fontWeight: 600,
              cursor: 'pointer',
              transition: 'var(--transition-smooth)',
            }}
          >
            {w === '1h' ? '1 Hour Lookback' : w === '24h' ? '24 Hours Lookback' : '30 Days Lookback'}
          </button>
        ))}
      </div>

      {/* KPI RIBBON SECTION */}
      <section className="kpi-ribbon">
        {/* Card 1: Total Corporate Spend */}
        <div className="kpi-card spend">
          <div className="kpi-label">Corporate Spend ({window})</div>
          <div className="kpi-value">
            <span className="kpi-unit">$</span>
            {analytics.total_cost.toFixed(4)}
          </div>
          <div className="kpi-footer">Total accumulated: ${kpis.total_spend.toFixed(4)}</div>
        </div>

        {/* Card 2: System Overhead */}
        <div className="kpi-card overhead">
          <div className="kpi-label">System Overhead ({window})</div>
          <div className="kpi-value">
            {analytics.p95_latency.toFixed(0)}
            <span className="kpi-unit">ms</span>
          </div>
          <div className="kpi-footer">P95 Proxy response latency</div>
        </div>

        {/* Card 3: Blocked Injections */}
        <div className="kpi-card blocked">
          <div className="kpi-label">Blocked Injections (Lifetime)</div>
          <div className="kpi-value">
            {kpis.blocked_injections}
            <span className="kpi-unit"> events</span>
          </div>
          <div className="kpi-footer">Threats dropped at budget & policy gate</div>
        </div>

        {/* Card 4: Average Faithfulness */}
        <div className="kpi-card" style={{ background: 'var(--bg-card)' }}>
          <div className="kpi-label">Avg RAG Faithfulness ({window})</div>
          <div className="kpi-value" style={{ color: 'var(--color-accent-blue)' }}>
            {analytics.avg_faithfulness.toFixed(3)}
          </div>
          <div className="kpi-footer">Drift boundary: &gt;0.82 threshold</div>
        </div>
      </section>

      {/* CHART & TENANT SECTION */}
      <div className="middle-grid">
        {/* Financial Consumption Charting */}
        <div className="panel">
          <div className="panel-header">
            <h2 className="panel-title">Financial Consumption ({window})</h2>
          </div>
          <div className="chart-container">
            {tenants.length > 0 ? (
              tenants.map((t) => {
                const percentage = (t.total_cost / maxTenantCost) * 100;
                return (
                  <div className="chart-bar-row" key={t.tenant_id}>
                    <div className="bar-info">
                      <span className="bar-label">{t.tenant_id}</span>
                      <span className="bar-value">${t.total_cost.toFixed(6)}</span>
                    </div>
                    <div className="bar-wrapper">
                      <div className="bar-fill" style={{ width: `${percentage}%` }}></div>
                    </div>
                  </div>
                );
              })
            ) : (
              <p className="no-data-msg">No tenant consumption data available for this lookback window.</p>
            )}
          </div>
        </div>

        {/* Tenant Rankings List */}
        <div className="panel">
          <div className="panel-header">
            <h2 className="panel-title">Tenant Spend Rankings</h2>
          </div>
          <div className="tenant-list">
            {tenants.length > 0 ? (
              tenants.map((t, index) => (
                <div className="tenant-list-item" key={t.tenant_id}>
                  <div className="tenant-item-id">
                    {index + 1}. {t.tenant_id}
                  </div>
                  <div className="tenant-item-cost">${t.total_cost.toFixed(4)}</div>
                </div>
              ))
            ) : (
              <p className="no-data-msg">No tenants logged.</p>
            )}
          </div>
        </div>
      </div>

      {/* LIVE VULNERABILITY LOG */}
      <section className="panel logs-panel">
        <div className="panel-header">
          <h2 className="panel-title">Live Vulnerability & Security Log</h2>
        </div>
        <div className="table-wrapper">
          <table className="styled-table">
            <thead>
              <tr>
                <th>Timestamp</th>
                <th>Affected App Context</th>
                <th>Model</th>
                <th>Triggered Violation</th>
                <th>HTTP Status</th>
                <th>Overhead</th>
                <th>Diagnostics</th>
              </tr>
            </thead>
            <tbody>
              {logs.length > 0 ? (
                logs.map((log) => {
                  const details = getViolationDetails(log.violations_triggered);
                  return (
                    <tr key={log.request_id} onClick={() => setSelectedLog(log)}>
                      <td>{formatDate(log.timestamp)}</td>
                      <td>
                        <strong style={{ fontFamily: 'monospace' }}>{log.tenant_id}</strong>
                      </td>
                      <td>{log.model}</td>
                      <td>
                        <span className={`badge-violation ${details.className}`}>
                          {details.friendly}
                        </span>
                      </td>
                      <td style={{ color: log.http_status >= 400 ? 'var(--color-accent-pink)' : 'inherit' }}>
                        {log.http_status}
                      </td>
                      <td>{log.latency_ms}ms</td>
                      <td>
                        <button
                          className="btn-diagnostics"
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedLog(log);
                          }}
                        >
                          View Diagnostics
                        </button>
                      </td>
                    </tr>
                  );
                })
              ) : (
                <tr>
                  <td colSpan={7} style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-muted)' }}>
                    No security violations logged. All systems nominal.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      {/* DIAGNOSTICS VIEW MODAL */}
      {selectedLog && (
        <div className="modal-overlay" onClick={() => setSelectedLog(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2 className="modal-title">🛡️ Security Auditing Diagnostics</h2>
              <button className="btn-close-modal" onClick={() => setSelectedLog(null)}>
                &times;
              </button>
            </div>
            <div className="modal-body">
              <div className="modal-meta-row">
                <div className="meta-item">
                  <span className="meta-label">Request Trace ID</span>
                  <span className="meta-value" style={{ fontFamily: 'monospace' }}>{selectedLog.request_id}</span>
                </div>
                <div className="meta-item">
                  <span className="meta-label">Affected App Context</span>
                  <span className="meta-value">{selectedLog.tenant_id}</span>
                </div>
              </div>

              <div className="modal-meta-row">
                <div className="meta-item">
                  <span className="meta-label">Violation Triggered</span>
                  <span className="meta-value">
                    <span className={`badge-violation ${getViolationDetails(selectedLog.violations_triggered).className}`}>
                      {getViolationDetails(selectedLog.violations_triggered).friendly}
                    </span>
                  </span>
                </div>
                <div className="meta-item">
                  <span className="meta-label">Timestamp</span>
                  <span className="meta-value">{formatDate(selectedLog.timestamp)}</span>
                </div>
              </div>

              <div className="diagnostics-mapping-area">
                <h3 className="diagnostics-title">Masked String Variations (PII Redactions)</h3>
                <div className="mapping-list">
                  {selectedLog.pii_mappings && Object.keys(selectedLog.pii_mappings).length > 0 ? (
                    Object.entries(selectedLog.pii_mappings).map(([token, original]) => (
                      <div className="mapping-item" key={token}>
                        <div className="mapping-token">Placeholder Token: {token}</div>
                        <div className="mapping-original">Original Value: {original}</div>
                      </div>
                    ))
                  ) : (
                    <p className="no-mappings-placeholder">
                      {selectedLog.violations_triggered.includes('PII') 
                        ? 'PII mappings have expired (TTL is 5 minutes for security compliance).' 
                        : 'No PII replacements associated with this event type.'}
                    </p>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
