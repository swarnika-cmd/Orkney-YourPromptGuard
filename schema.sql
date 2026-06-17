-- SQL Migration: Time-Series Logs Schema for ShieldWall
CREATE TABLE IF NOT EXISTS request_logs (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) UNIQUE NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    model VARCHAR(64) NOT NULL,
    upstream_provider VARCHAR(64) NOT NULL,
    latency_ms INTEGER NOT NULL,
    http_status INTEGER NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    cost NUMERIC(10, 6) NOT NULL,
    violations_triggered VARCHAR(256)
);

-- Optimize for dashboard aggregations filtering by tenant over time
CREATE INDEX IF NOT EXISTS idx_tenant_timestamp ON request_logs(tenant_id, timestamp DESC);

-- Optimize for dashboard performance KPIs (e.g. latency distributions over time)
CREATE INDEX IF NOT EXISTS idx_timestamp_latency ON request_logs(timestamp DESC, latency_ms);
