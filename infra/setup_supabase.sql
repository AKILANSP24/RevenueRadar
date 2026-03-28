-- Create raw_events table
CREATE TABLE raw_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id TEXT NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    amount FLOAT NOT NULL,
    currency TEXT DEFAULT 'INR',
    timestamp TIMESTAMPTZ NOT NULL,
    customer_id TEXT,
    plan_tier TEXT,
    region TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create anomaly_events table
CREATE TABLE anomaly_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id TEXT NOT NULL,
    source TEXT,
    amount FLOAT,
    timestamp TIMESTAMPTZ,
    severity TEXT,
    z_score FLOAT,
    baseline_mean FLOAT,
    baseline_std FLOAT,
    ai_explanation TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create daily_health_scores table
CREATE TABLE daily_health_scores (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL UNIQUE,
    health_score FLOAT,
    total_events INT DEFAULT 0,
    anomaly_count INT DEFAULT 0,
    critical_count INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Add indexes on timestamp and severity
CREATE INDEX IF NOT EXISTS idx_raw_events_timestamp ON raw_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_timestamp ON anomaly_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_severity ON anomaly_events(severity);

-- Enable Realtime on raw_events
ALTER PUBLICATION supabase_realtime ADD TABLE raw_events;
