CREATE TABLE IF NOT EXISTS active_rider_tracking (
    rider_id        UUID PRIMARY KEY,
    order_id        UUID NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude       DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    current_status  VARCHAR(20) NOT NULL CHECK (current_status IN ('AT_VENDOR', 'EN_ROUTE', 'ARRIVED', 'DELIVERED')),
    eta_minutes     INTEGER NOT NULL CHECK (eta_minutes >= 0),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- rider_id is PRIMARY KEY, which is both NOT NULL and UNIQUE
-- this is what ON CONFLICT (rider_id) in the UPSERT targets, and it directly encodes "a rider has exactly one live physical location at a time."
CREATE INDEX IF NOT EXISTS idx_active_rider_tracking_order_id
    ON active_rider_tracking (order_id);