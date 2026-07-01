CREATE TABLE IF NOT EXISTS taxi_speed (
    id               BIGSERIAL PRIMARY KEY,
    taxi_id          INTEGER NOT NULL,
    event_timestamp  TEXT,
    longitude        DOUBLE PRECISION,
    latitude         DOUBLE PRECISION,
    speed            DOUBLE PRECISION,
    average_speed    DOUBLE PRECISION,
    total_distance   DOUBLE PRECISION,
    is_speeding      BOOLEAN NOT NULL DEFAULT FALSE,
    is_out_of_area   BOOLEAN NOT NULL DEFAULT FALSE,
    last_moved       TEXT,
    is_parking       BOOLEAN NOT NULL DEFAULT FALSE,
    ingested_at      BIGINT,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_taxi_speed_taxi_id     ON taxi_speed (taxi_id);
CREATE INDEX IF NOT EXISTS idx_taxi_speed_received_at ON taxi_speed (received_at);