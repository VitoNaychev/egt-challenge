CREATE TABLE IF NOT EXISTS events (
    id VARCHAR PRIMARY KEY,
    session_id VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    message VARCHAR NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    -- persistence time, filled by the database; intentionally not part of
    -- the domain model — `timestamp` is when the event occurred at the
    -- source, `created_at` is when it landed here
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
