CREATE TABLE IF NOT EXISTS cdc_events
(
    id SERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    operation VARCHAR(10),
    source_table VARCHAR(100),
    record_id INTEGER,
    payload JSONB
);