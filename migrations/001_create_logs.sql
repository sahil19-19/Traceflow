-- ClickHouse DDL for the observability platform logs table

CREATE DATABASE IF NOT EXISTS observability;

CREATE TABLE IF NOT EXISTS observability.logs
(
    timestamp   DateTime,
 
    -- LowCardinality(String) for columns with few distinct values (like service names or log levels),
    service     LowCardinality(String),
    level       LowCardinality(String),  -- INFO / WARN / ERROR / DEBUG
 
    -- regular String for high-cardinality free-text fields.
    message     String,
    trace_id    String,
 
    -- JSON stored as a plain String
    metadata    String
)
ENGINE = MergeTree()

ORDER BY (timestamp, service) 
-- acts like primary index
-- timestamp first because time-range scans are the primary access pattern

TTL timestamp + INTERVAL 30 DAY; -- deletes rows older than 30 days