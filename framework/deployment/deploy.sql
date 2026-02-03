CREATE SCHEMA IF NOT EXISTS demo;

-- Bronze
CREATE TABLE IF NOT EXISTS demo.bronze_customers (
    ingest_ts TIMESTAMP,
    source STRING,
    raw_payload STRING
);

-- Silver (si quieres crearla aquí también, opcional)
CREATE TABLE IF NOT EXISTS demo.silver_customers (
    ingest_ts TIMESTAMP,
    customer_id INT,
    name STRING,
    age INT,
    country STRING,
    email STRING,
    dq_is_name_null BOOLEAN,
    dq_is_underage BOOLEAN,
    dq_status STRING
);

-- Monitoring / Ops
CREATE TABLE IF NOT EXISTS demo.ops_event_log (
    run_id STRING,
    event_ts TIMESTAMP,
    stage STRING,
    status STRING,
    details STRING
);

-- Resultados de validación (para cuando hagamos quality “formal”)
CREATE TABLE IF NOT EXISTS demo.ops_dq_results (
    run_id STRING,
    checked_at TIMESTAMP,
    dataset STRING,
    check_name STRING,
    status STRING,
    metric DOUBLE,
    details STRING
);