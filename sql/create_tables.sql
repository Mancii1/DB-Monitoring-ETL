--Create tables for Airflow metadata database

--Dimension: servers
CREATE TABLE IF NOT EXISTS servers (
    server_id SERIAL PRIMARY KEY,
    server_name TEXT UNIQUE NOT NULL,
    environment TEXT,
    region TEXT
);

-- Fact: Database performance logs
CREATE TABLE IF NOT EXISTS db_logs (
    log_id SERIAL PRIMARY KEY,
    server_id INT REFERENCES servers(server_id),
    timestamp TIMESTAMP NOT NULL,
    cpu_usage DOUBLE PRECISION,
    memory_usage DOUBLE PRECISION,
    query_time_ms DOUBLE PRECISION,
    active_connections INT,
    status TEXT
);

--Fact: ETL job runs
CREATE TABLE IF NOT EXISTS etl_jobs (
    job_id SERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status TEXT
);

--Fact: Alerts
CREATE TABLE IF NOT EXISTS alerts (
    alert_id SERIAL PRIMARY KEY,
    server_id INT REFERENCES servers(server_id),
    severity TEXT,
    message TEXT,
    created_at TIMESTAMP NOT NULL
);