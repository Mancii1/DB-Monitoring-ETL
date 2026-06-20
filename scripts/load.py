import pandas as pd
from scripts.config import engine

def load_data(logs_df, etl_df, alerts_df):
    # ------------------------------------------------------------
    # 1. DROP ALL TABLES (dependency order)
    # ------------------------------------------------------------
    with engine.begin() as conn:
        conn.execute("DROP TABLE IF EXISTS db_logs CASCADE")
        conn.execute("DROP TABLE IF EXISTS alerts CASCADE")
        conn.execute("DROP TABLE IF EXISTS etl_jobs CASCADE")
        conn.execute("DROP TABLE IF EXISTS servers CASCADE")
    print("[LOAD] Dropped existing tables")

    # ------------------------------------------------------------
    # 2. CREATE TABLES WITH CORRECT SCHEMA
    # ------------------------------------------------------------
    with engine.begin() as conn:
        conn.execute("""
            CREATE TABLE servers (
                server_id SERIAL PRIMARY KEY,
                server_name TEXT UNIQUE NOT NULL,
                environment TEXT,
                region TEXT
            );
        """)
        conn.execute("""
            CREATE TABLE db_logs (
                log_id SERIAL PRIMARY KEY,
                server_id INT REFERENCES servers(server_id),
                timestamp TIMESTAMP NOT NULL,
                cpu_usage DOUBLE PRECISION,
                memory_usage DOUBLE PRECISION,
                query_time_ms DOUBLE PRECISION,
                active_connections INT,
                status TEXT
            );
        """)
        conn.execute("""
            CREATE TABLE etl_jobs (
                job_id SERIAL PRIMARY KEY,
                job_name TEXT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                status TEXT,
                rows_processed INT
            );
        """)
        conn.execute("""
            CREATE TABLE alerts (
                alert_id SERIAL PRIMARY KEY,
                server_id INT REFERENCES servers(server_id),
                severity TEXT,
                message TEXT,
                created_at TIMESTAMP NOT NULL
            );
        """)
    print("[LOAD] Created tables with correct schema")

    # ------------------------------------------------------------
    # 3. INSERT SERVERS (raw SQL to keep SERIAL)
    # ------------------------------------------------------------
    servers_df = logs_df[["server_name", "environment", "region"]].drop_duplicates()
    with engine.begin() as conn:
        for _, row in servers_df.iterrows():
            conn.execute(
                """INSERT INTO servers (server_name, environment, region)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (server_name) DO NOTHING""",
                (row["server_name"], row["environment"], row["region"])
            )
    print(f"[LOAD] Inserted {len(servers_df)} servers")

    # Read back mapping
    servers_sql = pd.read_sql("SELECT server_id, server_name FROM servers", engine)

    # ------------------------------------------------------------
    # 4. LOAD DB_LOGS (only expected columns)
    # ------------------------------------------------------------
    logs_with_id = logs_df.merge(servers_sql, on="server_name", how="left")
    log_cols = ["server_id", "timestamp", "cpu_usage", "memory_usage",
                "query_time_ms", "active_connections", "status"]
    logs_with_id[log_cols].to_sql("db_logs", engine, if_exists="append", index=False)
    print(f"[LOAD] Inserted {len(logs_with_id)} db_logs")

    # ------------------------------------------------------------
    # 5. LOAD ETL_JOBS (only expected columns)
    # ------------------------------------------------------------
    etl_cols = ["job_name", "start_time", "end_time", "status", "rows_processed"]
    etl_df[etl_cols].to_sql("etl_jobs", engine, if_exists="append", index=False)
    print(f"[LOAD] Inserted {len(etl_df)} etl_jobs")

    # ------------------------------------------------------------
    # 6. LOAD ALERTS (only expected columns)
    # ------------------------------------------------------------
    alerts_with_id = alerts_df.merge(servers_sql, on="server_name", how="left")
    alert_cols = ["server_id", "severity", "message", "created_at"]
    alerts_with_id[alert_cols].to_sql("alerts", engine, if_exists="append", index=False)
    print(f"[LOAD] Inserted {len(alerts_with_id)} alerts")