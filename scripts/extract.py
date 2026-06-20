import pandas as pd
import os

DATA_DIR = "/opt/airflow/data"

def extract_data():
    logs = pd.read_csv(os.path.join(DATA_DIR, "db_logs.csv"), parse_dates=["timestamp"])
    etl = pd.read_csv(os.path.join(DATA_DIR, "etl_jobs.csv"), parse_dates=["start_time", "end_time"])
    alerts = pd.read_csv(os.path.join(DATA_DIR, "alerts.csv"), parse_dates=["created_at"])
    print(f"[EXTRACT] Loaded {len(logs)} logs, {len(etl)} jobs, {len(alerts)} alerts")
    return logs, etl, alerts