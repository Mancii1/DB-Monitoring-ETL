import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import os

Faker = Faker()
Faker.seed_instance(42)
np.random.seed(42)

#Config
DAYS = 14
START_DATE = datetime(2024, 6, 1)
LOG_INTERVAL_MINUTES = 15
OUTPUT_DIR = "/opt/airflow/data" #Inside Docker

#Server pool
SERVERS = [
    ("DB-PROD-01", "Production", "us-east-1"),
    ("DB-PROD-02", "Production", "us-west-2"),
    ("DB-STAGE-01", "Staging", "eu-central-1"),
    ("DB-QA-01", "QA", "ap-south-1")
]

ETL_JOB_NAMES = ["customer_load", "sales_aggregation", "inventory_sync", "report_refresh"]
ALERT_MESSAGES = [
    "High CPU usage detected",
    "Memory usage critical",
    "Query timeout exceeded",
    "Connection pool exhausted",
    "Disk space low",
]

#Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

#1. Generate db_logs

timestamps = pd.date_range(START_DATE, periods=DAYS * 24 * 60 // LOG_INTERVAL_MINUTES, freq=f"{LOG_INTERVAL_MINUTES}min")
db_logs = []

for ts in timestamps:
    for server, env, region in SERVERS:
        cpu = np.clip(np.random.normal(40, 20), 5, 95)  # CPU usage between 0-100%
        memory = np.clip(np.random.normal(55, 15), 10, 95)  # Memory usage between 0-100%
        query_time = np.random.exponential(50)  # Query time in ms
        active_conn = np.random.randint(10, 200)

        #status logic
        if cpu > 85 or memory > 90:
            status = "Critical"
        elif cpu > 70 or memory > 80:
            status = "Warning"
        else:
            status = "Healthy"

        db_logs.append({
            "server_name": server,
            "environment": env,
            "region": region,
            "timestamp": ts,
            "cpu_usage": round(cpu, 2),
            "memory_usage": round(memory, 2),
            "query_time_ms": round(query_time, 2),
            "active_connections": active_conn,
            "status": status
        })

df_logs = pd.DataFrame(db_logs)
df_logs.to_csv(os.path.join(OUTPUT_DIR, "db_logs.csv"), index=False)
print(f"Generated db_logs.csv with {len(df_logs)} records.")

#2. Generate etl_jobs

etl_jobs = []
for day_offset in range(DAYS):
    day = START_DATE + timedelta(days=day_offset)
    for job_name in ETL_JOB_NAMES:
        #Simulating a random start time during the day
        start_time = day + timedelta(hours=np.random.randint(0, 23),
                                     minutes=np.random.randint(0, 59))
        #Most jobs suceed, some fail
        status = np.random.choice(["Success", "Success", "Success", "Failed"], p= [0.85, 0.05, 0.05, 0.05])
        if status == "Failed":
            end_time = start_time + timedelta(minutes=np.random.randint(1, 10))
            rows_processed = 0
        else:
            end_time = start_time + timedelta(minutes=np.random.randint(5, 60))
            rows_processed = np.random.randint(1000, 50000)
        
        etl_jobs.append({
            "job_name": job_name,
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "rows_processed": rows_processed,
            "date": day.date()
        })

df_etl = pd.DataFrame(etl_jobs)
df_etl.to_csv(os.path.join(OUTPUT_DIR, "etl_jobs.csv"), index=False)
print(f"Generated etl_jobs.csv with {len(df_etl)} records.")

#3. Generate Alerts

alerts = []
#Use logs with Critical or Warning status to generate alerts
critical_logs = df_logs[df_logs["status"].isin(["Critical", "Warning"])]
for _, log in critical_logs.iterrows():
        if np.random.rand() < 0.3: #30% chance of generating an alert
            severity = "High" if log["status"] == "Critical" else "Medium"
            alerts.append({
                "server_name": log["server_name"],
                "severity": severity,
                "message": np.random.choice(ALERT_MESSAGES),
                "created_at": log["timestamp"] + timedelta(minutes=np.random.randint(5, 300))
            })

df_alerts = pd.DataFrame(alerts)
df_alerts.to_csv(os.path.join(OUTPUT_DIR, "alerts.csv"), index=False)
print(f"Generated alerts.csv with {len(df_alerts)} records.")