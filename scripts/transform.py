import pandas as pd
import numpy as np

def transform_data(logs_df, etl_df, alerts_df):
    #1. Clean db_logs - removing duplicates, ensure status values
    logs_df = logs_df.drop_duplicates()
    logs_df["cpu_usage"] = logs_df["cpu_usage"].clip(0, 100)
    logs_df["memory_usage"] = logs_df["memory_usage"].clip(0, 100)

    #Starts everyone as Healthy, then override
    logs_df["status"] = "Healthy"
    logs_df.loc[
        (logs_df["cpu_usage"] > 70) | (logs_df["memory_usage"] > 80), "status"
    ] = "Warning"
    logs_df.loc[
        (logs_df["cpu_usage"] > 85) | (logs_df["memory_usage"] > 90), "status"
    ] = "Critical"

    #2. Clean etl jobs: handle missing end_time for failed jobs (set to start_time)
    etl_df["end_time"] = etl_df["end_time"].fillna(etl_df["start_time"])
    etl_df["rows_processed"] = etl_df["rows_processed"].fillna(0).astype(int)
    etl_df = etl_df.drop_duplicates()

    #3. Clean alerts: Remove Duplicates
    alerts_df = alerts_df.drop_duplicates()

    #4. Feature Engineering (add derived columns for dashboard)
    logs_df["hour"] = logs_df["timestamp"].dt.hour
    logs_df["date"] = logs_df["timestamp"].dt.date
    logs_df["is_critical"] = logs_df["status"] == "Critical"

    #ETL job duration in minutes
    etl_df["duration_minutes"] = (etl_df["end_time"] - etl_df["start_time"]).dt.total_seconds() / 60

    print(f"[TRANSFORM] Cleaned: {len(logs_df)} logs, {len(etl_df)} jobs, {len(alerts_df)} alerts")
    return logs_df, etl_df, alerts_df
