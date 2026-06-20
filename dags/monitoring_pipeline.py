from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, '/opt/airflow')

from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_etl():
    logs, etl, alerts = extract_data()
    logs, etl, alerts = transform_data(logs, etl, alerts)
    load_data(logs, etl, alerts)
    print("Airflow DAG: ETL pipeline completed successfully")

with DAG(
    'monitoring_etl_pipeline',
    default_args=default_args,
    description='Database monitoring ETL pipeline',
    schedule_interval='@hourly',
    start_date=datetime(2024, 6, 1),
    catchup=False,
    tags=['monitoring', 'etl'],
) as dag:

    etl_task = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=run_etl,
    )

    etl_task