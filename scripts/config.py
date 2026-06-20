from sqlalchemy import create_engine

DATABASE_URL = "postgresql://airflow:airflow@postgres:5432/monitoring_db"
engine = create_engine(DATABASE_URL)