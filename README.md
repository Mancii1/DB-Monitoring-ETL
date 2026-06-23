# Database Monitoring ETL Pipeline with Airflow

![Python](https://img.shields.io/badge/Python-3.10-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-green)
![Airflow](https://img.shields.io/badge/Airflow-2.7.1-orange)
![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-yellow)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

A complete, production‑style **database monitoring and ETL pipeline** that simulates real‑time server performance logs, ETL job runs, and alerts. The pipeline is orchestrated by **Apache Airflow**, stored in a normalized **PostgreSQL** database, and visualised through an interactive **Power BI** dashboard. Built to demonstrate the exact skills required for a Junior Database Engineer or Data Engineer in a BPO or operations‑focused environment.

---

## 📖 Project Overview

**Goal:**  
To build an end‑to‑end monitoring system that:
- Generates realistic synthetic data (database logs, ETL jobs, alerts)
- Cleans, transforms, and normalises that data
- Stores it in a PostgreSQL database
- Runs on a schedule using Apache Airflow
- Surfaces key operational metrics in a Power BI dashboard

**Key Outcomes:**
- An automated, scheduled ETL pipeline with Airflow
- A normalised PostgreSQL database (4 tables)
- A set of reusable SQL analysis queries
- A dual‑page Power BI dashboard (Database Health & ETL Monitoring)
- Full documentation and version control (Git)

---

## 🧱 Tech Stack

| Category | Tools / Libraries |
|----------|-------------------|
| **Orchestration** | Apache Airflow 2.7.1 (Docker) |
| **Data Processing** | Python, Pandas, NumPy |
| **Data Generation** | Faker |
| **Database** | PostgreSQL 13 (Docker & local) |
| **Connection** | SQLAlchemy, psycopg2-binary |
| **BI & Analytics** | Power BI Desktop (DAX measures) |
| **Version Control** | Git, GitHub |
| **IDE** | VS Code |
| **Containerisation** | Docker, Docker Compose |

---

## 🚀 ETL & Monitoring Architecture

Data Generator (Python)
↓
CSV files (data/)
↓
Airflow DAG (@hourly)
↓
Python ETL scripts (extract → transform → load)
↓
PostgreSQL (normalised tables)
↓
SQL Analysis Queries
↓
Power BI Dashboard (Database Health + ETL Monitoring)

---

## 🧪 Datasets (Synthetic)

Three datasets were generated using the `Faker` and `NumPy` libraries to simulate 14 days of monitoring data:

### 1. `db_logs`
Server performance metrics logged every 15 minutes from 4 servers.
| Column | Description |
|--------|-------------|
| `server_name` | Server identifier (e.g., DB-PROD-01) |
| `environment` | Production, Staging, QA |
| `region` | us-east-1, eu-central-1, etc. |
| `timestamp` | Observation time |
| `cpu_usage` | CPU percentage (5–95) |
| `memory_usage` | Memory percentage (10–95) |
| `query_time_ms` | Simulated query duration (ms) |
| `active_connections` | Current connections |
| `status` | Healthy, Warning, Critical (based on thresholds) |

### 2. `etl_jobs`
Daily ETL job runs (customer_load, sales_aggregation, etc.) with success/failure status.
| Column | Description |
|--------|-------------|
| `job_name` | Name of the ETL job |
| `start_time` / `end_time` | Job run timestamps |
| `status` | Success / Failed |
| `rows_processed` | Rows affected |

### 3. `alerts`
System alerts triggered when metrics crossed thresholds.
| Column | Description |
|--------|-------------|
| `server_name` | Affected server |
| `severity` | High, Medium |
| `message` | Alert description |
| `created_at` | Timestamp |

---

## ⚙️ ETL Pipeline Details

### 1. Data Generation (`generate_data.py`)
- Creates realistic distributions for CPU, memory, query time, and connections.
- Assigns status based on thresholds (e.g., CPU > 85% → Critical).
- Generates 14 days of backfilled data.
- Runs once initially, but could be integrated into the DAG for continuous generation.

### 2. Extract (`extract.py`)
- Reads the three CSV files into Pandas DataFrames.
- Parses timestamp columns automatically.

### 3. Transform (`transform.py`)
- Cleans duplicates, clips out‑of‑range values, and standardises statuses.
- Adds derived features: `hour`, `date`, `is_critical` for dashboard convenience.
- Calculates ETL job durations (`duration_minutes`).

### 4. Load (`load.py`)
- Drops and recreates tables with correct schema (SERIAL primary keys, foreign keys).
- Inserts **servers** dimension via raw SQL to preserve `server_id`.
- Loads fact tables (`db_logs`, `etl_jobs`, `alerts`) using `to_sql` with explicit column selection.
- Handles dependency order gracefully (DROP CASCADE).

### 5. Airflow DAG (`monitoring_pipeline.py`)
- A single PythonOperator that calls the full ETL chain.
- Runs **@hourly** – simulating a continuous monitoring pipeline.
- Manually unpausing allows ad‑hoc runs.

---

## 📊 Power BI Dashboard

Two report pages serve the operations and engineering teams.

![Database Health](screenshots/Database_Health_Dashboard.png")
![ETL Monitoring](screenshots/ETL-Monitoring_Dashboard.png")

### **Database Health**
- **Cards:** Total Servers, Log Entries, Avg CPU, Avg Memory, Avg Active Connections, Avg Query Time.
- **Gauge:** Average CPU usage with a target of 75%.
- **Line Chart:** Active Connections over time.
- **Bar Chart:** Status distribution (Healthy / Warning / Critical).
- **Table:** Real‑time server health snapshot (CPU, Memory per server – last hour).
- **Slicers:** Environment, Region.

### **ETL Monitoring**
- **Cards:** Total Job Runs, Successful Runs, Failed Runs, Job Success Rate, Avg Job Duration.
- **Pie Chart:** Job status breakdown.
- **Line Chart:** Job duration trend.
- **Table:** Detailed job‑level log with status and rows processed.
- **Slicers:** Job Name, Status.

All visuals are driven by **DAX measures** created on the `etl_jobs` and `db_logs` fact tables, using relationships with the `servers` dimension. This star‑schema design enables cross‑filtering and drill‑down.

---

## 🧠 Challenges & Solutions

### 1. Docker Airflow PostgreSQL Image Version Conflict
**Problem:** The stack pulled `postgres:18` instead of `postgres:13`, causing volume incompatibility errors.
**Solution:** Explicitly pinned `image: postgres:13` in `docker-compose.yaml` and cleared old volumes with `docker volume rm`.

### 2. Port Conflict with Local PostgreSQL
**Problem:** Local PostgreSQL service on port 5432 prevented the Docker container from starting.
**Solution:** Stopped the local service via `services.msc`; documented how to restart when needed.

### 3. Airflow Webserver “Invalid Login”
**Problem:** No default admin user exists in the official Airflow Docker image.
**Solution:** Added an `airflow-create-user` service that runs `airflow users create` at startup, ensuring the `admin/admin` credentials are always available.

### 4. ETL Import Errors in the DAG
**Problem:** `ModuleNotFoundError: No module named 'scripts'` when the DAG tried to import the ETL scripts.
**Solution:** Added `sys.path.insert(0, '/opt/airflow')` at the top of the DAG file so Python can find the `scripts` package.

### 5. Foreign Key Constraint on Table Drop
**Problem:** `load.py` used `to_sql(if_exists="replace")` which tried to drop `servers` before its dependent tables.
**Solution:** Replaced with raw SQL `DROP TABLE ... CASCADE` and explicit `CREATE TABLE` statements to control the load order precisely.

### 6. Column Mismatch on Insert
**Problem:** Derived columns (e.g., `duration_minutes`) that weren’t in the database schema caused insert failures.
**Solution:** Selected only the expected columns before `to_sql` (e.g., `etl_df[["job_name","start_time","end_time","status","rows_processed"]]`).

### 7. DAX Measure for Missing Column
**Problem:** Power BI needed average job duration but the column wasn’t loaded.
**Solution:** Created a DAX measure using `DATEDIFF(etl_jobs[start_time], etl_jobs[end_time], MINUTE)` – demonstrating the separation of transformation and reporting logic.

---

## 🏆 Why This Project Matters

This pipeline directly demonstrates the responsibilities of a Junior Database Engineer:

- **PostgreSQL** – schema design, normalisation, foreign keys, querying.
- **ETL Processes** – full Python pipeline with extract, transform, load.
- **Production Support** – monitoring database health, job success/failure, alerting.
- **Scheduling & Automation** – Airflow DAG with hourly runs.
- **Reporting** – Power BI dashboards for operations and management.
- **Operational Stability** – handling data quality issues, dependency errors, and containerised deployment.

It also showcases modern **data engineering practices** like Docker, environment isolation, and version‑controlled SQL/Python/DAGs.

---

