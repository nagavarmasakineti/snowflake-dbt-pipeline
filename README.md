# End-to-End Automated Data Ingestion & Transformation Pipeline (Snowflake + Airflow + dbt)

This project demonstrates an automated Medallion Architecture data pipeline that ingests raw healthcare CSV datasets, validates schema integrity, stages data to Snowflake, checks for state changes via Snowflake Directory Streams, and triggers an Airflow DAG to perform dbt modeling and testing.

---

## 🏗️ Architecture Overview

1. **Local File Validation & Staging (`validate_stage_files.py`)**:
   * Scans a local drop directory (`data_drop`) for inbound raw CSV files.
   * Performs schema, non-empty, and file name pattern validations.
   * Routes invalid files to `data_processing_failed/`.
   * Uploads valid files directly to a Snowflake Internal Stage (`@MY_RAW_STAGE`) via `PUT`.

2. **Event-Driven Orchestration Trigger (`check_snowflake_stream.py`)**:
   * Refreshes the Snowflake stage directory and evaluates `SYSTEM$STREAM_HAS_DATA` on a Directory Stream.
   * Executes a `COPY INTO` operation to load stage data into Bronze (`RAW`) tables.
   * Verifies Airflow cluster health via Airflow REST API.
   * Authenticates and dynamically triggers the downstream Airflow DAG (`02_snowflake_dbt_pipeline`).

3. **Orchestration & Transformation (`02_snowflake_dbt_pipeline.py` & dbt)**:
   * Airflow DAG runs `dbt debug` connection checks.
   * Executes `dbt build --select staging` (Silver Layer: Cleaning, Type Casting, Deduplication).
   * Executes `dbt build --select marts` (Gold Layer: Dimensional & Fact Modeling).

---

## 📂 Project Structure

```text
├── data_drop/                    # Landing folder for raw incoming CSVs
├── data_processing_failed/       # Quarantine folder for invalid files
├── dags/
│   ├── 02_snowflake_dbt_pipeline.py # Airflow DAG orchestration definition
│   └── dbt_project/              # dbt project directory
│       ├── models/
│       │   ├── staging/          # Silver layer dbt models
│       │   └── marts/            # Gold layer dbt models
│       └── profiles.yml          # dbt connection profiles
├── validate_stage_files.py       # Python file ingestion & validation engine
├── check_snowflake_stream.py     # Stream monitoring & Airflow trigger service
├── snowflake_setup.sql           # DDL/DML for Warehouses, RBAC, Tables, & Stages
└── README.md
