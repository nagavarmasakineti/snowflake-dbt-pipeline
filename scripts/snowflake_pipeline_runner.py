import os
# import sys
# import time
# import requests
from datetime import datetime
from flask import jsonify
from dotenv import load_dotenv
from airflow_health_check import check_dags_health
from airflow_api_clientV1 import get_access_token, unpause_dag, trigger_dag_run
from snowflake_config import  get_snowflake_connection

# Load dot env
load_dotenv()

host_name = os.getenv("AIRFLOW_HOST_NAME", "http://127.0.0.1:8080")

STREAM_NAME = 'RAW_STAGE_FILE_STREAM'
LOOKBACK_MINUTES = 15 #checks for files uploaded in last 15 minutes


# Step 1: Check Snowflake Stage / Stream
def check_snowflake_stream():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Checking in stream {STREAM_NAME} for new files...")

    conn = None
    try:   
        # Reuse Snowflake Connection
        conn = get_snowflake_connection()
        cur = conn.cursor()

        # Refresh Stram Data
        cur.execute(f"ALTER STAGE MY_RAW_STAGE REFRESH;")

        # Check if Stream has Data
        row = cur.execute(f"SELECT SYSTEM$STREAM_HAS_DATA('{STREAM_NAME}');").fetchone()
        stream_has_data = row[0] if row else False
        print(f"The value of stream_has_data :: {stream_has_data}")

        if stream_has_data:
            print(f"New Files Detected!, Resetting Stream Trigger...")
            #1. Reset Stream Offset
            cur.execute(F"CREATE OR REPLACE STREAM {STREAM_NAME} ON STAGE MY_RAW_STAGE;")

            #2. Ingest the data into the raw tables
            print("Bronze Layer :: Loading the data into raw table....")
            cur.execute("""
            COPY INTO healthcare_analytics.raw.raw_patients (
                patient_id, first_name, last_name, gender, dob, created_at)
                FROM (SELECT $1, $2, $3, $4, $5, CURRENT_TIMESTAMP() FROM @MY_RAW_STAGE/patients) 
                FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER=1);""")
            cur.execute("""COPY INTO healthcare_analytics.raw.raw_visit (visit_id, patient_id, visit_date, diagnosis_code, total_amount)
            FROM (SELECT $1, $2, $3, $4, $5 FROM @MY_RAW_STAGE/visits) 
            FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER=1);""")
            

            #3. Check Airflow Health
            if not check_dags_health():
                print(f"Airflow health check failed, Aborting Pipeline Trigger...")
                return jsonify({"status": "Failed", "reason":"Airflow is not healthy"}), 503
            print(f"********************** Health Check Completed ****************************")
            
            #3. Retrieve Token
            token = get_access_token()
            if not token:
                print(f"Autahntication of Airflow Failed, Aborting Pipeline...")
                return jsonify({"status":"Failed", "reason": "Airflow Authantication Failed"}), 401
            print(f"********************** JWS Token Received ****************************")
            
            # 4. Unsuspend DAG and Trigger
            target_dag_id = "02_snowflake_dbt_pipeline"
            print(f" --> [SUCCESS]: Fetch Token Successfull. Unsuspending DAG...")
            if unpause_dag(target_dag_id, token, host_name):
                run_data = trigger_dag_run('02_snowflake_dbt_pipeline', token, host_name)
                print(f"--> [SUCCESS]: Trigger DAG Successfull: {run_data}")
            else:
                print("Aborting trigger because DAG could not be unpaused.")
        else:
            print(f"This {STREAM_NAME} Stream has no updated data")

        
    except Exception as e:
        print(f"Failed in checking files in Stream: {e}")

if __name__ == "__main__":
    check_snowflake_stream()
