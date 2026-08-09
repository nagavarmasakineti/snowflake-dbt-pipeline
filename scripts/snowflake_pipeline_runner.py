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
        stream_has_data = cur.execute(f"SELECT SYSTEM$STREAM_HAS_DATA('{STREAM_NAME}');")

        if stream_has_data:
            print(f"Stram has uncommitted files.")
            #1. Check Airflow Health
            if not check_dags_health():
                print(f"Airflow health check failed, Aborting Pipeline Trigger...")
                return jsonify({"status": "Failed", "reason":"Airflow is not healthy"}), 503
            print(f"********************** Health Check Completed ****************************")
            
            #2. Retrieve Token
            token = get_access_token()
            if not token:
                print(f"Autahntication of Airflow Failed, Aborting Pipeline...")
                return jsonify({"status":"Failed", "reason": "Airflow Authantication Failed"}), 401
            print(f"********************** JWS Token Received ****************************")
            
            # 3. Unsuspend DAG and Trigger
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
