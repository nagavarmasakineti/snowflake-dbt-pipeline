import os
from flask import Flask, jsonify, request
from airflow_health_check import check_dags_health
from airflow_api_clientV1 import get_access_token, trigger_dag

app = Flask(__name__)

@app.route("/snowflake-webhook", methods=["POST"])
def handle_snowflake_notification():
    payload = request.get_json(silent=True) or {}
    print(f"Received Notification from Snowflake: {payload}")

    #1. Check Airflow Health
    if not check_dags_health():
        print(f"Airflow health check failed, Aborting Pipeline Trigger...")
        return jsonify({"status": "Failed", "reason":"Airflow is not healthy"}), 503

    #2. Retrieve Token
    token = get_access_token()
    if not token:
        print(f"Autahntication of Airflow Failed, Aborting Pipeline...")
        return jsonify({"status":"Failed", "reason": "Airflow Authantication Failed"}), 401

    #3. API Call to DAG
    target_dag_id = "01_hello_world_test"
    dag_run_response = get_all_dags(token)

    if dag_run_response:
        print(f"Successfully Triggered DAG: {target_dag_id}")
        return jsonify({"status": "success", 
                        "dag_id":target_dag_id,
                        "dag_run_id" : dag_run_response.get("dag_run_id")}), 200
    else:
        print(f"Triggering DAG call Failed")
        return jsonify({"status": "failed", "reason": "DAG Trigger Failed"}), 500


if __name__ =="__main__":
    app.run(host="0.0.0.0", port=5000)



