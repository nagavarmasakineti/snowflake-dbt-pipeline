from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
#from airflow.providers.standard.operators.empty import EmptyOperator

#Default Afguments applied to all tasks
default_args = {
    'owner':'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 1)
}

with DAG (
    dag_id = '02_snowflake_dbt_pipeline',
    default_args = default_args,
    description = 'Orchestrates Snowflake raw data checks and dbt transformation',
    schedule = None,
    start_date = datetime(2026, 1, 1),
    catchup = False,
    tags = ['snowflake', 'dbt', 'production']
) as dag:

    # 1: Start Pipeline Marker
    def log_start():
        print("Step 1: Start Pipeline is successfull!")
    start_pipeline = PythonOperator(
        task_id = 'start_pipeline',
        python_callable = log_start
    )

    # 2: Test dbt connection to snowflake
    Test_DBT_Snowflake_Connection = BashOperator(
        task_id = 'Test_DBT_Snowflake_Connection',
        bash_command = 'cd /opt/airflow/dags/dbt_project && dbt debug --profiles-dir . && echo "Step 2: Connection between dbt and snowflake is successfull"'
    )

    # 3: Run the DBT Models
    run_dbt_models = BashOperator(
        task_id = 'run_dbt_models',
        bash_command = 'cd /opt/airflow/dags/dbt_project && dbt build'
    )

    # Task Dependencies: Step Order
    start_pipeline >> Test_DBT_Snowflake_Connection >> run_dbt_models


