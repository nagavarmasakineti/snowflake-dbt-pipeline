from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Step 1: Set default taks arguments
default_args = {
    'owner' : 'data_engineer',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

# Step 2: Define a simple python function to run in a task
def print_python_welcome():
    print("Python operator is working")

# Step 3: Initiate the DAG
with DAG(
    dag_id = '01_hello_world_test',
    default_args = default_args,
    description='A simple text DAG',
    schedule=None,
    start_date = datetime(2026,1,1),
    catchup= False,
    tags=['test', 'basic']
) as dag:
    # Task A: Run a terminal command
    task_bash = BashOperator(
        task_id = 'Say_hello_bash',
        bash_command='echo "This should work now"'
    )

    # Task B: Run a python function
    task_python = PythonOperator(
        task_id = 'Say_hello_python',
        python_callable=print_python_welcome
    )

    # Step 4 : Set Task Dependency(Bash run first and then python)
    task_bash >> task_python
