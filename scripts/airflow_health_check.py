import os
import requests
from dotenv import load_dotenv

# Force Pythin requests to use direct ipv4 loopback  ignore windows system proxies
os.environ["NO_PROXY"] = '127.0.0.1,localhost'

#load dot env
load_dotenv()

health_url = os.getenv("AIRFLOW_HOST_NAME", 'http://127.0.0.1:8080')

def check_dags_health():

    print("Checking Airflow Health Status....")
    health_check_url = f"{health_url}/api/v2/monitor/health"
    try:
        response = requests.get(health_check_url,timeout=5)
        response.raise_for_status()

        health_data = response.json()
        print(f'Airflow host is reachable and healthy')
        return True


    except Exception as e:
        print(f'Failed in connecting to airflow: {e}')
        return False

if __name__ == "__main__":
    # Fetch Token String
    check_dags_health()

