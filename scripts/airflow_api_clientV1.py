import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timezone
from airflow_health_check import check_dags_health


# Force Pythin requests to use direct ipv4 loopback  ignore windows system proxies
os.environ["NO_PROXY"] = '127.0.0.1,localhost'

#Load ENV file
load_dotenv()

# Fetch ENV Values
host_name = os.getenv("AIRFLOW_HOST_NAME", "http://127.0.0.1:8080")
access_Token_URL = os.getenv("AIRFLOW_ACCESS_TOKEN_URL", "auth/token")
username = os.getenv("AIRFLOW_USER", "admin")
password = os.getenv("AIRFLOW_PASSWORD", "admin")



# Fetch Access Token
def get_access_token():
    """Fetch JWT Token for Airflow Connection"""
    token_url = f"{host_name}/{access_Token_URL}"
    payload = {
            "username": username,
            "password": password
        }

    try:
            print(f"URL is : {token_url}")
            print(f"Payload is : {payload}")

            response = requests.post(
                 token_url,
                 json = payload,
                 headers={"Content-Type" : "application/json"},
                 timeout= 20
            )

            response.raise_for_status()

            print(f"Status Code is : {response.status_code}")
            #print(f"Reponse: {response.text}")
            token_data = response.json()
            token = token_data.get("access_token")
            print("Access Token Retrieved Successfully")
            return token
            

    except Exception as e:
        print(f"Failed to generate access token for airflow connection: {e}")


#Fetch All Dags --Not Using
# def get_all_dags(token):
#     """Use JWS Authantication and call DAG Engine"""
#     if not token:
#         print(f"No Valid token provided. Skipping API Request")
#         return None

#     # Airflow URL
#     dags_url = f"{host_name}/{base_URL}?limit=2"

#     # Send Token using bearer autahntication
#     headers = {
#          "Authorization" : f"Bearer {token}",
#          "Content-Type" : "application/json"
#     }

#     try :
#         response = requests.get(dags_url,headers=headers,timeout=10)
#         response.raise_for_status()

#         dags_data = response.json()
#         print(f"Successfully Receives DAGs List")
#         print(dags_data)
#         return dags_data

#     except Exception as e:
#          print(f"Calling DAGS API is failed: {e}")
#          return None

# Unsuspend particular DAG and Trigger it
def unpause_dag(dag_id: str, access_token: str, base_url: str) -> bool:
     """This function will be used to unpause the dag"""
     if not access_token:
         print(f"No Valid token provided. Unable to proceed wih DAG Unpause")
         return False
     
     # Unsuspend DAG
     url = f"{base_url}/api/v2/dags/{dag_id}"
     payload = {"is_paused": "False"}
     headers = {
         "Authorization" : f"Bearer {access_token}",
         "Content-Type" : "application/json"
     }
     try:
         response = requests.patch(url,json=payload,headers = headers,timeout = 20)
         response.raise_for_status()

         if response.status_code == 200:
              print(f"--> [Success] DAG {dag_id} is active(unsuspended).")
              return True
         else : 
              print(f"--> [ERROR] Failed to unsuspend DAG {dag_id}")
              return False
     except Exception as e:
          print(f"--> [ERROR] Network Error while unsuspending the dag {dag_id}:{e}")
          return False

def trigger_dag_run(dag_id: str, access_token: str, base_url: str) ->tuple[int,dict]:
     """Triggers a new execution for the specified DAG
        And returns tuple[int,dict]:(status_code, response_payload)"""
     
     url = f"{base_url}/api/v2/dags/{dag_id}/dagRuns"
     headers = {
                    "Authorization" : f"Bearer {access_token}",
                    "Content-Type" : "application/json"
                }
     payload = {
                 "logical_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                 "conf": {"triggered_by": "snowflake_pipeline_runner"}
                }
     

     try:
          response = requests.post(url,json= payload,headers=headers,timeout=10)
          status_code = response.status_code

          try:
               res_data = response.json()
          except ValueError:
               res_data = {"text":response.text}

          if(status_code in [200, 201]):
               dag_run_id = res_data.get("dag_run_id")
               print(f" -> [SUCCESS] Triggered {dag_id}, dag_run_id: {dag_run_id}")
               return status_code, {"dag_run_id": dag_run_id, "dag_data": res_data}

          else:
               print(f" ->[ERROR] Trigger Failed({status_code}, {res_data})")
               return status_code, {"error":res_data}   

     except Exception as e:
          print(f"--> [ERROR] Network/Client Exception: {e}")


     

#Call this function
if __name__ == "__main__":
    # Check Helath
    if not check_dags_health():
         print(f"Airflow Health Check is failed, Aborting the call")
    else :
        print(f"Airflow is Healthy, Going with fetch token")
        # Fetch Token String
        jwt_token = get_access_token()

        if jwt_token:
            print(f" --> [SUCCESS]: Fetch Token Successfull. Unsuspending DAG...")
            if unpause_dag('02_snowflake_dbt_pipeline', jwt_token, host_name):
                run_data = trigger_dag_run('02_snowflake_dbt_pipeline', jwt_token, host_name)
                print(f"--> [SUCCESS]: Trigger DAG Successfull: {run_data}")
            else:
                print("Aborting trigger because DAG could not be unpaused.")
                 
