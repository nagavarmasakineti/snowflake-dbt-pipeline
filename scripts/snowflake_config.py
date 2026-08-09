import os
from dotenv import load_dotenv
import snowflake.connector

# Loan ENV Variables from dot env
load_dotenv()

# Define snowflake connection
def get_snowflake_connection():
    """Returns an active snowflake connection using .env credentials"""
    return snowflake.connector.connect(
        account = os.getenv("SNOWFLAKE_ACCOUNT"),
        user = os.getenv("SNOWFLAKE_USERNAME"),
        password = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        database = os.getenv("SNOWFLAKE_DATABASE"),
        schema = os.getenv("SNOWFLAKE_RAW_SCHEMA")
    )