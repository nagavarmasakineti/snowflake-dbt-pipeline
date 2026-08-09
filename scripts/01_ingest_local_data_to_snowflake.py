import os
from snowflake_config import  get_snowflake_connection

# --Configuration--
# Dynamic Folder where we drop raw data
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_DATA_DIR = os.path.join(BASE_DIR, "data_drop")

# Validate the files before preocessing
def validate_file_level(file_path):
    """Perform light file level check"""
    filename = os.path.basename(file_path)

    # Check 1: Check if file is completely empty
    if os.path.getsize(file_path) == 0:
        print(f"skipping file {filename} as no data exists")
        return False
    return True 

# Business logic to copy the data to snoflake staging
def stage_clean_files():
    # 1. Create data drop directory if not exists
    if not os.path.exists(LOCAL_DATA_DIR):
        os.makedirs(LOCAL_DATA_DIR)
        print(f"Created missing directory {LOCAL_DATA_DIR}")
        print(f"Please place the files in the newly created directory")
        return

    csv_files = [f for f in os.listdir(LOCAL_DATA_DIR) if f.endswith('.csv')]

    if not csv_files:
        print('No CSV files found in data_drop directory.')
        return

    # Reuse Snowflake Connection
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        for filename in csv_files:
            file_path = os.path.join(LOCAL_DATA_DIR, filename)
            formatted_path = file_path.replace("\\", "/")

            print(f"\n---Processing: {filename} to snowflake stage...")

            if not validate_file_level(file_path):
                continue

            # Determine staging subfolder to place the records
            subfolder = "patients" if "patient" in filename.lower() else "visits"

            try:
                # Dump file to snoflake stage
                print(f"1. Executing PUT to @MY_RAW_STAGE/{subfolder}/...")
                cur.execute(f"""
                                PUT file://{formatted_path} @MY_RAW_STAGE/{subfolder}/
                                AUTO_COMPRESS = TRUE
                                OVERWRITE = TRUE;
                            """)
                print(f"2. Successfully Staged {filename}")

                # Remove the staged file from the directory
                os.remove(file_path)
                print(f"3. Cleared source file from local directory filename: {filename}")

            except Exception as e:
             print('Failed to stage {filename}: {e} ')

    except Exception as e:
        print(f"error {e}")

    finally:
        cur.close()
        conn.close()

#Call this function
if __name__ == "__main__":
    stage_clean_files()




