import os
import csv
import shutil
from snowflake_config import  get_snowflake_connection

# --Configuration--
# Dynamic Folder where we drop raw data
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_DATA_DIR = os.path.join(BASE_DIR, "data_drop")
FAILED_DATA_DIR = os.path.join(BASE_DIR, "data_processing_failed")

#Expected Headers Definition:
EXPECTED_PATIENT_HEADERS = ["patient_id", "first_name", "last_name", "gender", "dob"]
EXPECTED_VISIT_HEADERS = ["visit_id", "patient_id", "visit_date", "diagnostic_code","total_cost"]


def move_to_failed_dir(file_path):
    filename = os.path.basename(file_path)
    dest_path = os.path.join(FAILED_DATA_DIR, filename)

    # Create data_processing_failed directory if it is missing
    if not os.path.exists(FAILED_DATA_DIR):
        os.makedirs(FAILED_DATA_DIR)
        print(f"Created missing directory {FAILED_DATA_DIR}")

    shutil.move(file_path, dest_path)
    print(f" --> [MOVED] {filename} -->{FAILED_DATA_DIR}")



def validate_files_directory():
    # Create data drop directory if not exists
    if not os.path.exists(LOCAL_DATA_DIR):
        os.makedirs(LOCAL_DATA_DIR)
        print(f"Created missing directory {LOCAL_DATA_DIR}")
        print(f"Please place the files in the newly created directory")
        return False
    return True



# Validate the files before preocessing
def validate_stage_files():
    """Validate and Stage Files"""
    try:
        #Check if data_drop directory exists
        if not validate_files_directory():
            print(f"--> [Failed] data_drop directory is missing and it was created. Please place new files there.")
            return

        csv_files = [f for f in os.listdir(LOCAL_DATA_DIR) if f.endswith('.csv')]
        if not csv_files:
            print(f"No CSV Files in Data Drop Directory")
            return

        for file_name in csv_files:
            file_path = os.path.join(LOCAL_DATA_DIR, file_name)
            file_name_lower = file_name.lower()

            # 1.Check file name has patient or visit
            if "patient" not in file_name_lower and "visit" not in file_name_lower:
                print(f"--> [Failed] Invalid file name pattern:{file_name}")
                move_to_failed_dir(file_path)
                continue

            # 2. Check if file has data > 0 bytes
            if os.path.getsize(file_path) == 0:
                print(f"[FAILED]: {file_name} has no data.")
                move_to_failed_dir(file_path)
                continue

            #3.Check if files has required headers and atleast one column of data
            try:
                with open(file_path, mode = 'r', encoding='utf-8') as f:
                    reader = list(csv.reader(f))
                    if not reader or len(reader) < 2:
                        print(f"[FAILED]: {file_name} missing headers or data.")
                        move_to_failed_dir(file_path)
                        continue

                    header = [h.strip().lower() for h in reader[0]]
                    expected = [h.lower() for h in 
                                (EXPECTED_PATIENT_HEADERS if "patient" in file_name_lower 
                                 else EXPECTED_VISIT_HEADERS)]
                    if header != expected:
                        print(f"[FAILED]: {file_name} headers are invalid.")
                        move_to_failed_dir(file_path)
                        continue
            except Exception as e:
                print(f"[FAILED]: unable to read {file_name} : {e}")
                move_to_failed_dir(file_path)
                continue


            # Formatted File Path for snowflake PUT
            formatted_path = file_path.replace("\\", "/")
            print(f"\n---Processing: {file_name} to snowflake stage...")

            # Determine staging subfolder to place the records
            subfolder = "patients" if "patient" in file_name_lower else "visits"

            # Initialize connetion variables:
            conn = None
            cur = None
            try:
                # Reuse Snowflake Connection
                conn = get_snowflake_connection()
                cur = conn.cursor()

                # Dump file to snoflake stage
                print(f"1. Executing PUT to @MY_RAW_STAGE/{subfolder}/...")
                cur.execute(f"""
                                PUT file://{formatted_path} @MY_RAW_STAGE/{subfolder}/
                                AUTO_COMPRESS = TRUE
                                OVERWRITE = TRUE;
                            """)
                print(f"2. Successfully Staged {file_name}")

                # Remove the staged file from the directory
                os.remove(file_path)
                print(f"3. Cleared source file from local directory filename: {file_name}")

            except Exception as e:
             print(f'Failed to stage {file_name}: {e} ')

            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()

    except Exception as e:
        print(f"error {e}")

#Call this function
if __name__ == "__main__":
    validate_stage_files()




