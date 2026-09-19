SHOW WAREHOUSES;
USE WAREHOUSE TRANSFORM_WH;

SHOW ROLES;
USE ROLE TRANSFORM_ROLE;

USE DATABASE HEALTHCARE_ANALYTICS;
USE SCHEMA RAW;

SHOW STAGES;
LIST @MY_RAW_STAGE;

SELECT $1, $2, $3, $4, $5, $6 FROM @MY_RAW_STAGE;

--Check Number of files in external stage
LIST @MY_RAW_STAGE;
SELECT COUNT(*) AS totalFileCount FROM TABLE(result_scan(last_query_id()));

--RAW SCHEMA patients and visits table
CREATE TABLE IF NOT EXISTS healthcare_analytics.raw.raw_patients(
    patient_id varchar,
    first_name varchar,
    last_name varchar,
    gender varchar,
    dob date,
    created_at timestamp,
    loaded_at_utc timestamp_ntz default current_timestamp()
);

--DATA
INSERT INTO healthcare_analytics.raw.raw_patients 
(patient_id, first_name, last_name, gender, dob, created_at)
VALUES ('P101', 'john', 'smith', 'male', current_date(), current_timestamp());

CREATE TABLE IF NOT EXISTS healthcare_analytics.raw.raw_visit(
    visit_id varchar,
    patient_id varchar,
    visit_date date,
    diagnosis_code varchar,
    total_amount number,
    loaded_at_utc timestamp_ntz default current_timestamp()
);

DESCRIBE STAGE HEALTHCARE_ANALYTICS.RAW.MY_RAW_STAGE;
LIST @MY_RAW_STAGE;
ALTER STAGE HEALTHCARE_ANALYTICS.RAW.MY_RAW_STAGE REFRESH;
SELECT * FROM DIRECTORY(@HEALTHCARE_ANALYTICS.RAW.MY_RAW_STAGE);
SELECT SYSTEM$STREAM_HAS_DATA('HEALTHCARE_ANALYTICS.RAW.RAW_STAGE_FILE_STREAM');

COPY INTO healthcare_analytics.raw.raw_patient (
    patient_id, first_name, last_name, gender, dob, created_at
)
FROM (
    SELECT $1, $2, $3, $4, $5, CURRENT_TIMESTAMP() FROM @MY_RAW_STAGE/patients
) 
FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER=1);

SELECT * FROM RAW_PATIENTS;

COPY INTO healthcare_analytics.raw.raw_visit (
    visit_id, patient_id, visit_date, diagnosis_code, total_amount
)
FROM (
    SELECT $1, $2, $3, $4, $5 FROM @MY_RAW_STAGE/visits
) 
FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER=1);

select * from RAW_VISIT;

--SILVER Stage Setup
CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.STG.STG_PATIENT AS
    WITH source AS (
        SELECT
        trim(patient_id::varchar) as patient_id,
        initcap(trim(first_name::varchar)) as first_name,
        initcap(trim(last_name::varchar)) as last_name,
        CASE
        WHEN upper(TRIM(gender::varchar)) IN ('M', 'MALE') THEN 'MALE'
        WHEN upper(TRIM(gender::varchar)) IN ('F', 'FEMALE') THEN 'FEMALE'
        ELSE 'OTHER'
        END as gender,
        dob::date as dob,
        created_at::timestamp as created_at,
        loaded_at_utc as ingested_at
        FROM HEALTHCARE_ANALYTICS.RAW.RAW_PATIENT 
        WHERE patient_id IS NOT NULL
        AND trim(patient_id) !='' 
        AND dob <= CURRENT_DATE()
    ),
    deduped AS (
        SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id
            ORDER BY ingested_at desc
        ) AS row_num
        FROM source
    )
    SELECT
        patient_id,
        first_name,
        last_name,
        gender,
        dob,
        created_at,
        ingested_at
    FROM deduped
    WHERE row_num = 1;
    
select * from healthcare_analytics.stg.stg_patient;


--GOLD Layer
CREATE OR REPLACE TABLE HEALTHCARE_ANALYTICS.ANALYTICS.FACT_PATIENT AS
SELECT
    patient_id as patient_id,
    first_name || ' ' || last_name as full_name,
    gender,
    dob,
    DATEDIFF('year', dob, current_date()) as age,
    CASE
    WHEN DATEDIFF('year', dob, current_date()) < 18 THEN 'Peidatric(0-17)'
    WHEN DATEDIFF('year', dob, current_date()) BETWEEN 18 AND 64 THEN 'Adult(18-64)'
    ELSE 'Senior (65+)'
    END as age_group,
    created_at,
    ingested_at
    FROM HEALTHCARE_ANALYTICS.STG.STG_PATIENT;

 SELECT * FROM HEALTHCARE_ANALYTICS.ANALYTICS.FACT_PATIENT;