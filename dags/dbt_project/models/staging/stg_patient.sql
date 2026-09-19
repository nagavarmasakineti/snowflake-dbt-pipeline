{{ config(materialized = 'view')}}
WITH patient AS (
    SELECT * FROM {{source('raw_data', 'RAW_PATIENTS')}}
),
source AS (
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
        FROM patient 
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
    WHERE row_num = 1