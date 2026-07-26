WITH source AS (
    SELECT * FROM {{ source('raw_data', 'RAW_PATIENTS') }}
)
SELECT
    patient_id,
    upper(first_name) as first_name,
    upper(last_name) as last_name,
    gender,
    try_cast(dob AS DATE) as birth_date,
    created_at
FROM source