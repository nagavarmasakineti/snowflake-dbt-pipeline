WITH patient AS (
    SELECT * FROM {{ref('stg_patient')}}
)
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
FROM patient