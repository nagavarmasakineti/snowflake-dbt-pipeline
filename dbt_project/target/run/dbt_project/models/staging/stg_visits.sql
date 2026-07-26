
  create or replace   view HEALTHCARE_ANALYTICS.STG.stg_visits
  
  
  
  
  as (
    WITH source AS (
    SELECT * FROM HEALTHCARE_ANALYTICS.RAW.RAW_VISITS
)
SELECT
    visit_id,
    patient_id,
    try_cast(visit_date AS DATE) AS visit_date,
    diagnosis_code,
    total_amount,
    created_at
FROM source
  );

