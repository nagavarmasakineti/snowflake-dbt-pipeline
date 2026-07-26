WITH patient AS (
    SELECT * FROM {{ref('stg_patients')}}
),
visit AS (
    SELECT * FROM {{ref('stg_visits')}}
)
SELECT
    p.patient_id,
    p.first_name || ' ' || p.last_name AS full_name,
    p.gender,
    p.birth_date,
    COUNT(v.visit_id) AS total_visits,
    NVL(SUM(v.total_amount), 0) AS total_spent,
    MAX(v.visit_date) AS latest_visit_date
FROM patient p
LEFT JOIN visit v on p.patient_id = v.patient_id
GROUP BY 1,2,3,4