
    
    

select
    patient_id as unique_field,
    count(*) as n_records

from HEALTHCARE_ANALYTICS.STG.fct_patient_summary
where patient_id is not null
group by patient_id
having count(*) > 1


