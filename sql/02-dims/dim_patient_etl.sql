CREATE TABLE dim_patient_etl
    WITH (
        format = 'PARQUET',
        external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_patient_clean/'
    ) AS
    SELECT 
        ROW_NUMBER() OVER (ORDER BY patient_id) AS patient_sk,
        patient_id,
        date_of_birth,
        date_of_death,
        gender,
        race,
        state_code,
        county_code,
        has_renal_disease,
        chronic_condition_count,
        is_deceased,
        CASE 
            WHEN chronic_condition_count >= 5 THEN 'Very High'
            WHEN chronic_condition_count >= 3 THEN 'High'
            WHEN chronic_condition_count >= 1 THEN 'Medium'
            ELSE 'Low'
        END AS risk_category,
        has_alzheimer,
        has_heart_failure,
        has_kidney_disease,
        has_cancer,
        has_copd,
        has_depression,
        has_diabetes,
        has_ischemic_heart,
        has_osteoporosis,
        has_arthritis,
        has_stroke
    FROM v_patients_etl