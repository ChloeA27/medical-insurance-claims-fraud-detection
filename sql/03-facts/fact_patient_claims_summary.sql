CREATE TABLE fact_patient_claims_summary_etl
    WITH (
        format = 'PARQUET',
        external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/fact_tables/fact_patient_summary_v2/'
    ) AS
    SELECT 
        pat.patient_sk,
        DATE_FORMAT(c.claim_start_date, '%Y%m') AS month_key,
        COUNT(*) AS total_claims,
        SUM(CASE WHEN c.claim_type = 'Inpatient' THEN 1 ELSE 0 END) AS inpatient_visits,
        SUM(CASE WHEN c.claim_type = 'Outpatient' THEN 1 ELSE 0 END) AS outpatient_visits,
        SUM(c.claim_amount) AS total_claimed,
        SUM(c.deductible_amount) AS total_deductible,
        SUM(CASE WHEN prov.is_fraudulent = TRUE THEN 1 ELSE 0 END) AS fraudulent_provider_visits,
        SUM(CASE WHEN prov.is_fraudulent = TRUE THEN c.claim_amount ELSE 0 END) AS fraud_exposure_amount
    FROM v_all_claims_etl c
    JOIN dim_patient_etl pat ON c.patient_id = pat.patient_id
    JOIN dim_provider_etl prov ON c.provider_id = prov.provider_id
    GROUP BY 
        pat.patient_sk,
        DATE_FORMAT(c.claim_start_date, '%Y%m')