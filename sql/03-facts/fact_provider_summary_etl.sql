CREATE TABLE fact_provider_summary_etl
    WITH (
        format = 'PARQUET',
        external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/fact_tables/fact_provider_summary_v2/'
    ) AS
    SELECT 
        prov.provider_sk,
        DATE_FORMAT(c.claim_start_date, '%Y%m') AS month_key,
        COUNT(*) AS total_claims,
        SUM(CASE WHEN c.claim_type = 'Inpatient' THEN 1 ELSE 0 END) AS inpatient_claims,
        SUM(CASE WHEN c.claim_type = 'Outpatient' THEN 1 ELSE 0 END) AS outpatient_claims,
        COUNT(DISTINCT c.patient_id) AS unique_patients,
        SUM(c.claim_amount) AS total_claimed,
        AVG(c.claim_amount) AS avg_claim_amount,
        prov.is_fraudulent AS provider_is_fraudulent,
        SUM(CASE WHEN prov.is_fraudulent = TRUE THEN c.claim_amount ELSE 0 END) AS fraud_exposure_amount
    FROM v_all_claims_etl c
    JOIN dim_provider_etl prov ON c.provider_id = prov.provider_id
    GROUP BY 
        prov.provider_sk,
        prov.is_fraudulent,
        DATE_FORMAT(c.claim_start_date, '%Y%m')