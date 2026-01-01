-- sql/03-facts/fact_claims_etl.sql
--
-- TABLE: fact_claims_etl
-- GRAIN: One row per claim (inpatient + outpatient combined)
-- PURPOSE: Central fact table for claims analysis
-- RECORDS: ~558K claims
-- STORAGE: Parquet format in S3
--

CREATE TABLE fact_claims_etl
WITH (
    format = 'PARQUET',
    external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/fact_tables/fact_claims/'
) AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY c.claim_id) AS claim_sk,
    pat.patient_sk,
    prov.provider_sk,
    COALESCE(CAST(DATE_FORMAT(c.claim_start_date, '%Y%m%d') AS INT), 0) AS claim_start_date_key,
    COALESCE(CAST(DATE_FORMAT(c.claim_end_date, '%Y%m%d') AS INT), 0) AS claim_end_date_key,
    COALESCE(CAST(DATE_FORMAT(c.admission_date, '%Y%m%d') AS INT), 0) AS admission_date_key,
    COALESCE(CAST(DATE_FORMAT(c.discharge_date, '%Y%m%d') AS INT), 0) AS discharge_date_key,
    c.claim_id,
    c.claim_type,
    COALESCE(c.claim_amount, 0) AS claim_amount,
    COALESCE(c.deductible_amount, 0) AS deductible_amount,
    COALESCE(c.length_of_stay, 0) AS length_of_stay,
    COALESCE(c.claim_amount, 0) + COALESCE(c.deductible_amount, 0) AS total_amount,
    prov.is_fraudulent,
    c.admit_diagnosis_code,
    c.diagnosis_group_code,
    c.diagnosis_code_1,
    c.procedure_code_1,
    c.attending_physician_id,
    c.operating_physician_id,
    c.other_physician_id
FROM v_all_claims_etl c
JOIN dim_patient_etl pat ON c.patient_id = pat.patient_id
JOIN dim_provider_etl prov ON c.provider_id = prov.provider_id;