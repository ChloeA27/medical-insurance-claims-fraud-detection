-- v_all_claims_etl
CREATE OR REPLACE VIEW v_all_claims_etl AS
SELECT
    patient_id,
    claim_id,
    provider_id,
    claim_start_date,
    claim_end_date,
    admission_date,
    discharge_date,
    length_of_stay,
    claim_amount,
    deductible_amount,
    attending_physician_id,
    operating_physician_id,
    other_physician_id,
    admit_diagnosis_code,
    diagnosis_group_code,
    diagnosis_code_1,
    procedure_code_1,
    claim_type
FROM v_inpatient_claims_etl

UNION ALL

SELECT
    patient_id,
    claim_id,
    provider_id,
    claim_start_date,
    claim_end_date,
    NULL AS admission_date,
    NULL AS discharge_date,
    0 AS length_of_stay,
    claim_amount,
    deductible_amount,
    attending_physician_id,
    operating_physician_id,
    other_physician_id,
    admit_diagnosis_code,
    diagnosis_group_code,
    diagnosis_code_1,
    procedure_code_1,
    claim_type
FROM v_outpatient_claims_etl