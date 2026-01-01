-- v_inpatient_claims_etl

CREATE OR REPLACE VIEW v_inpatient_claims_etl AS
SELECT
    BeneID AS patient_id,
    ClaimID AS claim_id,
    Provider AS provider_id,
    TRY_CAST(ClaimStartDt AS DATE) AS claim_start_date,
    TRY_CAST(ClaimEndDt AS DATE) AS claim_end_date,
    TRY_CAST(AdmissionDt AS DATE) AS admission_date,
    TRY_CAST(DischargeDt AS DATE) AS discharge_date,
    DATE_DIFF('day', TRY_CAST(AdmissionDt AS DATE), TRY_CAST(DischargeDt AS DATE)) AS length_of_stay,
    TRY_CAST(InscClaimAmtReimbursed AS DOUBLE) AS claim_amount,
    TRY_CAST(DeductibleAmtPaid AS DOUBLE) AS deductible_amount,
    AttendingPhysician AS attending_physician_id,
    OperatingPhysician AS operating_physician_id,
    OtherPhysician AS other_physician_id,
    ClmAdmitDiagnosisCode AS admit_diagnosis_code,
    DiagnosisGroupCode AS diagnosis_group_code,
    ClmDiagnosisCode_1 AS diagnosis_code_1,
    ClmDiagnosisCode_2 AS diagnosis_code_2,
    ClmDiagnosisCode_3 AS diagnosis_code_3,
    ClmDiagnosisCode_4 AS diagnosis_code_4,
    ClmDiagnosisCode_5 AS diagnosis_code_5,
    ClmDiagnosisCode_6 AS diagnosis_code_6,
    ClmDiagnosisCode_7 AS diagnosis_code_7,
    ClmDiagnosisCode_8 AS diagnosis_code_8,
    ClmDiagnosisCode_9 AS diagnosis_code_9,
    ClmDiagnosisCode_10 AS diagnosis_code_10,
    ClmProcedureCode_1 AS procedure_code_1,
    ClmProcedureCode_2 AS procedure_code_2,
    ClmProcedureCode_3 AS procedure_code_3,
    ClmProcedureCode_4 AS procedure_code_4,
    ClmProcedureCode_5 AS procedure_code_5,
    ClmProcedureCode_6 AS procedure_code_6,
    'Inpatient' AS claim_type
FROM inpatient
WHERE ClaimID IS NOT NULL