-- v_patients_etl

CREATE OR REPLACE VIEW v_patients_etl AS
SELECT
    BeneID AS patient_id,
    CASE WHEN DOB = 'NA' THEN NULL ELSE TRY_CAST(DOB AS DATE) END AS date_of_birth,
    CASE WHEN DOD = 'NA' THEN NULL ELSE TRY_CAST(DOD AS DATE) END AS date_of_death,
    CASE 
    WHEN Gender = '1' THEN 'Male' 
    WHEN Gender = '2' THEN 'Female' 
    END AS gender,
    CASE 
    WHEN Race = '1' THEN 'White'
    WHEN Race = '2' THEN 'Black'
    WHEN Race = '3' THEN 'Asian'
    WHEN Race = '4' THEN 'Hispanic'
    WHEN Race = '5' THEN 'Other'
    END AS race,
    CASE 
    WHEN RenalDiseaseIndicator = 'Y' THEN true
    WHEN RenalDiseaseIndicator = '1' THEN true
    ELSE false 
    END AS has_renal_disease,
    TRY_CAST(State AS INT) AS state_code,
    TRY_CAST(County AS INT) AS county_code,
    TRY_CAST(NoOfMonths_PartACov AS INT) AS months_part_a,
    TRY_CAST(NoOfMonths_PartBCov AS INT) AS months_part_b,
    CASE WHEN ChronicCond_Alzheimer = '1' THEN true ELSE false END AS has_alzheimer,
    CASE WHEN ChronicCond_Heartfailure = '1' THEN true ELSE false END AS has_heart_failure,
    CASE WHEN ChronicCond_KidneyDisease = '1' THEN true ELSE false END AS has_kidney_disease,
    CASE WHEN ChronicCond_Cancer = '1' THEN true ELSE false END AS has_cancer,
    CASE WHEN ChronicCond_ObstrPulmonary = '1' THEN true ELSE false END AS has_copd,
    CASE WHEN ChronicCond_Depression = '1' THEN true ELSE false END AS has_depression,
    CASE WHEN ChronicCond_Diabetes = '1' THEN true ELSE false END AS has_diabetes,
    CASE WHEN ChronicCond_IschemicHeart = '1' THEN true ELSE false END AS has_ischemic_heart,
    CASE WHEN ChronicCond_Osteoporasis = '1' THEN true ELSE false END AS has_osteoporosis,
    CASE WHEN ChronicCond_rheumatoidarthritis = '1' THEN true ELSE false END AS has_arthritis,
    CASE WHEN ChronicCond_stroke = '1' THEN true ELSE false END AS has_stroke,
    (
    (CASE WHEN ChronicCond_Alzheimer = '1' THEN 1 ELSE 0 END) +
    (CASE WHEN ChronicCond_Heartfailure = '1' THEN 1 ELSE 0 END) +
    (CASE WHEN ChronicCond_KidneyDisease = '1' THEN 1 ELSE 0 END) +
    (CASE WHEN ChronicCond_Cancer = '1' THEN 1 ELSE 0 END) +
    (CASE WHEN ChronicCond_ObstrPulmonary = '1' THEN 1 ELSE 0 END) +
    (CASE WHEN ChronicCond_Depression = '1' THEN 1 ELSE 0 END) +
    (CASE WHEN ChronicCond_Diabetes = '1' THEN 1 ELSE 0 END) +
    (CASE WHEN ChronicCond_IschemicHeart = '1' THEN 1 ELSE 0 END) +
    (CASE WHEN ChronicCond_Osteoporasis = '1' THEN 1 ELSE 0 END) +
    (CASE WHEN ChronicCond_rheumatoidarthritis = '1' THEN 1 ELSE 0 END) +
    (CASE WHEN ChronicCond_stroke = '1' THEN 1 ELSE 0 END)
    ) AS chronic_condition_count,
    TRY_CAST(IPAnnualReimbursementAmt AS DOUBLE) AS ip_annual_reimbursement,
    TRY_CAST(IPAnnualDeductibleAmt AS DOUBLE) AS ip_annual_deductible,
    TRY_CAST(OPAnnualReimbursementAmt AS DOUBLE) AS op_annual_reimbursement,
    TRY_CAST(OPAnnualDeductibleAmt AS DOUBLE) AS op_annual_deductible,
    CASE 
    WHEN (DOD = 'NA' OR DOD IS NULL OR DOD = '') THEN false 
    ELSE true 
    END AS is_deceased
FROM beneficiary
WHERE BeneID IS NOT NULL
