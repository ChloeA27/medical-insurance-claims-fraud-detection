
# lambda_function.py - ETL Pipeline with step control

import json
import boto3
import time
from datetime import datetime

# ==============================
# AWS CLIENTS & CONFIG
# ==============================

s3_client = boto3.client('s3')
athena_client = boto3.client('athena')

BUCKET = 'insurance-claim-qian-2025'
DATABASE = 'insurance_claim_db'

# Athena query results output
OUTPUT_LOCATION = f's3://{BUCKET}/athena_results/'

# ==============================
# HELPER: EXECUTE ATHENA QUERY
# ==============================

def execute_athena_query(query, database=None, label=None):
    """
    Execute a single Athena query and wait for completion.
    Returns dict with status and optional error.
    """
    if label:
        print(f"\n=== Running: {label} ===")
    print("Query:\n", query)

    try:
        params = {
            'QueryString': query,
            'ResultConfiguration': {'OutputLocation': OUTPUT_LOCATION}
        }

        # Only set DB context when not creating/dropping DB itself
        if database and not any(
            keyword in query.upper()
            for keyword in ['CREATE DATABASE', 'DROP DATABASE', 'SHOW DATABASES']
        ):
            params['QueryExecutionContext'] = {'Database': database}

        response = athena_client.start_query_execution(**params)
        query_id = response['QueryExecutionId']

        # Poll for completion
        max_attempts = 150
        for attempt in range(max_attempts):
            result = athena_client.get_query_execution(QueryExecutionId=query_id)
            status = result['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                print("✓ Query succeeded")
                return {'status': 'success', 'query_id': query_id}

            elif status in ['FAILED', 'CANCELLED']:
                error_msg = result['QueryExecution']['Status'].get(
                    'StateChangeReason', 'Unknown error'
                )
                print(f"✗ Query failed: {error_msg}")
                return {'status': 'failed', 'error': error_msg}

            time.sleep(2)

        print("✗ Query timeout")
        return {'status': 'timeout', 'error': 'Query execution timeout'}

    except Exception as e:
        print(f"✗ Query execution error: {str(e)}")
        return {'status': 'error', 'error': str(e)}

# ==============================
# STEP: (OPTIONAL) RAW LAYER
# ==============================

def step_raw():
    """
    Placeholder for raw-layer creation if you ever want Lambda to create
    raw external tables from CSVs. Right now we leave it empty because
    your views read from existing tables: provider, beneficiary, inpatient, outpatient.
    """
    print("\n[STEP raw] No raw-table actions defined (using existing base tables).")
    return []

# ==============================
# STEP: CREATE / REPLACE VIEWS
# ==============================

def step_views():
    created_views = []

    # v_providers_etl
    v_providers = """
    CREATE OR REPLACE VIEW v_providers_etl AS
    SELECT DISTINCT
      Provider AS provider_id,
      CASE 
        WHEN PotentialFraud LIKE '%es%' THEN true  -- matches "Yes" etc.
        WHEN PotentialFraud = 'Y' THEN true
        WHEN PotentialFraud = '1' THEN true
        ELSE false 
      END AS is_fraudulent,
      PotentialFraud AS raw_fraud_value
    FROM provider
    WHERE Provider IS NOT NULL
    """

    # v_patients_etl
    v_patients = """
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
    """

    # v_inpatient_claims_etl
    v_inpatient_claims = """
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
    """

    # v_outpatient_claims_etl
    v_outpatient_claims = """
    CREATE OR REPLACE VIEW v_outpatient_claims_etl AS
    SELECT
      BeneID AS patient_id,
      ClaimID AS claim_id,
      Provider AS provider_id,
      TRY_CAST(ClaimStartDt AS DATE) AS claim_start_date,
      TRY_CAST(ClaimEndDt AS DATE) AS claim_end_date,
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
      'Outpatient' AS claim_type
    FROM outpatient
    WHERE ClaimID IS NOT NULL
    """

    # v_all_claims_etl
    v_all_claims = """
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
    """

    view_queries = {
        "v_providers_etl": v_providers,
        "v_patients_etl": v_patients,
        "v_inpatient_claims_etl": v_inpatient_claims,
        "v_outpatient_claims_etl": v_outpatient_claims,
        "v_all_claims_etl": v_all_claims,
    }

    for name, q in view_queries.items():
        # DROP VIEW IF EXISTS (for safety)
        drop_q = f"DROP VIEW IF EXISTS {name}"
        execute_athena_query(drop_q, DATABASE, label=f"Drop view {name} (if exists)")

        res = execute_athena_query(q, DATABASE, label=f"Create view {name}")
        if res['status'] != 'success':
            raise Exception(f"Failed to create view {name}: {res.get('error')}")
        created_views.append(name)

    return created_views

# ==============================
# STEP: CREATE DIM TABLES
# ==============================

def step_dims():
    created_dims = []

    # 1) dim_date_etl
    execute_athena_query(
        "DROP TABLE IF EXISTS dim_date_etl",
        DATABASE,
        label="Drop table dim_date_etl (if exists)"
    )
    dim_date_sql = """
    CREATE TABLE dim_date_etl
    WITH (
        format = 'PARQUET',
        external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_date/'
    ) AS
    SELECT DISTINCT
        CAST(DATE_FORMAT(date_val, '%Y%m%d') AS INT) AS date_key,
        date_val AS full_date,
        YEAR(date_val) AS year,
        QUARTER(date_val) AS quarter,
        MONTH(date_val) AS month,
        DATE_FORMAT(date_val, '%M') AS month_name,
        DAY(date_val) AS day_of_month,
        DAY_OF_WEEK(date_val) AS day_of_week,
        DATE_FORMAT(date_val, '%W') AS day_name,
        CASE 
            WHEN DAY_OF_WEEK(date_val) IN (6,7) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        DATE_FORMAT(date_val, '%Y-%m') AS year_month
    FROM (
        SELECT claim_start_date AS date_val FROM v_all_claims_etl WHERE claim_start_date IS NOT NULL
        UNION
        SELECT claim_end_date FROM v_all_claims_etl WHERE claim_end_date IS NOT NULL
        UNION
        SELECT admission_date FROM v_all_claims_etl WHERE admission_date IS NOT NULL
        UNION
        SELECT discharge_date FROM v_all_claims_etl WHERE discharge_date IS NOT NULL
    )
    """
    res = execute_athena_query(dim_date_sql, DATABASE, label="Create dim_date_etl")
    if res['status'] != 'success':
        raise Exception(f"Failed to create dim_date_etl: {res.get('error')}")
    created_dims.append("dim_date_etl")

    # 2) dim_provider_etl
    execute_athena_query(
        "DROP TABLE IF EXISTS dim_provider_etl",
        DATABASE,
        label="Drop table dim_provider_etl (if exists)"
    )
    dim_provider_sql = """
    CREATE TABLE dim_provider_etl
    WITH (
        format = 'PARQUET',
        external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_provider/'
    ) AS
    WITH provider_stats AS (
        SELECT 
            provider_id,
            COUNT(DISTINCT patient_id) AS total_patients,
            COUNT(claim_id) AS total_claims,
            SUM(claim_amount) AS total_amount,
            AVG(claim_amount) AS avg_claim_amount,
            MAX(claim_amount) AS max_claim_amount,
            MIN(claim_amount) AS min_claim_amount,
            SUM(CASE WHEN claim_type = 'Inpatient' THEN 1 ELSE 0 END) AS inpatient_claims,
            SUM(CASE WHEN claim_type = 'Outpatient' THEN 1 ELSE 0 END) AS outpatient_claims,
            AVG(CASE WHEN claim_type = 'Inpatient' THEN length_of_stay ELSE NULL END) AS avg_length_of_stay
        FROM v_all_claims_etl
        GROUP BY provider_id
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY p.provider_id) AS provider_sk,
        p.provider_id,
        p.is_fraudulent,
        COALESCE(ps.total_patients, 0) AS total_patients,
        COALESCE(ps.total_claims, 0) AS total_claims,
        COALESCE(ps.total_amount, 0) AS total_amount,
        COALESCE(ps.avg_claim_amount, 0) AS avg_claim_amount,
        COALESCE(ps.max_claim_amount, 0) AS max_claim_amount,
        COALESCE(ps.min_claim_amount, 0) AS min_claim_amount,
        COALESCE(ps.inpatient_claims, 0) AS inpatient_claims,
        COALESCE(ps.outpatient_claims, 0) AS outpatient_claims,
        COALESCE(ps.avg_length_of_stay, 0) AS avg_length_of_stay,
        CASE 
            WHEN ps.inpatient_claims > 0 AND ps.outpatient_claims = 0 THEN 'Hospital Only'
            WHEN ps.inpatient_claims = 0 AND ps.outpatient_claims > 0 THEN 'Clinic Only'
            WHEN ps.inpatient_claims > ps.outpatient_claims THEN 'Primarily Hospital'
            WHEN ps.outpatient_claims > ps.inpatient_claims THEN 'Primarily Clinic'
            ELSE 'Mixed'
        END AS provider_type,
        CASE 
            WHEN p.is_fraudulent = TRUE THEN 'Confirmed Fraud'
            WHEN ps.avg_claim_amount > (
                SELECT AVG(claim_amount) + 2 * STDDEV(claim_amount) 
                FROM v_all_claims_etl
            ) THEN 'High Risk'
            WHEN ps.avg_claim_amount > (
                SELECT AVG(claim_amount) + STDDEV(claim_amount) 
                FROM v_all_claims_etl
            ) THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS risk_level,
        CASE 
            WHEN ps.total_claims > 1000 THEN 'Very High Volume'
            WHEN ps.total_claims > 500 THEN 'High Volume'
            WHEN ps.total_claims > 100 THEN 'Medium Volume'
            WHEN ps.total_claims > 0 THEN 'Low Volume'
            ELSE 'Inactive'
        END AS activity_level
    FROM v_providers_etl p
    LEFT JOIN provider_stats ps ON p.provider_id = ps.provider_id
    WHERE p.provider_id IS NOT NULL
    """
    res = execute_athena_query(dim_provider_sql, DATABASE, label="Create dim_provider_etl")
    if res['status'] != 'success':
        raise Exception(f"Failed to create dim_provider_etl: {res.get('error')}")
    created_dims.append("dim_provider_etl")

    # 3) dim_patient_etl
    execute_athena_query(
        "DROP TABLE IF EXISTS dim_patient_etl",
        DATABASE,
        label="Drop table dim_patient_etl (if exists)"
    )
    dim_patient_sql = """
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
    """
    res = execute_athena_query(dim_patient_sql, DATABASE, label="Create dim_patient_etl")
    if res['status'] != 'success':
        raise Exception(f"Failed to create dim_patient_etl: {res.get('error')}")
    created_dims.append("dim_patient_etl")

    # 4) dim_diagnosis_etl
    execute_athena_query(
        "DROP TABLE IF EXISTS dim_diagnosis_etl",
        DATABASE,
        label="Drop table dim_diagnosis_etl (if exists)"
    )
    dim_diagnosis_sql = """
    CREATE TABLE dim_diagnosis_etl
    WITH (
        format = 'PARQUET',
        external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_diagnosis/'
    ) AS
    WITH all_diagnosis_codes AS (
        SELECT DISTINCT diagnosis_code
        FROM (
            SELECT diagnosis_code_1 AS diagnosis_code FROM v_inpatient_claims_etl WHERE diagnosis_code_1 IS NOT NULL
            UNION SELECT diagnosis_code_2 FROM v_inpatient_claims_etl WHERE diagnosis_code_2 IS NOT NULL
            UNION SELECT diagnosis_code_3 FROM v_inpatient_claims_etl WHERE diagnosis_code_3 IS NOT NULL
            UNION SELECT diagnosis_code_4 FROM v_inpatient_claims_etl WHERE diagnosis_code_4 IS NOT NULL
            UNION SELECT diagnosis_code_5 FROM v_inpatient_claims_etl WHERE diagnosis_code_5 IS NOT NULL
            UNION SELECT diagnosis_code_6 FROM v_inpatient_claims_etl WHERE diagnosis_code_6 IS NOT NULL
            UNION SELECT diagnosis_code_7 FROM v_inpatient_claims_etl WHERE diagnosis_code_7 IS NOT NULL
            UNION SELECT diagnosis_code_8 FROM v_inpatient_claims_etl WHERE diagnosis_code_8 IS NOT NULL
            UNION SELECT diagnosis_code_9 FROM v_inpatient_claims_etl WHERE diagnosis_code_9 IS NOT NULL
            UNION SELECT diagnosis_code_10 FROM v_inpatient_claims_etl WHERE diagnosis_code_10 IS NOT NULL
            UNION SELECT admit_diagnosis_code FROM v_inpatient_claims_etl WHERE admit_diagnosis_code IS NOT NULL

            UNION SELECT diagnosis_code_1 FROM v_outpatient_claims_etl WHERE diagnosis_code_1 IS NOT NULL
            UNION SELECT diagnosis_code_2 FROM v_outpatient_claims_etl WHERE diagnosis_code_2 IS NOT NULL
            UNION SELECT diagnosis_code_3 FROM v_outpatient_claims_etl WHERE diagnosis_code_3 IS NOT NULL
            UNION SELECT diagnosis_code_4 FROM v_outpatient_claims_etl WHERE diagnosis_code_4 IS NOT NULL
            UNION SELECT diagnosis_code_5 FROM v_outpatient_claims_etl WHERE diagnosis_code_5 IS NOT NULL
            UNION SELECT diagnosis_code_6 FROM v_outpatient_claims_etl WHERE diagnosis_code_6 IS NOT NULL
            UNION SELECT diagnosis_code_7 FROM v_outpatient_claims_etl WHERE diagnosis_code_7 IS NOT NULL
            UNION SELECT diagnosis_code_8 FROM v_outpatient_claims_etl WHERE diagnosis_code_8 IS NOT NULL
            UNION SELECT diagnosis_code_9 FROM v_outpatient_claims_etl WHERE diagnosis_code_9 IS NOT NULL
            UNION SELECT diagnosis_code_10 FROM v_outpatient_claims_etl WHERE diagnosis_code_10 IS NOT NULL
            UNION SELECT admit_diagnosis_code FROM v_outpatient_claims_etl WHERE admit_diagnosis_code IS NOT NULL
        )
    ),
    cleaned_codes AS (
        SELECT 
            REPLACE(REPLACE(diagnosis_code, '\"', ''), '''', '') AS diagnosis_code_clean,
            diagnosis_code AS diagnosis_code_original
        FROM all_diagnosis_codes
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY diagnosis_code_clean) AS diagnosis_sk,
        diagnosis_code_clean AS diagnosis_code,
        SUBSTRING(diagnosis_code_clean, 1, 3) AS code_category,
        CASE 
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = 'V' THEN 'Supplementary Classification (V Codes)'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = 'E' THEN 'External Causes of Injury (E Codes)'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '0' THEN 'Infectious and Parasitic Diseases'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '1' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '139' THEN 'Infectious and Parasitic Diseases'
                    ELSE 'Neoplasms'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '2' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '239' THEN 'Neoplasms'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '279' THEN 'Endocrine, Nutritional and Metabolic'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '289' THEN 'Blood and Blood-Forming Organs'
                    ELSE 'Mental Disorders'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '3' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '319' THEN 'Mental Disorders'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '389' THEN 'Nervous System and Sense Organs'
                    ELSE 'Circulatory System'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '4' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '459' THEN 'Circulatory System'
                    ELSE 'Respiratory System'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '5' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '579' THEN 'Digestive System'
                    ELSE 'Genitourinary System'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '6' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '629' THEN 'Genitourinary System'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '679' THEN 'Pregnancy, Childbirth and Puerperium'
                    ELSE 'Skin and Subcutaneous Tissue'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '7' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '709' THEN 'Skin and Subcutaneous Tissue'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '739' THEN 'Musculoskeletal and Connective Tissue'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '759' THEN 'Congenital Anomalies'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '779' THEN 'Perinatal Period Conditions'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '799' THEN 'Symptoms, Signs and Ill-Defined Conditions'
                    ELSE 'Injury and Poisoning'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) IN ('8', '9') THEN 'Injury and Poisoning'
            ELSE 'Other/Unknown'
        END AS icd9_chapter,
        CASE 
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) = '250' THEN 'Diabetes Mellitus'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) IN ('401', '402', '403', '404', '405') THEN 'Hypertensive Disease'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) = '428' THEN 'Heart Failure'
            WHEN diagnosis_code_clean = '4019' THEN 'Essential Hypertension'
            WHEN diagnosis_code_clean IN ('2720', '2724') THEN 'Hyperlipidemia'
            ELSE NULL
        END AS subcategory,
        CASE 
            WHEN diagnosis_code_clean IN ('4019', '25000', '2724', '496', '4280', '42731', '2859') THEN TRUE
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) IN ('250', '401', '402', '403', '404', '405', '428', '496') THEN TRUE
            ELSE FALSE
        END AS is_chronic,
        CASE 
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) IN ('1', '2') AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) >= '140' AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) <= '239' THEN 'High'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) >= '390' AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) <= '459' THEN 'High'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) >= '580' AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) <= '629' THEN 'High'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = 'V' THEN 'Low'
            ELSE 'Standard'
        END AS risk_level,
        CASE 
            WHEN diagnosis_code_clean IN ('4019', '2724', '25000', '4280', '2859', '496') THEN TRUE
            ELSE FALSE
        END AS is_common_code,
        CASE 
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) IN ('1', '2') AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) >= '140' AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) <= '239' THEN 'Very High'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) IN ('8', '9') THEN 'High'
            ELSE 'Standard'
        END AS cost_category
    FROM cleaned_codes
    WHERE diagnosis_code_clean IS NOT NULL 
      AND diagnosis_code_clean != ''
      AND diagnosis_code_clean != 'NA'
    """
    res = execute_athena_query(dim_diagnosis_sql, DATABASE, label="Create dim_diagnosis_etl")
    if res['status'] != 'success':
        raise Exception(f"Failed to create dim_diagnosis_etl: {res.get('error')}")
    created_dims.append("dim_diagnosis_etl")

    # 5) dim_procedure_etl
    execute_athena_query(
        "DROP TABLE IF EXISTS dim_procedure_etl",
        DATABASE,
        label="Drop table dim_procedure_etl (if exists)"
    )
    dim_procedure_sql = """
    CREATE TABLE dim_procedure_etl
    WITH (
        format = 'PARQUET',
        external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_procedure/'
    ) AS
    WITH all_procedure_codes AS (
        SELECT DISTINCT procedure_code
        FROM (
            SELECT procedure_code_1 AS procedure_code FROM v_inpatient_claims_etl 
            WHERE procedure_code_1 IS NOT NULL AND procedure_code_1 != '' AND procedure_code_1 != 'NA'
            UNION SELECT procedure_code_2 FROM v_inpatient_claims_etl 
            WHERE procedure_code_2 IS NOT NULL AND procedure_code_2 != '' AND procedure_code_2 != 'NA'
            UNION SELECT procedure_code_3 FROM v_inpatient_claims_etl 
            WHERE procedure_code_3 IS NOT NULL AND procedure_code_3 != '' AND procedure_code_3 != 'NA'
            UNION SELECT procedure_code_4 FROM v_inpatient_claims_etl 
            WHERE procedure_code_4 IS NOT NULL AND procedure_code_4 != '' AND procedure_code_4 != 'NA'
            UNION SELECT procedure_code_5 FROM v_inpatient_claims_etl 
            WHERE procedure_code_5 IS NOT NULL AND procedure_code_5 != '' AND procedure_code_5 != 'NA'
            UNION SELECT procedure_code_6 FROM v_inpatient_claims_etl 
            WHERE procedure_code_6 IS NOT NULL AND procedure_code_6 != '' AND procedure_code_6 != 'NA'
            UNION SELECT procedure_code_1 FROM v_outpatient_claims_etl 
            WHERE procedure_code_1 IS NOT NULL AND procedure_code_1 != '' AND procedure_code_1 != 'NA'
            UNION SELECT procedure_code_2 FROM v_outpatient_claims_etl 
            WHERE procedure_code_2 IS NOT NULL AND procedure_code_2 != '' AND procedure_code_2 != 'NA'
            UNION SELECT procedure_code_3 FROM v_outpatient_claims_etl 
            WHERE procedure_code_3 IS NOT NULL AND procedure_code_3 != '' AND procedure_code_3 != 'NA'
        )
    ),
    cleaned_codes AS (
        SELECT 
            REPLACE(REPLACE(REPLACE(procedure_code, '\"', ''), '''', ''), ' ', '') AS procedure_code_clean
        FROM all_procedure_codes
        WHERE procedure_code IS NOT NULL
    ),
    validated_codes AS (
        SELECT DISTINCT procedure_code_clean
        FROM cleaned_codes
        WHERE 
            procedure_code_clean NOT LIKE 'V%%'
            AND procedure_code_clean NOT LIKE 'E%%'
            AND LENGTH(procedure_code_clean) <= 4
            AND procedure_code_clean NOT IN ('0', '00', '000', '0000')
            AND TRY_CAST(procedure_code_clean AS INT) IS NOT NULL
            AND TRY_CAST(procedure_code_clean AS INT) > 0
            AND TRY_CAST(procedure_code_clean AS INT) <= 9999
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY procedure_code_clean) AS procedure_sk,
        procedure_code_clean AS procedure_code,
        SUBSTRING(procedure_code_clean, 1, 2) AS code_category,
        'ICD-9 Procedure' AS code_type,
        CASE 
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 5 THEN 'Operations on Nervous System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 7 THEN 'Operations on Endocrine System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 16 THEN 'Operations on Eye'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 20 THEN 'Operations on Ear'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 29 THEN 'Operations on Nose, Mouth, and Pharynx'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 34 THEN 'Operations on Respiratory System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 39 THEN 'Operations on Cardiovascular System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 41 THEN 'Operations on Hemic and Lymphatic System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 54 THEN 'Operations on Digestive System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 59 THEN 'Operations on Urinary System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 64 THEN 'Operations on Male Genital Organs'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 71 THEN 'Operations on Female Genital Organs'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 75 THEN 'Obstetrical Procedures'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 84 THEN 'Operations on Musculoskeletal System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 86 THEN 'Operations on Integumentary System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) >= 87 THEN 'Miscellaneous Diagnostic and Therapeutic Procedures'
            ELSE 'Other/Unknown'
        END AS procedure_category,
        CASE 
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 35 AND 39 THEN TRUE
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 1 AND 5 THEN TRUE
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 60 AND 71 THEN TRUE
            ELSE FALSE
        END AS is_major_procedure,
        CASE 
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 1 AND 5 THEN 'High'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 35 AND 39 THEN 'High'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) >= 87 THEN 'Low'
            ELSE 'Medium'
        END AS risk_level,
        CASE 
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 35 AND 39 THEN 'Very High'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 1 AND 5 THEN 'Very High'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 76 AND 84 THEN 'High'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) >= 87 THEN 'Low'
            ELSE 'Medium'
        END AS cost_category
    FROM validated_codes
    ORDER BY procedure_code_clean
    """
    res = execute_athena_query(dim_procedure_sql, DATABASE, label="Create dim_procedure_etl")
    if res['status'] != 'success':
        raise Exception(f"Failed to create dim_procedure_etl: {res.get('error')}")
    created_dims.append("dim_procedure_etl")

    return created_dims

# ==============================
# STEP: CREATE FACT TABLES
# ==============================

def step_facts():
    created_facts = []

    # 1) fact_claims_etl
    execute_athena_query(
        "DROP TABLE IF EXISTS fact_claims_etl",
        DATABASE,
        label="Drop table fact_claims_etl (if exists)"
    )
    fact_claims_sql = """
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
    JOIN dim_provider_etl prov ON c.provider_id = prov.provider_id
    """
    res = execute_athena_query(fact_claims_sql, DATABASE, label="Create fact_claims_etl")
    if res['status'] != 'success':
        raise Exception(f"Failed to create fact_claims_etl: {res.get('error')}")
    created_facts.append("fact_claims_etl")

    # 2) fact_provider_summary_etl
    execute_athena_query(
        "DROP TABLE IF EXISTS fact_provider_summary_etl",
        DATABASE,
        label="Drop table fact_provider_summary_etl (if exists)"
    )
    fact_provider_summary_sql = """
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
    """
    res = execute_athena_query(
        fact_provider_summary_sql,
        DATABASE,
        label="Create fact_provider_summary_etl"
    )
    if res['status'] != 'success':
        raise Exception(f"Failed to create fact_provider_summary_etl: {res.get('error')}")
    created_facts.append("fact_provider_summary_etl")

    # 3) fact_patient_claims_summary_etl
    execute_athena_query(
        "DROP TABLE IF EXISTS fact_patient_claims_summary_etl",
        DATABASE,
        label="Drop table fact_patient_claims_summary_etl (if exists)"
    )
    fact_patient_summary_sql = """
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
    """
    res = execute_athena_query(
        fact_patient_summary_sql,
        DATABASE,
        label="Create fact_patient_claims_summary_etl"
    )
    if res['status'] != 'success':
        raise Exception(f"Failed to create fact_patient_claims_summary_etl: {res.get('error')}")
    created_facts.append("fact_patient_claims_summary_etl")

    return created_facts

# ==============================
# STEP: VALIDATION
# ==============================

def step_validate():
    queries = [
        "SELECT 'dim_date_etl' AS table_name, COUNT(*) AS cnt FROM dim_date_etl",
        "SELECT 'dim_provider_etl' AS table_name, COUNT(*) AS cnt FROM dim_provider_etl",
        "SELECT 'dim_patient_etl' AS table_name, COUNT(*) AS cnt FROM dim_patient_etl",
        "SELECT 'dim_diagnosis_etl' AS table_name, COUNT(*) AS cnt FROM dim_diagnosis_etl",
        "SELECT 'dim_procedure_etl' AS table_name, COUNT(*) AS cnt FROM dim_procedure_etl",
        "SELECT 'fact_claims_etl' AS table_name, COUNT(*) AS cnt FROM fact_claims_etl",
        "SELECT 'fact_provider_summary_etl' AS table_name, COUNT(*) AS cnt FROM fact_provider_summary_etl",
        "SELECT 'fact_patient_claims_summary_etl' AS table_name, COUNT(*) AS cnt FROM fact_patient_claims_summary_etl",
    ]

    for q in queries:
        execute_athena_query(q, DATABASE, label="Validation count")

    return True

# ==============================
# MAIN LAMBDA HANDLER
# ==============================

def lambda_handler(event, context):
    """
    step values:
      - "raw"       : (currently placeholder)
      - "views"     : create / replace all *_etl views
      - "dims"      : build all dim_*_etl tables
      - "facts"     : build all fact_*_etl tables
      - "validate"  : run simple COUNT(*) checks
      - "all" (default): run raw -> views -> dims -> facts -> validate
    """
    print("=" * 80)
    print("Medical Claims ETL Pipeline (Lambda / Athena)")
    print(f"Started at: {datetime.now()}")
    print("=" * 80)

    step = (event or {}).get("step", "all")
    print(f"Requested step: {step}")

    result = {
        "statusCode": 200,
        "step": step,
        "timestamp": str(datetime.now()),
        "views_created": [],
        "dims_created": [],
        "facts_created": []
    }

    try:
        if step in ["all", "raw"]:
            result["raw_info"] = step_raw()

        if step in ["all", "views"]:
            result["views_created"] = step_views()

        if step in ["all", "dims"]:
            result["dims_created"] = step_dims()

        if step in ["all", "facts"]:
            result["facts_created"] = step_facts()

        if step in ["all", "validate"]:
            step_validate()

        print("\nETL pipeline completed successfully.")
        return result

    except Exception as e:
        print(f"\n❌ Pipeline error: {str(e)}")
        result["statusCode"] = 500
        result["error"] = str(e)
        return result
