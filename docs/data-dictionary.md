# 📊 Data Dictionary - Medical Insurance Claims ETL

## Table of Contents
1. [Overview](#overview)
2. [Dimension Tables](#dimension-tables)
3. [Fact Tables](#fact-tables)
4. [Views](#views)
5. [Key Relationships](#key-relationships)
6. [Business Logic](#business-logic)
7. [Data Quality Notes](#data-quality-notes)
8. [Example Queries](#example-queries)

---

## Overview

### Data Warehouse Structure
This data warehouse follows **Kimball dimensional modeling** with a star schema:
- **5 Dimension Tables**: Provide context and attributes
- **3 Fact Tables**: Contain measurable events and metrics
- **5 Views**: Transform raw operational data to cleansed state

### Total Records
- **Dimension Records**: ~146K (date + provider + patient + diagnosis + procedure)
- **Fact Records**: ~558K claims + aggregations
- **Time Period**: 2008-2010 (3 years)

### Storage Format
All tables stored as **Parquet** files in S3 for optimal query performance and compression.

---

# DIMENSION TABLES

---

## DIM_DATE_ETL

**Grain:** One row per unique date in claims data  
**Record Count:** ~1,090 dates  
**Source:** Derived from claim dates (union of start, end, admission, discharge)  
**Storage:** `s3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_date/`  
**Primary Key:** `date_key`

### Column Definitions

| Column | Data Type | Description | Source | Business Logic |
|--------|-----------|-------------|--------|-----------------|
| **date_key** | INT | Date in YYYYMMDD format (e.g., 20091215) | Derived | `DATE_FORMAT(date_val, '%Y%m%d')` cast to INT |
| **full_date** | DATE | Calendar date | Direct | Date from claim records |
| **year** | INT | Calendar year (2008, 2009, 2010) | Derived | `YEAR(date_val)` |
| **quarter** | INT | Quarter (1-4) | Derived | `QUARTER(date_val)` |
| **month** | INT | Month (1-12) | Derived | `MONTH(date_val)` |
| **month_name** | VARCHAR | Month name (January, February, etc.) | Derived | `DATE_FORMAT(date_val, '%M')` |
| **day_of_month** | INT | Day of month (1-31) | Derived | `DAY(date_val)` |
| **day_of_week** | INT | Day of week (1=Sunday, 7=Saturday) | Derived | `DAY_OF_WEEK(date_val)` |
| **day_name** | VARCHAR | Day name (Monday, Tuesday, etc.) | Derived | `DATE_FORMAT(date_val, '%W')` |
| **is_weekend** | BOOLEAN | TRUE if Saturday or Sunday | Derived | `day_of_week IN (6,7)` |
| **year_month** | VARCHAR | YYYY-MM format for grouping | Derived | `DATE_FORMAT(date_val, '%Y-%m')` |

### Sample Data
```
date_key | full_date  | year | month | month_name | day_name   | is_weekend
---------|------------|------|-------|------------|------------|----------
20080101 | 2008-01-01 | 2008 | 1     | January    | Tuesday    | FALSE
20081225 | 2008-12-25 | 2008 | 12    | December   | Thursday   | FALSE
20090704 | 2009-07-04 | 2009 | 7     | July       | Saturday   | TRUE
```

### Use Cases
- Filtering claims by date range or fiscal period
- Time-based aggregations (monthly, quarterly, yearly trends)
- Identifying seasonal patterns in fraud
- Joining with fact tables for temporal analysis

---

## DIM_PROVIDER_ETL

**Grain:** One row per provider  
**Record Count:** 5,410 providers  
**Source:** `provider` table from operational data  
**Storage:** `s3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_provider/`  
**Primary Key:** `provider_sk` (Surrogate Key)  
**Natural Key:** `provider_id`

### Column Definitions

| Column | Data Type | Description | Source | Business Logic |
|--------|-----------|-------------|--------|-----------------|
| **provider_sk** | INT | Surrogate key (auto-generated) | Generated | `ROW_NUMBER() OVER (ORDER BY provider_id)` |
| **provider_id** | VARCHAR | Provider identifier | Operational | Direct from provider table |
| **is_fraudulent** | BOOLEAN | Known fraud flag | Operational | `PotentialFraud` mapped to boolean |
| **total_patients** | INT | Unique patients served | Derived | `COUNT(DISTINCT patient_id)` across all claims |
| **total_claims** | INT | Total claims submitted | Derived | `COUNT(*)` of all claims for provider |
| **total_amount** | DECIMAL(15,2) | Total amount claimed ($) | Derived | `SUM(claim_amount)` for provider |
| **avg_claim_amount** | DECIMAL(12,2) | Average claim amount ($) | Derived | `AVG(claim_amount)` for provider |
| **max_claim_amount** | DECIMAL(12,2) | Largest single claim ($) | Derived | `MAX(claim_amount)` for provider |
| **min_claim_amount** | DECIMAL(12,2) | Smallest single claim ($) | Derived | `MIN(claim_amount)` for provider |
| **inpatient_claims** | INT | Count of inpatient claims | Derived | `COUNT(*) WHERE claim_type = 'Inpatient'` |
| **outpatient_claims** | INT | Count of outpatient claims | Derived | `COUNT(*) WHERE claim_type = 'Outpatient'` |
| **avg_length_of_stay** | DECIMAL(5,2) | Average LOS for inpatient (days) | Derived | `AVG(length_of_stay) WHERE claim_type = 'Inpatient'` |
| **provider_type** | VARCHAR | Classification: 'Hospital Only' \| 'Clinic Only' \| 'Primarily Hospital' \| 'Primarily Clinic' \| 'Mixed' | Derived | Based on inpatient vs outpatient claim ratios |
| **risk_level** | VARCHAR | Risk tier: 'Confirmed Fraud' \| 'High Risk' \| 'Medium Risk' \| 'Low Risk' | Derived | Based on `is_fraudulent` flag or statistical outliers (mean ± 2σ) |
| **activity_level** | VARCHAR | Claim volume: 'Very High Volume' (>1000) \| 'High Volume' (>500) \| 'Medium Volume' (>100) \| 'Low Volume' (>0) \| 'Inactive' | Derived | Based on `total_claims` buckets |

### Sample Data
```
provider_sk | provider_id | is_fraudulent | total_claims | avg_claim_amount | risk_level    | activity_level
------------|-------------|---------------|--------------|------------------|---------------|----------------
1           | PRV001      | TRUE          | 4,250        | 8,523.45         | Confirmed Fraud | Very High Volume
2           | PRV002      | FALSE         | 89           | 5,234.12         | Low Risk       | Low Volume
3           | PRV003      | FALSE         | 2,134        | 12,456.78        | High Risk      | High Volume
```

### Fraud Indicators
Fraudulent providers exhibit these patterns:
- Significantly higher average claim amounts (>2σ from mean)
- Unusual procedure-to-diagnosis combinations
- High concentration of specific procedure codes
- Claims submitted with minimal length of stay

### Use Cases
- Identifying high-risk providers for investigation
- Provider performance benchmarking
- Network analysis (peer comparisons)
- Fraud detection model features

---

## DIM_PATIENT_ETL

**Grain:** One row per patient/beneficiary  
**Record Count:** 138,556 patients  
**Source:** `beneficiary` table from operational data  
**Storage:** `s3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_patient_clean/`  
**Primary Key:** `patient_sk` (Surrogate Key)  
**Natural Key:** `patient_id` (BeneID)

### Column Definitions

| Column | Data Type | Description | Source | Business Logic |
|--------|-----------|-------------|--------|-----------------|
| **patient_sk** | INT | Surrogate key (auto-generated) | Generated | `ROW_NUMBER() OVER (ORDER BY patient_id)` |
| **patient_id** | VARCHAR | Beneficiary ID (unique identifier) | Operational | `BeneID` from beneficiary table |
| **date_of_birth** | DATE | Patient birth date | Operational | Direct from beneficiary table |
| **date_of_death** | DATE | Date of death (NULL if living) | Operational | `DOD` field, NULL if not deceased |
| **gender** | VARCHAR | 'Male' \| 'Female' | Operational | Decoded from gender codes (1='Male', 2='Female') |
| **race** | VARCHAR | 'White' \| 'Black' \| 'Asian' \| 'Hispanic' \| 'Other' | Operational | Decoded from race codes (1-5) |
| **state_code** | INT | State FIPS code | Operational | Direct from State field |
| **county_code** | INT | County FIPS code | Operational | Direct from County field |
| **has_renal_disease** | BOOLEAN | TRUE if renal disease indicator = 'Y' | Operational | `RenalDiseaseIndicator mapped to boolean |
| **chronic_condition_count** | INT | Total number of chronic conditions (0-11) | Derived | Sum of all chronic condition flags |
| **is_deceased** | BOOLEAN | TRUE if date_of_death is not null | Derived | `DOD IS NOT NULL` |
| **risk_category** | VARCHAR | Risk tier: 'Very High' (5+ conditions) \| 'High' (3-4) \| 'Medium' (1-2) \| 'Low' (0) | Derived | Based on chronic_condition_count buckets |
| **has_alzheimer** | BOOLEAN | Alzheimer's disease diagnosis | Operational | `ChronicCond_Alzheimer = '1'` |
| **has_heart_failure** | BOOLEAN | Heart failure diagnosis | Operational | `ChronicCond_Heartfailure = '1'` |
| **has_kidney_disease** | BOOLEAN | Kidney disease diagnosis | Operational | `ChronicCond_KidneyDisease = '1'` |
| **has_cancer** | BOOLEAN | Cancer diagnosis | Operational | `ChronicCond_Cancer = '1'` |
| **has_copd** | BOOLEAN | Chronic Obstructive Pulmonary Disease | Operational | `ChronicCond_ObstrPulmonary = '1'` |
| **has_depression** | BOOLEAN | Depression diagnosis | Operational | `ChronicCond_Depression = '1'` |
| **has_diabetes** | BOOLEAN | Diabetes diagnosis | Operational | `ChronicCond_Diabetes = '1'` |
| **has_ischemic_heart** | BOOLEAN | Ischemic heart disease | Operational | `ChronicCond_IschemicHeart = '1'` |
| **has_osteoporosis** | BOOLEAN | Osteoporosis diagnosis | Operational | `ChronicCond_Osteoporasis = '1'` |
| **has_arthritis** | BOOLEAN | Rheumatoid arthritis diagnosis | Operational | `ChronicCond_rheumatoidarthritis = '1'` |
| **has_stroke** | BOOLEAN | Stroke diagnosis | Operational | `ChronicCond_stroke = '1'` |

### Sample Data
```
patient_sk | patient_id | gender | race  | chronic_condition_count | risk_category | has_diabetes | has_heart_failure
-----------|------------|--------|-------|-------------------------|---------------|--------------|------------------
1          | BEN001     | Male   | White | 5                       | Very High     | TRUE         | TRUE
2          | BEN002     | Female | Black | 2                       | Medium        | TRUE         | FALSE
3          | BEN003     | Male   | Asian | 0                       | Low           | FALSE        | FALSE
```

### Chronic Condition Notes
- Medicare tracks 11 chronic conditions
- Each condition is a separate boolean field
- More chronic conditions = higher healthcare costs and risk
- Fraud patterns often correlate with patient risk profiles

### Use Cases
- Patient risk stratification
- Claims cost prediction
- Identifying high-utilizers
- Measuring health outcomes
- Fraud pattern analysis (e.g., do fraudulent providers disproportionately treat high-risk patients?)

---

## DIM_DIAGNOSIS_ETL

**Grain:** One row per unique diagnosis code  
**Record Count:** ~1,200 diagnosis codes  
**Source:** Extracted from all ClmDiagnosisCode_* fields in claims  
**Storage:** `s3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_diagnosis/`  
**Primary Key:** `diagnosis_sk` (Surrogate Key)  
**Natural Key:** `diagnosis_code`

### Column Definitions

| Column | Data Type | Description | Source | Business Logic |
|--------|-----------|-------------|--------|-----------------|
| **diagnosis_sk** | INT | Surrogate key | Generated | `ROW_NUMBER() OVER (ORDER BY diagnosis_code)` |
| **diagnosis_code** | VARCHAR | ICD-9 diagnosis code (e.g., '4019') | Operational | Extracted from claim diagnosis fields |
| **code_category** | VARCHAR | First 3 digits of code | Derived | `SUBSTRING(diagnosis_code, 1, 3)` |
| **icd9_chapter** | VARCHAR | Medical chapter (e.g., 'Circulatory System') | Derived | Mapped based on ICD-9 ranges |
| **subcategory** | VARCHAR | Specific condition (e.g., 'Diabetes Mellitus') | Derived | Mapped for common codes |
| **is_chronic** | BOOLEAN | TRUE if chronic condition | Derived | Common chronic codes flagged |
| **risk_level** | VARCHAR | 'High' \| 'Standard' \| 'Low' | Derived | Cancers and cardio = High, V codes = Low |
| **is_common_code** | BOOLEAN | TRUE if frequently appears in data | Derived | Top 6 diagnosis codes flagged |
| **cost_category** | VARCHAR | 'Very High' \| 'High' \| 'Standard' | Derived | Based on typical costs for code |

### ICD-9 Chapters
Diagnosis codes are organized into chapters:
- 001-139: Infectious and Parasitic Diseases
- 140-239: Neoplasms (cancers)
- 240-279: Endocrine, Nutritional, Metabolic
- 280-289: Diseases of Blood and Blood-forming Organs
- 290-319: Mental Disorders
- 320-389: Diseases of Nervous System and Sense Organs
- 390-459: Diseases of Circulatory System
- 460-519: Diseases of Respiratory System
- 520-579: Diseases of Digestive System
- 580-629: Diseases of Genitourinary System
- 630-679: Complications of Pregnancy, Childbirth, Puerperium
- 680-709: Diseases of Skin and Subcutaneous Tissue
- 710-739: Diseases of Musculoskeletal System and Connective Tissue
- 740-759: Congenital Anomalies
- 760-779: Certain Conditions Originating in Perinatal Period
- 780-799: Symptoms, Signs, and Ill-Defined Conditions
- 800-999: Injury and Poisoning
- V01-V89: Supplementary Classification (preventive care, routine visits)
- E800-E999: External Causes of Injury and Poisoning

### Fraud Implications
- **Commonly Upcoded**: Certain diagnoses (e.g., 786 = chest pain) often billed with more expensive procedures
- **Unusual Combinations**: Some diagnosis-procedure pairs are extremely rare and may indicate fraud
- **V Codes**: Preventive care codes (V codes) are low-cost; if associated with expensive procedures = red flag

### Use Cases
- Diagnosis-specific analytics
- Identifying unusual diagnosis-procedure combinations
- Chronic disease burden analysis
- Cost prediction models
- Fraud pattern detection

---

## DIM_PROCEDURE_ETL

**Grain:** One row per unique procedure code  
**Record Count:** ~320 procedure codes  
**Source:** Extracted from all ClmProcedureCode_* fields in claims  
**Storage:** `s3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_procedure/`  
**Primary Key:** `procedure_sk` (Surrogate Key)  
**Natural Key:** `procedure_code`

### Column Definitions

| Column | Data Type | Description | Source | Business Logic |
|--------|-----------|-------------|--------|-----------------|
| **procedure_sk** | INT | Surrogate key | Generated | `ROW_NUMBER() OVER (ORDER BY procedure_code)` |
| **procedure_code** | VARCHAR | ICD-9 procedure code (e.g., '3895') | Operational | Extracted from claim procedure fields |
| **code_category** | VARCHAR | First 2 digits | Derived | `SUBSTRING(procedure_code, 1, 2)` |
| **code_type** | VARCHAR | Always 'ICD-9 Procedure' | Fixed | Standard for this dataset |
| **procedure_category** | VARCHAR | Procedure type (e.g., 'Operations on Cardiovascular System') | Derived | Mapped based on ICD-9-CM ranges |
| **is_major_procedure** | BOOLEAN | TRUE for complex/surgical procedures | Derived | Codes 01-05, 35-39, 60-71 flagged |
| **risk_level** | VARCHAR | 'High' \| 'Medium' \| 'Low' | Derived | Major procedures = High |
| **cost_category** | VARCHAR | 'Very High' \| 'High' \| 'Medium' \| 'Low' | Derived | Based on typical procedure costs |

### ICD-9 Procedure Ranges
- 01-05: Operations on Nervous System
- 06-07: Operations on Endocrine System
- 08-16: Operations on Eye
- 17-20: Operations on Ear
- 21-29: Operations on Nose, Mouth, and Pharynx
- 30-34: Operations on Respiratory System
- **35-39: Operations on Cardiovascular System** (High cost, often fraudulent)
- 40-41: Operations on Hemic and Lymphatic System
- 42-54: Operations on Digestive System
- 55-59: Operations on Urinary System
- 60-64: Operations on Male Genital Organs
- 65-71: Operations on Female Genital Organs
- 72-75: Obstetrical Procedures
- 76-84: Operations on Musculoskeletal System
- 85-86: Operations on Integumentary System
- 87+: Miscellaneous Diagnostic and Therapeutic Procedures

### Fraud Implications
- **High-Cost Procedures**: Cardiac and orthopedic procedures are expensive targets for fraud
- **Bunching**: Submitting multiple procedures in one claim can inflate billing
- **Unlikely Combinations**: E.g., cardiac surgery (35-39) for patient with chest pain code only = suspicious

### Use Cases
- Procedure-specific cost analysis
- Identifying unusual procedure combinations
- Surgical volume trends
- Fraud detection (unusual procedure codes)
- Provider specialty inference

---

# FACT TABLES

---

## FACT_CLAIMS_ETL

**Grain:** One row per claim (transaction-level)  
**Record Count:** 558,211 claims  
**Time Coverage:** 2008-2010  
**Source:** Inpatient + Outpatient claims combined  
**Storage:** `s3://insurance-claim-qian-2025/data/warehouse/lambda_etl/fact_tables/fact_claims/`

### Column Definitions

| Column | Data Type | Description | Source | Business Logic |
|--------|-----------|-------------|--------|-----------------|
| **claim_sk** | BIGINT | Claim surrogate key (unique per claim) | Generated | `ROW_NUMBER() OVER (ORDER BY claim_id)` |
| **patient_sk** | INT | FK to dim_patient_etl | Operational | Join on patient_id |
| **provider_sk** | INT | FK to dim_provider_etl | Operational | Join on provider_id |
| **claim_start_date_key** | INT | FK to dim_date_etl (YYYYMMDD) | Operational | ClaimStartDt |
| **claim_end_date_key** | INT | FK to dim_date_etl (YYYYMMDD) | Operational | ClaimEndDt |
| **admission_date_key** | INT | FK to dim_date_etl (YYYYMMDD) or 0 if NULL | Operational | AdmissionDt (inpatient only) |
| **discharge_date_key** | INT | FK to dim_date_etl (YYYYMMDD) or 0 if NULL | Operational | DischargeDt (inpatient only) |
| **claim_id** | VARCHAR(50) | Unique claim identifier | Operational | ClaimID from source |
| **claim_type** | VARCHAR(20) | 'Inpatient' \| 'Outpatient' | Operational | Derived from source table |
| **claim_amount** | DECIMAL(12,2) | Insurance reimbursement ($) | Operational | InscClaimAmtReimbursed |
| **deductible_amount** | DECIMAL(12,2) | Patient deductible paid ($) | Operational | DeductibleAmtPaid |
| **length_of_stay** | INT | Days from admission to discharge | Derived | `DATEDIFF(day, admission_date, discharge_date)` |
| **total_amount** | DECIMAL(12,2) | Claim + deductible ($) | Derived | `claim_amount + deductible_amount` |
| **is_fraudulent** | BOOLEAN | Known fraud flag | Operational | Mapped from Train.csv PotentialFraud |
| **admit_diagnosis_code** | VARCHAR(10) | Primary admission diagnosis | Operational | ClmAdmitDiagnosisCode |
| **diagnosis_group_code** | VARCHAR(10) | Diagnosis Related Group (DRG) | Operational | DiagnosisGroupCode |
| **diagnosis_code_1** | VARCHAR(10) | Primary diagnosis | Operational | ClmDiagnosisCode_1 |
| **procedure_code_1** | VARCHAR(10) | Primary procedure | Operational | ClmProcedureCode_1 |
| **attending_physician_id** | VARCHAR(20) | Physician providing primary care | Operational | AttendingPhysician |
| **operating_physician_id** | VARCHAR(20) | Surgeon/operator (if applicable) | Operational | OperatingPhysician |
| **other_physician_id** | VARCHAR(20) | Other involved physician | Operational | OtherPhysician |

### Sample Data
```
claim_sk | patient_sk | provider_sk | claim_start_date_key | claim_type  | claim_amount | is_fraudulent
---------|------------|-------------|----------------------|-------------|--------------|---------------
1        | 5          | 23          | 20081015             | Inpatient   | 12500.00     | FALSE
2        | 105        | 478         | 20090523             | Outpatient  | 850.00       | TRUE
3        | 205        | 89          | 20090704             | Inpatient   | 8900.00      | FALSE
```

### Key Observations
- **Inpatient vs Outpatient**: Inpatient claims average $10K+; outpatient average $2K
- **Fraud Indicator**: ~38% of claims flagged as fraudulent
- **Date Fields**: Admission/discharge dates are NULL for outpatient (use claim dates instead)
- **Physicians**: Identifies responsible providers for investigation

### Use Cases
- Individual claim analysis
- Detailed fraud investigation
- Physician performance review
- Diagnosis-procedure verification
- Cost accounting by type

---

## FACT_PROVIDER_SUMMARY_ETL

**Grain:** One row per provider per month  
**Record Count:** ~70K (5,410 providers × 12-13 months per provider)  
**Purpose:** Aggregated view for provider-level analysis  
**Storage:** `s3://insurance-claim-qian-2025/data/warehouse/lambda_etl/fact_tables/fact_provider_summary_v2/`

### Column Definitions

| Column | Data Type | Description | Source | Business Logic |
|--------|-----------|-------------|--------|-----------------|
| **provider_sk** | INT | FK to dim_provider_etl | Operational | Join on provider_id |
| **month_key** | VARCHAR | YYYYMM format (e.g., '200810') | Derived | `DATE_FORMAT(claim_start_date, '%Y%m')` |
| **total_claims** | INT | Total claims submitted | Derived | `COUNT(*)` |
| **inpatient_claims** | INT | Count of inpatient claims | Derived | `COUNT(*) WHERE claim_type = 'Inpatient'` |
| **outpatient_claims** | INT | Count of outpatient claims | Derived | `COUNT(*) WHERE claim_type = 'Outpatient'` |
| **unique_patients** | INT | Distinct patients treated | Derived | `COUNT(DISTINCT patient_id)` |
| **total_claimed** | DECIMAL(15,2) | Total claims amount ($) | Derived | `SUM(claim_amount)` |
| **avg_claim_amount** | DECIMAL(12,2) | Average claim value ($) | Derived | `AVG(claim_amount)` |
| **provider_is_fraudulent** | BOOLEAN | Known fraud flag | Operational | From dim_provider_etl |
| **fraud_exposure_amount** | DECIMAL(15,2) | Sum of fraudulent claims ($) | Derived | `SUM(claim_amount) WHERE is_fraudulent = TRUE` |

### Sample Data
```
provider_sk | month_key | total_claims | inpatient_claims | avg_claim_amount | fraud_exposure_amount
------------|-----------|--------------|------------------|------------------|----------------------
1           | 200810    | 456          | 145               | 8,234.50         | 1,250,000.00
1           | 200811    | 423          | 138               | 8,901.23         | 1,450,000.00
2           | 200810    | 23           | 5                 | 4,567.89         | 0.00
```

### Use Cases
- Monthly provider performance tracking
- Fraud trends over time by provider
- Seasonal pattern identification
- Provider capacity planning
- Risk score updates

---

## FACT_PATIENT_CLAIMS_SUMMARY_ETL

**Grain:** One row per patient per month  
**Record Count:** ~500K (138K patients × variable months)  
**Purpose:** Patient-level monthly utilization and costs  
**Storage:** `s3://insurance-claim-qian-2025/data/warehouse/lambda_etl/fact_tables/fact_patient_summary_v2/`

### Column Definitions

| Column | Data Type | Description | Source | Business Logic |
|--------|-----------|-------------|--------|-----------------|
| **patient_sk** | INT | FK to dim_patient_etl | Operational | Join on patient_id |
| **month_key** | VARCHAR | YYYYMM format | Derived | `DATE_FORMAT(claim_start_date, '%Y%m')` |
| **total_claims** | INT | Total claims submitted | Derived | `COUNT(*)` |
| **inpatient_visits** | INT | Inpatient hospital visits | Derived | `COUNT(*) WHERE claim_type = 'Inpatient'` |
| **outpatient_visits** | INT | Outpatient clinic visits | Derived | `COUNT(*) WHERE claim_type = 'Outpatient'` |
| **total_claimed** | DECIMAL(12,2) | Total claim amounts ($) | Derived | `SUM(claim_amount)` |
| **total_deductible** | DECIMAL(12,2) | Patient's deductible portions ($) | Derived | `SUM(deductible_amount)` |
| **fraudulent_provider_visits** | INT | Visits to known fraudulent providers | Derived | `COUNT(*) WHERE provider.is_fraudulent = TRUE` |
| **fraud_exposure_amount** | DECIMAL(12,2) | Amount claimed by fraudulent providers ($) | Derived | `SUM(claim_amount) WHERE provider.is_fraudulent = TRUE` |

### Sample Data
```
patient_sk | month_key | total_claims | inpatient_visits | total_claimed | fraudulent_provider_visits | fraud_exposure_amount
-----------|-----------|--------------|------------------|---------------|---------------------------|----------------------
1          | 200810    | 3            | 1                 | 15,234.50     | 1                         | 10,234.50
1          | 200811    | 2            | 0                 | 4,567.89      | 0                         | 0.00
5          | 200810    | 1            | 0                 | 2,345.67      | 1                         | 2,345.67
```

### Use Cases
- Patient health cost tracking
- Identifying high-utilizers
- Fraud exposure per patient
- Patient risk stratification
- Predictive modeling (future costs)

---

# VIEWS

---

## V_PROVIDERS_ETL

**Type:** Transformation view  
**Purpose:** Clean and standardize provider fraud flags  
**Source:** provider table  
**Records:** 5,410 providers

**Transformation Logic:**
- Maps inconsistent fraud indicators to boolean
- Preserves raw value for debugging
- Standardizes various representations: "Yes", "Y", "1" → TRUE

```sql
SELECT DISTINCT
    Provider AS provider_id,
    CASE 
        WHEN PotentialFraud LIKE '%es%' THEN true
        WHEN PotentialFraud = 'Y' THEN true
        WHEN PotentialFraud = '1' THEN true
        ELSE false 
    END AS is_fraudulent,
    PotentialFraud AS raw_fraud_value
FROM provider
```

---

## V_PATIENTS_ETL

**Type:** Transformation view  
**Purpose:** Decode demographic and health codes from operational format  
**Source:** beneficiary table  
**Records:** 138,556 patients

**Transformation Logic:**
- Gender codes: 1→Male, 2→Female
- Race codes: 1→White, 2→Black, 3→Asian, 4→Hispanic, 5→Other
- Chronic conditions: 1→TRUE, 2→FALSE
- Calculate total chronic condition count (0-11)
- Handle date fields (convert NA strings to NULL)

---

## V_INPATIENT_CLAIMS_ETL

**Type:** Transformation view  
**Purpose:** Clean and standardize inpatient claim records  
**Source:** inpatient table  
**Records:** 40,474 inpatient claims

**Transformation Logic:**
- Cast dates from strings to DATE type
- Calculate length of stay: DATEDIFF(days)
- Rename columns for consistency
- Standardize all 10 diagnosis codes
- Standardize all 6 procedure codes
- Add claim_type = 'Inpatient' identifier

---

## V_OUTPATIENT_CLAIMS_ETL

**Type:** Transformation view  
**Purpose:** Clean and standardize outpatient claim records  
**Source:** outpatient table  
**Records:** 517,737 outpatient claims

**Transformation Logic:**
- Same cleaning as inpatient
- No length of stay (ambulatory)
- No admission/discharge dates
- Add claim_type = 'Outpatient' identifier

---

## V_ALL_CLAIMS_ETL

**Type:** Transformation view  
**Purpose:** Union inpatient and outpatient for unified analysis  
**Source:** v_inpatient_claims_etl + v_outpatient_claims_etl  
**Records:** 558,211 total claims

**Transformation Logic:**
- UNION ALL (preserves duplicates)
- Inpatient has more fields (admission, discharge, LOS)
- Outpatient sets those to NULL
- All other fields aligned across both sources

---

# KEY RELATIONSHIPS

---

## Relationship Diagram

```
dim_patient_etl
    ↑
    |
    └────────────────┐
                     │
                     ↓
            fact_claims_etl      ←--- dim_date_etl
                     ↑
                     |
            ┌────────┘
            │
            └─ dim_provider_etl
                     ↑
                     |
            └──────────────┐
                            │
            dim_diagnosis_etl  dim_procedure_etl
```

## Primary & Foreign Key Relationships

| Table | PK | FKs | Related To |
|-------|----|----|-----------|
| **dim_patient_etl** | patient_sk | None | fact_claims_etl |
| **dim_provider_etl** | provider_sk | None | fact_claims_etl, fact_provider_summary_etl |
| **dim_date_etl** | date_key | None | fact_claims_etl, fact_provider_summary_etl, fact_patient_claims_summary_etl |
| **dim_diagnosis_etl** | diagnosis_sk | None | Lookups via diagnosis_code |
| **dim_procedure_etl** | procedure_sk | None | Lookups via procedure_code |
| **fact_claims_etl** | claim_sk | patient_sk, provider_sk, date_keys | Core fact table |
| **fact_provider_summary_etl** | Composite: (provider_sk, month_key) | provider_sk | Aggregation of fact_claims |
| **fact_patient_claims_summary_etl** | Composite: (patient_sk, month_key) | patient_sk | Aggregation of fact_claims |

---

# BUSINESS LOGIC

---

## Fraud Classification

### Fields Related to Fraud
- `fact_claims_etl.is_fraudulent` - Known fraud indicator
- `dim_provider_etl.is_fraudulent` - Provider-level fraud flag
- `dim_provider_etl.risk_level` - Risk classification

### Data Quality Issues Addressed
**Problem:** Inconsistent fraud field format
- Some values: "Yes", "YES", "yes"
- Some values: "Y"
- Some values: "1"
- Some values: Empty/NULL

**Solution:** Case statement standardizes to boolean in `v_providers_etl`

### Fraud Pattern Detection
1. **Provider-Level Patterns:**
   - High avg claim amount (>2σ from mean)
   - Unusual diagnosis-procedure combinations
   - High concentration of specific codes
   - Disproportionate inpatient vs outpatient ratio

2. **Patient-Level Patterns:**
   - Multiple visits to same fraudulent provider
   - High healthcare costs despite low-risk conditions
   - Unusual service dates (e.g., weekend procedures)

3. **Claim-Level Patterns:**
   - Inconsistent DRG for diagnosis
   - Procedure codes rare in combination
   - Amounts inconsistent with typical ranges

---

## Date Handling

### Date Key Format
All dates stored as INT in YYYYMMDD format for:
- Efficient indexing
- Easy range filtering
- Compatibility with date arithmetic

**Example:**
- Physical date: 2008-10-15
- Stored as: 20081015 (INT)
- Used in: claim_start_date_key, admission_date_key, etc.

### NULL Date Handling
- Outpatient claims: admission_date_key = 0, discharge_date_key = 0
- Use COALESCE to handle NULL dates in calculations
- Length of stay = 0 for outpatient claims

---

## Cost Calculations

### Claim Amount Fields
- **claim_amount**: Insurance pays (main metric)
- **deductible_amount**: Patient pays (deductible)
- **total_amount**: claim_amount + deductible_amount (total cost to system)

### Outpatient Special Cases
- Deductible typically NULL (covered under annual deductible)
- Claim amounts are smaller (~$2K vs $10K inpatient)
- Patient may have copay (not tracked in these fields)

---

## Aggregation Rules

### Provider Aggregations
- **Grain:** Provider × Month
- **Dimensions aggregated:** Claims summed across patients, dates
- **Metrics recalculated:** Averages, counts, sums
- **Fraud exposure:** Isolated to fraudulent claims

### Patient Aggregations
- **Grain:** Patient × Month
- **Dimensions aggregated:** Claims summed across providers, dates
- **Metrics tracked:** Utilization and fraud exposure
- **Risk tracking:** Fraud exposure by provider type

---

# DATA QUALITY NOTES

---

## Known Issues & Resolutions

### 1. Embedded Quotes in CSV
**Issue:** Some diagnosis/procedure codes had embedded quotes
**Resolution:** REPLACE() functions remove quotes during transformation
**Impact:** <0.1% of records

### 2. Date Format Inconsistency
**Issue:** Date fields stored as strings in operational data
**Resolution:** TRY_CAST to DATE; failed conversions result in NULL
**Impact:** <1% of dates fail conversion

### 3. Missing Diagnosis/Procedure Codes
**Issue:** Many claims have NULL values for diagnoses 2-10, procedures 2-6
**Resolution:** Treat as optional; only analyze non-NULL codes
**Impact:** Analysis focuses on primary diagnosis/procedure

### 4. State/County Codes
**Issue:** Stored as numeric codes, not state abbreviations
**Resolution:** Mapping table required for human-readable values (not included in current ETL)
**Note:** Raw codes available for geographic analysis

### 5. Deceased Patients
**Issue:** Some deceased patients have claims AFTER death date
**Resolution:** Data quality issue; trust operational data as-is
**Recommendation:** Filter out post-death claims in analytics

---

## Validation Queries

### Record Counts (Expected)
```sql
-- Should see ~1,090 dates
SELECT COUNT(DISTINCT full_date) FROM dim_date_etl;

-- Should see ~5,410 providers
SELECT COUNT(*) FROM dim_provider_etl;

-- Should see ~138,556 patients
SELECT COUNT(*) FROM dim_patient_etl;

-- Should see ~558,211 claims
SELECT COUNT(*) FROM fact_claims_etl;
```

### Data Integrity Checks
```sql
-- Check for orphaned patients (shouldn't exist)
SELECT COUNT(*) FROM fact_claims_etl f
WHERE f.patient_sk NOT IN (SELECT patient_sk FROM dim_patient_etl);

-- Check for orphaned providers (shouldn't exist)
SELECT COUNT(*) FROM fact_claims_etl f
WHERE f.provider_sk NOT IN (SELECT provider_sk FROM dim_provider_etl);

-- Distribution of claim types (should be ~93% outpatient)
SELECT claim_type, COUNT(*), ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) pct
FROM fact_claims_etl
GROUP BY claim_type;
```

---

# EXAMPLE QUERIES

---

## Example 1: Top 10 Fraudulent Providers by Exposure

```sql
SELECT TOP 10
    p.provider_id,
    p.total_claims,
    p.total_patients,
    ROUND(fpm.fraud_exposure_amount, 2) as total_fraud_exposure,
    ROUND(100.0 * fpm.fraud_exposure_amount / p.total_amount, 1) as fraud_pct
FROM dim_provider_etl p
LEFT JOIN (
    SELECT provider_sk, SUM(fraud_exposure_amount) as fraud_exposure_amount
    FROM fact_provider_summary_etl
    GROUP BY provider_sk
) fpm ON p.provider_sk = fpm.provider_sk
WHERE p.is_fraudulent = TRUE
ORDER BY fraud_exposure_amount DESC;
```

## Example 2: Average Claim Amount by Provider Type

```sql
SELECT
    p.provider_type,
    COUNT(*) as total_claims,
    COUNT(DISTINCT fc.patient_sk) as unique_patients,
    ROUND(AVG(fc.claim_amount), 2) as avg_claim_amount,
    ROUND(MAX(fc.claim_amount), 2) as max_claim_amount
FROM fact_claims_etl fc
JOIN dim_provider_etl p ON fc.provider_sk = p.provider_sk
GROUP BY p.provider_type
ORDER BY avg_claim_amount DESC;
```

## Example 3: Patient Risk Stratification

```sql
SELECT
    dp.risk_category,
    COUNT(DISTINCT dp.patient_sk) as patient_count,
    COUNT(*) as total_claims,
    ROUND(AVG(fc.claim_amount), 2) as avg_claim_cost,
    ROUND(SUM(fc.claim_amount), 0) as total_cost,
    COUNT(DISTINCT fc.provider_sk) as unique_providers
FROM dim_patient_etl dp
LEFT JOIN fact_claims_etl fc ON dp.patient_sk = fc.patient_sk
GROUP BY dp.risk_category
ORDER BY patient_count DESC;
```

## Example 4: Fraud Exposure by Month (Trend)

```sql
SELECT
    fps.month_key,
    SUM(fps.total_claims) as total_claims,
    SUM(fps.fraud_exposure_amount) as fraud_exposure,
    ROUND(100.0 * SUM(fps.fraud_exposure_amount) / SUM(fps.total_claimed), 1) as fraud_pct
FROM fact_provider_summary_etl fps
GROUP BY fps.month_key
ORDER BY fps.month_key;
```

## Example 5: Unusual Diagnosis-Procedure Combinations

```sql
SELECT TOP 20
    dd.diagnosis_code,
    dd.subcategory,
    dp.procedure_code,
    dp.procedure_category,
    COUNT(*) as frequency,
    ROUND(AVG(fc.claim_amount), 2) as avg_amount
FROM fact_claims_etl fc
JOIN dim_diagnosis_etl dd ON fc.diagnosis_code_1 = dd.diagnosis_code
JOIN dim_procedure_etl dp ON fc.procedure_code_1 = dp.procedure_code
WHERE dd.diagnosis_code IS NOT NULL
  AND dp.procedure_code IS NOT NULL
GROUP BY dd.diagnosis_code, dd.subcategory, dp.procedure_code, dp.procedure_category
ORDER BY frequency DESC;
```

---

## Glossary of Terms

| Term | Definition |
|------|-----------|
| **Grain** | The level of detail (what does each row represent?) |
| **Surrogate Key** | Auto-generated primary key (often INT or BIGINT) |
| **Natural Key** | Business identifier from operational data |
| **Fact** | Quantifiable event (claim, visit, transaction) |
| **Dimension** | Context/attribute data (who, what, when, where) |
| **Slowly Changing Dimension** | Data that changes over time (track history with dates) |
| **Star Schema** | Fact table in center, dimensions as points (simplified design) |
| **OLAP** | Online Analytical Processing (queries across dimensions) |
| **DRG** | Diagnosis Related Group (payment classification) |
| **ICD-9** | International Classification of Diseases, 9th revision (medical coding system) |

---

**Last Updated:** 2025.12.31  
**Data Period:** 2008-2010  
**Total Records:** ~700K (claims + dimensions)  
**Maintenance:** Update monthly with new claims data
