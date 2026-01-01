# 📜 SQL Queries - Medical Insurance Claims ETL

## Table of Contents

1. [Overview](#overview)
2. [Directory Structure](#directory-structure)
3. [Execution Order](#execution-order)
4. [Views (01-views/)](#views-01-views)
5. [Dimensions (02-dims/)](#dimensions-02-dims)
6. [Facts (03-facts/)](#facts-03-facts)
7. [Validation (04-validate/)](#validation-04-validate)
8. [Running Queries](#running-queries)
9. [Performance Notes](#performance-notes)
10. [Troubleshooting](#troubleshooting)

---

## Overview

This folder contains all **SQL transformation queries** for the medical insurance claims data warehouse. The queries follow a **dimensional modeling** approach (Kimball method) to build a **star schema** optimized for analytics.

### Purpose
Transform raw operational data into a clean, aggregated data warehouse suitable for:
- ✅ Fraud detection & analysis
- ✅ Provider performance tracking
- ✅ Patient risk stratification
- ✅ Claims cost analysis
- ✅ Business intelligence dashboards

### Technology Stack
- **Query Engine:** Amazon Athena (Presto/Trino SQL dialect)
- **Data Format:** Parquet (compressed, columnar)
- **Storage:** S3 data lake
- **Processing:** AWS Lambda (orchestrates queries)
- **Execution Time:** ~5 minutes for full pipeline

### Data Volume
- **Input:** 558K claims + 138K patients + 5.4K providers
- **Output:** ~700K total records across 8 tables/views
- **Time Period:** 2008-2010 (3 years of data)

---

## Directory Structure

```
sql/
├── 01-views/                          # Transformation views
│   ├── v_providers_etl.sql           # Standardize provider fraud flags
│   ├── v_patients_etl.sql            # Decode patient demographics
│   ├── v_inpatient_claims_etl.sql    # Clean inpatient claims
│   ├── v_outpatient_claims_etl.sql   # Clean outpatient claims
│   └── v_all_claims_etl.sql          # Union all claims
│
├── 02-dims/                           # Dimension tables
│   ├── dim_date_etl.sql              # Calendar dimensions (~1K rows)
│   ├── dim_provider_etl.sql          # Provider context (5.4K rows)
│   ├── dim_patient_etl.sql           # Patient demographics (138K rows)
│   ├── dim_diagnosis_etl.sql         # Diagnosis codes (1.2K rows)
│   └── dim_procedure_etl.sql         # Procedure codes (320 rows)
│
├── 03-facts/                          # Fact tables
│   ├── fact_claims_etl.sql           # Individual claims (558K rows)
│   ├── fact_provider_summary_etl.sql # Monthly provider summaries (70K rows)
│   └── fact_patient_claims_summary_etl.sql # Monthly patient summaries (500K rows)
│
├── 04-validate/                       # Validation & QA queries
│   └── validation_queries.sql        # Data quality checks
│
└── README.md                          # This file
```

---

## Execution Order

### Critical: Follow This Sequence

```
Step 1: VIEWS (Foundation)
├── v_providers_etl.sql              [0.5 min] ✅ Creates cleaned provider data
├── v_patients_etl.sql               [0.5 min] ✅ Creates cleaned patient data
├── v_inpatient_claims_etl.sql       [1 min]   ✅ Creates cleaned inpatient claims
├── v_outpatient_claims_etl.sql      [1 min]   ✅ Creates cleaned outpatient claims
└── v_all_claims_etl.sql             [1 min]   ✅ Combines all claims (dependency on views above)

        ↓ (All views must complete before proceeding)

Step 2: DIMENSIONS (Context)
├── dim_date_etl.sql                 [1 min]   ✅ Extract unique dates
├── dim_provider_etl.sql             [1 min]   ✅ Build provider dimension
├── dim_patient_etl.sql              [0.5 min] ✅ Build patient dimension
├── dim_diagnosis_etl.sql            [1 min]   ✅ Build diagnosis dimension
└── dim_procedure_etl.sql            [0.5 min] ✅ Build procedure dimension

        ↓ (All dimensions must complete before facts)

Step 3: FACTS (Measures)
├── fact_claims_etl.sql              [1 min]   ✅ Individual claims (depends on all dims)
├── fact_provider_summary_etl.sql    [0.5 min] ✅ Provider monthly summary
└── fact_patient_claims_summary_etl.sql [0.5 min] ✅ Patient monthly summary

        ↓

Step 4: VALIDATION (QA)
└── validation_queries.sql           [0.5 min] ✅ Run data quality checks

TOTAL TIME: ~5-6 minutes
```

**Why This Order?**
1. Views depend on raw tables → execute first
2. Dimensions are independent of each other → parallel execution possible
3. Facts depend on dimensions → execute after
4. Validation confirms success → execute last

---

## Views (01-views/)

Views transform raw operational data into a cleansed, standardized state. They handle:
- ✅ Data type conversions
- ✅ Code standardization
- ✅ NULL handling
- ✅ Data quality issues

### v_providers_etl.sql

**Purpose:** Standardize provider fraud flags  
**Source:** `provider` table  
**Output:** View with `provider_id` and `is_fraudulent` (boolean)

**What It Does:**
```
Input fraud values:          → Output (Boolean)
"Yes", "YES", "yes"         → TRUE
"Y", "y"                    → TRUE
"1"                         → TRUE
"No", "N", "0", NULL, ""   → FALSE
```

**Record Count:** 5,410 unique providers  
**Execution Time:** ~0.5 minutes

**Key Columns:**
- `provider_id` - Provider identifier
- `is_fraudulent` - Standardized to TRUE/FALSE
- `raw_fraud_value` - Original value (for debugging)

**Usage:**
```sql
-- Find all fraudulent providers
SELECT * FROM v_providers_etl WHERE is_fraudulent = TRUE;

-- Count distribution
SELECT is_fraudulent, COUNT(*) FROM v_providers_etl GROUP BY is_fraudulent;
```

---

### v_patients_etl.sql

**Purpose:** Decode patient demographic codes  
**Source:** `beneficiary` table  
**Output:** View with decoded patient attributes

**What It Does:**
- Converts gender codes to text: `1` → "Male", `2` → "Female"
- Converts race codes: `1` → "White", `2` → "Black", `3` → "Asian", `4` → "Hispanic", `5` → "Other"
- Converts chronic condition flags from `1/2` to `TRUE/FALSE`
- Counts total chronic conditions per patient (0-11)
- Handles date fields (converts NA strings to NULL)

**Record Count:** 138,556 unique patients  
**Execution Time:** ~0.5 minutes

**Key Columns:**
- `patient_id` - Beneficiary ID
- `gender` - Decoded gender
- `race` - Decoded race
- `chronic_condition_count` - Total conditions (0-11)
- `has_diabetes`, `has_heart_failure`, etc. - Boolean flags for each condition
- `is_deceased` - TRUE if deceased

**Usage:**
```sql
-- Find high-risk patients (5+ chronic conditions)
SELECT * FROM v_patients_etl WHERE chronic_condition_count >= 5;

-- Demographic distribution
SELECT gender, race, COUNT(*) FROM v_patients_etl GROUP BY gender, race;

-- Chronic disease prevalence
SELECT 
    'Diabetes' as condition,
    COUNT(*) as patient_count
FROM v_patients_etl
WHERE has_diabetes = TRUE;
```

---

### v_inpatient_claims_etl.sql

**Purpose:** Clean and standardize inpatient (hospital) claim records  
**Source:** `inpatient` table  
**Output:** View with clean, standardized inpatient claims

**What It Does:**
- Casts date fields from strings to DATE type
- Calculates length of stay: `DATEDIFF(day, admission_date, discharge_date)`
- Extracts all 10 diagnosis codes
- Extracts all 6 procedure codes
- Renames columns for consistency
- Adds `claim_type = 'Inpatient'` identifier
- Converts amounts to numeric types

**Record Count:** 40,474 inpatient claims  
**Execution Time:** ~1 minute

**Key Columns:**
- `patient_id`, `claim_id`, `provider_id` - Keys
- `claim_start_date`, `claim_end_date` - Service dates
- `admission_date`, `discharge_date` - Hospital stay dates
- `length_of_stay` - Days in hospital
- `claim_amount` - Insurance reimbursement
- `deductible_amount` - Patient deductible
- `diagnosis_code_1` to `diagnosis_code_10` - Medical diagnoses
- `procedure_code_1` to `procedure_code_6` - Surgical procedures
- `claim_type` - Always 'Inpatient'

**Usage:**
```sql
-- Find long hospital stays (>7 days)
SELECT * FROM v_inpatient_claims_etl 
WHERE length_of_stay > 7 
ORDER BY length_of_stay DESC;

-- Average stay by admission diagnosis
SELECT 
    admit_diagnosis_code,
    COUNT(*) as admissions,
    ROUND(AVG(length_of_stay), 1) as avg_los
FROM v_inpatient_claims_etl
GROUP BY admit_diagnosis_code
ORDER BY avg_los DESC;
```

---

### v_outpatient_claims_etl.sql

**Purpose:** Clean and standardize outpatient (clinic/ambulatory) claim records  
**Source:** `outpatient` table  
**Output:** View with clean, standardized outpatient claims

**What It Does:**
- Same cleaning as inpatient
- No admission/discharge dates (NULL for outpatient)
- No length of stay (ambulatory service)
- Adds `claim_type = 'Outpatient'` identifier
- Lower typical claim amounts (~$2K vs $10K inpatient)

**Record Count:** 517,737 outpatient claims  
**Execution Time:** ~1 minute

**Key Columns:**
- Similar to inpatient but WITHOUT admission/discharge/length_of_stay
- `claim_start_date` and `claim_end_date` define service period
- `claim_type` - Always 'Outpatient'

**Usage:**
```sql
-- Find high-cost outpatient claims (>$5000)
SELECT * FROM v_outpatient_claims_etl 
WHERE claim_amount > 5000
ORDER BY claim_amount DESC;

-- Most common outpatient procedures
SELECT 
    procedure_code_1,
    COUNT(*) as frequency,
    ROUND(AVG(claim_amount), 2) as avg_cost
FROM v_outpatient_claims_etl
WHERE procedure_code_1 IS NOT NULL
GROUP BY procedure_code_1
ORDER BY frequency DESC;
```

---

### v_all_claims_etl.sql

**Purpose:** Union inpatient and outpatient into single unified claims view  
**Source:** `v_inpatient_claims_etl` + `v_outpatient_claims_etl`  
**Output:** Combined view of all 558K claims

**What It Does:**
- UNION ALL both claim types (preserves all records)
- Aligns columns (outpatient sets admission/discharge/LOS to NULL)
- Creates foundation for dimensional model

**Record Count:** 558,211 total claims  
**Execution Time:** ~1 minute

**Key Columns:**
- All columns from both inpatient and outpatient
- `claim_type` identifies source: 'Inpatient' or 'Outpatient'

**Usage:**
```sql
-- Total claims by type
SELECT claim_type, COUNT(*) FROM v_all_claims_etl GROUP BY claim_type;

-- Claims over time
SELECT 
    DATE_FORMAT(claim_start_date, '%Y-%m') as month,
    COUNT(*) as claim_count
FROM v_all_claims_etl
GROUP BY DATE_FORMAT(claim_start_date, '%Y-%m')
ORDER BY month;
```

---

## Dimensions (02-dims/)

Dimensions provide **context and attributes** for facts. They answer "WHO, WHAT, WHERE, WHEN" questions.

### dim_date_etl.sql

**Purpose:** Create calendar dimension for time-based analysis  
**Source:** Extracted from all claim dates (union of start, end, admission, discharge)  
**Output:** One row per unique date

**Record Count:** ~1,090 unique dates (2008-2010)  
**Execution Time:** ~1 minute

**Key Columns:**
- `date_key` - INT in YYYYMMDD format (e.g., 20091225)
- `full_date` - Actual DATE type
- `year`, `quarter`, `month`, `day_of_month` - Date components
- `day_name`, `month_name` - Human-readable names
- `is_weekend` - Boolean for filtering

**Use Cases:**
- Time-based filtering (by month, quarter, year)
- Seasonal pattern detection
- Fiscal period analysis
- Holiday/weekend analysis

**Sample:**
```
date_key | full_date  | year | month | month_name | day_name   | is_weekend
---------|------------|------|-------|------------|------------|----------
20080101 | 2008-01-01 | 2008 | 1     | January    | Tuesday    | FALSE
20090704 | 2009-07-04 | 2009 | 7     | July       | Saturday   | TRUE
20101225 | 2010-12-25 | 2010 | 12    | December   | Saturday   | TRUE
```

---

### dim_provider_etl.sql

**Purpose:** Create provider dimension with profiles and metrics  
**Source:** `v_providers_etl` + aggregations from all claims  
**Output:** One row per provider with statistics

**Record Count:** 5,410 providers  
**Execution Time:** ~1 minute

**Key Columns:**
- `provider_sk` - Surrogate key (use in facts)
- `provider_id` - Business key (human-readable)
- `is_fraudulent` - Fraud indicator (TRUE/FALSE)
- `total_claims` - Count of all claims from provider
- `total_patients` - Count of unique patients treated
- `avg_claim_amount` - Average claim value
- `inpatient_claims` - Count of inpatient vs outpatient
- `outpatient_claims` - Helps identify provider type
- `provider_type` - Classification (Hospital/Clinic/Mixed)
- `risk_level` - Risk tier (Confirmed Fraud/High/Medium/Low)

**Use Cases:**
- Provider performance analysis
- Fraud risk identification
- Provider benchmarking
- Network analysis

**Key Statistic:**
- Fraudulent providers: ~5% of total
- Average fraud provider: 800+ claims
- Non-fraud providers: 50-100 claims average

---

### dim_patient_etl.sql

**Purpose:** Create patient dimension with demographics and health factors  
**Source:** `v_patients_etl` with risk categorization  
**Output:** One row per patient

**Record Count:** 138,556 patients  
**Execution Time:** ~0.5 minutes

**Key Columns:**
- `patient_sk` - Surrogate key (use in facts)
- `patient_id` - Beneficiary ID (business key)
- `date_of_birth`, `date_of_death` - Vital dates
- `gender`, `race` - Demographics
- `chronic_condition_count` - Number of conditions (0-11)
- `risk_category` - Risk tier based on conditions
- `has_diabetes`, `has_heart_failure`, etc. - Condition booleans

**Use Cases:**
- Patient risk stratification
- Health outcome analysis
- Demographic analysis
- High-utilizer identification

**Key Insights:**
- ~20% of patients have 5+ chronic conditions
- Chronic patients have 5-10x higher costs
- Fraud exposure correlates with patient risk profile

---

### dim_diagnosis_etl.sql

**Purpose:** Create diagnosis code dimension with medical context  
**Source:** Extracted from all ClmDiagnosisCode_* fields  
**Output:** One row per unique diagnosis code

**Record Count:** ~1,200 diagnosis codes  
**Execution Time:** ~1 minute

**Key Columns:**
- `diagnosis_sk` - Surrogate key
- `diagnosis_code` - ICD-9 code (e.g., '4019')
- `icd9_chapter` - Medical chapter (e.g., 'Circulatory System')
- `subcategory` - Specific condition (e.g., 'Diabetes Mellitus')
- `is_chronic` - Chronic vs acute indicator
- `risk_level` - Clinical risk (High/Standard/Low)
- `cost_category` - Expected cost tier

**ICD-9 Chapters:** 17 major categories (infections, neoplasms, mental disorders, etc.)

**Use Cases:**
- Diagnosis-specific analysis
- Chronic disease burden
- Medical cost prediction
- Fraud pattern detection (unusual diagnosis-procedure combos)

---

### dim_procedure_etl.sql

**Purpose:** Create procedure code dimension with medical context  
**Source:** Extracted from all ClmProcedureCode_* fields  
**Output:** One row per unique procedure code

**Record Count:** ~320 procedure codes  
**Execution Time:** ~0.5 minutes

**Key Columns:**
- `procedure_sk` - Surrogate key
- `procedure_code` - ICD-9 code (e.g., '3895')
- `procedure_category` - Type of procedure
- `is_major_procedure` - Major vs minor indicator
- `cost_category` - Expected cost (Very High/High/Medium/Low)

**Procedure Ranges:**
- 01-05: Nervous System (High cost)
- 35-39: **Cardiovascular** (Very High cost, fraud hotspot)
- 76-84: Musculoskeletal (High cost)
- 87+: Diagnostic (Low cost)

**Use Cases:**
- Procedure volume analysis
- Cost trend detection
- Fraud pattern identification
- Unusual procedure combinations

---

## Facts (03-facts/)

Facts contain **measurable events** (claims, services) and their associated costs. They are typically much larger than dimensions.

### fact_claims_etl.sql

**Purpose:** Main claims fact table at transaction level  
**Source:** `v_all_claims_etl` + all dimensions (joins)  
**Output:** One row per claim with all context

**Record Count:** 558,211 claims  
**Execution Time:** ~1 minute (depends on dimension completion)

**Key Columns (Surrogate Keys):**
- `claim_sk` - Unique claim key
- `patient_sk` - FK to patient dimension
- `provider_sk` - FK to provider dimension
- `claim_start_date_key` - FK to date dimension

**Key Columns (Measures):**
- `claim_amount` - Insurance reimbursement ($)
- `deductible_amount` - Patient deductible ($)
- `length_of_stay` - Days (inpatient only)
- `is_fraudulent` - Fraud flag

**Key Columns (Attributes):**
- `claim_type` - 'Inpatient' or 'Outpatient'
- `diagnosis_code_1` - Primary diagnosis
- `procedure_code_1` - Primary procedure
- `attending_physician_id` - Responsible physician

**Data Distribution:**
- **93% Outpatient** (~517K claims, avg $2K)
- **7% Inpatient** (~40K claims, avg $10K)
- **38% Fraudulent** (~212K claims)

**Usage:**
```sql
-- Top 10 highest-cost claims
SELECT TOP 10 * FROM fact_claims_etl 
ORDER BY claim_amount DESC;

-- Fraud by claim type
SELECT 
    claim_type,
    COUNT(*) as total_claims,
    SUM(CASE WHEN is_fraudulent THEN claim_amount ELSE 0 END) as fraud_amount
FROM fact_claims_etl
GROUP BY claim_type;
```

---

### fact_provider_summary_etl.sql

**Purpose:** Monthly provider aggregation for trending  
**Source:** `v_all_claims_etl` + dimensions, grouped by provider × month  
**Output:** One row per provider per month

**Record Count:** ~70K (5.4K providers × ~13 months average)  
**Execution Time:** ~0.5 minutes

**Key Columns:**
- `provider_sk` - FK to provider
- `month_key` - YYYYMM format
- `total_claims` - Claims in month
- `inpatient_claims`, `outpatient_claims` - Split
- `unique_patients` - Patients seen that month
- `avg_claim_amount` - Average claim value
- `fraud_exposure_amount` - Sum of fraudulent claims

**Use Cases:**
- Monthly provider performance tracking
- Fraud trends over time
- Provider capacity planning
- Seasonal patterns

**Usage:**
```sql
-- Provider fraud trend by month
SELECT 
    p.provider_id,
    fps.month_key,
    fps.total_claims,
    fps.fraud_exposure_amount
FROM fact_provider_summary_etl fps
JOIN dim_provider_etl p ON fps.provider_sk = p.provider_sk
WHERE p.is_fraudulent = TRUE
ORDER BY fps.month_key;
```

---

### fact_patient_claims_summary_etl.sql

**Purpose:** Monthly patient aggregation for utilization tracking  
**Source:** `v_all_claims_etl` + dimensions, grouped by patient × month  
**Output:** One row per patient per month

**Record Count:** ~500K (138K patients × varying months)  
**Execution Time:** ~0.5 minutes

**Key Columns:**
- `patient_sk` - FK to patient
- `month_key` - YYYYMM format
- `total_claims` - Claims that month
- `inpatient_visits`, `outpatient_visits` - Split
- `total_claimed` - Total claim amounts
- `fraudulent_provider_visits` - Visits to fraudulent providers
- `fraud_exposure_amount` - Amount from fraudulent providers

**Use Cases:**
- Patient healthcare utilization tracking
- High-utilizer identification
- Fraud exposure by patient
- Risk prediction models

**Usage:**
```sql
-- High utilizers (5+ claims per month)
SELECT 
    p.patient_id,
    pcs.month_key,
    pcs.total_claims,
    pcs.total_claimed
FROM fact_patient_claims_summary_etl pcs
JOIN dim_patient_etl p ON pcs.patient_sk = p.patient_sk
WHERE pcs.total_claims >= 5
ORDER BY pcs.total_claims DESC;
```

---

## Validation (04-validate/)

Validation queries verify data quality and completeness after ETL.

### validation_queries.sql

**Purpose:** QA checks to ensure successful data warehouse build  
**Execution Time:** ~0.5 minutes

**Checks Included:**

1. **Record Counts** - Verify expected row counts in each table
```sql
SELECT COUNT(*) FROM dim_date_etl;           -- Expect ~1,090
SELECT COUNT(*) FROM dim_provider_etl;       -- Expect ~5,410
SELECT COUNT(*) FROM dim_patient_etl;        -- Expect ~138,556
SELECT COUNT(*) FROM fact_claims_etl;        -- Expect ~558,211
```

2. **Referential Integrity** - Check for orphaned records
```sql
-- Patients in claims should exist in dimension
SELECT COUNT(*) FROM fact_claims_etl f
WHERE f.patient_sk NOT IN (SELECT patient_sk FROM dim_patient_etl);
-- Should return 0
```

3. **Data Distribution** - Verify expected patterns
```sql
-- Inpatient vs Outpatient split
SELECT 
    claim_type, 
    COUNT(*) as cnt,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
FROM fact_claims_etl
GROUP BY claim_type;
-- Expect: ~93% Outpatient, ~7% Inpatient
```

4. **Fraud Distribution** - Check fraud flag consistency
```sql
SELECT 
    is_fraudulent,
    COUNT(*) as cnt,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) as pct
FROM fact_claims_etl
GROUP BY is_fraudulent;
-- Expect: ~62% FALSE, ~38% TRUE
```

---

## Running Queries

### Option 1: Via AWS Lambda (Automated)

Your Lambda function automatically runs all queries in sequence:

```bash
# Deploy Lambda function
aws lambda update-function-code --function-name etl --zip-file fileb://function.zip

# Trigger full pipeline
aws lambda invoke --function-name etl response.json

# View response
cat response.json
```

---

### Option 2: Via AWS Athena Console (Manual)

1. **Open Athena Console** → [https://console.aws.amazon.com/athena/](https://console.aws.amazon.com/athena/)

2. **Create Views** (run in order):
```sql
-- Run 01-views in this order
COPY-PASTE v_providers_etl.sql
COPY-PASTE v_patients_etl.sql
COPY-PASTE v_inpatient_claims_etl.sql
COPY-PASTE v_outpatient_claims_etl.sql
COPY-PASTE v_all_claims_etl.sql
```

3. **Create Dimensions** (in any order, or parallel):
```sql
COPY-PASTE each file from 02-dims/ folder
```

4. **Create Facts** (after all dimensions complete):
```sql
COPY-PASTE each file from 03-facts/ folder
```

5. **Run Validation**:
```sql
COPY-PASTE validation_queries.sql
```

---

### Option 3: Via AWS CLI

Run query against Athena from command line:

```bash
# Run a single query
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM dim_patient_etl;" \
  --query-execution-context Database=insurance_claim_db \
  --result-configuration OutputLocation=s3://your-bucket/results/

# Get results
aws athena get-query-execution --query-execution-id <query-id>
```

---

## Performance Notes

### Execution Times

| Step | Component | Time | Notes |
|------|-----------|------|-------|
| 1 | Views | ~4 min | Serial execution (dependencies) |
| 2 | Dimensions | ~3 min | Can be parallel (independent) |
| 3 | Facts | ~2 min | Serial (depend on dims) |
| 4 | Validation | ~0.5 min | Queries only |
| **Total** | **Full Pipeline** | **~5-6 min** | Sequential execution |

### Cost Optimization

**Athena Costs:** $5 per TB of data scanned

**Strategies:**
- ✅ Parquet format (10:1 compression vs CSV)
- ✅ Partitioned by date (scan less data)
- ✅ External tables (don't store redundantly)
- ✅ Estimate: Full run costs ~$0.01-0.05

### Query Optimization Tips

1. **Use Column Projections** (don't SELECT *)
```sql
-- Good: Only columns needed
SELECT patient_sk, claim_amount, is_fraudulent FROM fact_claims_etl;

-- Expensive: All columns
SELECT * FROM fact_claims_etl;
```

2. **Use Date Filters** (pushdown predicates)
```sql
-- Good: Filter at query time
SELECT * FROM fact_claims_etl WHERE claim_start_date_key >= 20090101;

-- Expensive: No filter
SELECT * FROM fact_claims_etl;
```

3. **Use LIMIT** for exploration
```sql
-- Fast: Check structure
SELECT * FROM fact_claims_etl LIMIT 10;

-- Expensive: Full scan
SELECT * FROM fact_claims_etl;
```

---

## Troubleshooting

### Issue: Views Fail with "Unknown Table"

**Cause:** Raw base tables (`provider`, `beneficiary`, `inpatient`, `outpatient`) not found

**Solution:**
```sql
-- Verify base tables exist
SHOW TABLES IN insurance_claim_db;

-- If missing, create external tables from S3:
CREATE EXTERNAL TABLE provider (
    Provider VARCHAR,
    PotentialFraud VARCHAR
)
STORED AS CSV
LOCATION 's3://your-bucket/raw/provider/';
```

---

### Issue: Dimensions Fail with "View Does Not Exist"

**Cause:** Views weren't created yet

**Solution:** Run all views first (01-views/) before dimensions (02-dims/)

---

### Issue: Facts Fail with FK Errors

**Cause:** Dimensions weren't fully created

**Solution:**
```sql
-- Verify dimensions exist and have data
SELECT COUNT(*) FROM dim_patient_etl;      -- Should be 138,556
SELECT COUNT(*) FROM dim_provider_etl;     -- Should be 5,410
SELECT COUNT(*) FROM dim_date_etl;         -- Should be ~1,090
```

---

### Issue: Validation Queries Return Mismatched Counts

**Cause:** Possible data quality issue or join loss

**Solution:**
1. Rerun queries
2. Check for NULL values in join keys
```sql
-- Check for NULLs
SELECT COUNT(*) FROM fact_claims_etl WHERE patient_sk IS NULL;
SELECT COUNT(*) FROM fact_claims_etl WHERE provider_sk IS NULL;
```

3. Review data-dictionary.md for known issues

---

### Issue: Query Timeout (>5 minutes)

**Cause:** Large table scan without filtering

**Solution:**
```sql
-- Add WHERE clause to filter
SELECT * FROM fact_claims_etl 
WHERE claim_start_date_key >= 20090101  -- Filter by date
LIMIT 100;                               -- Limit results
```

---

## Quick Reference: Common Queries

### Count Records in Each Table
```sql
SELECT 'dim_date_etl' as table_name, COUNT(*) as cnt FROM dim_date_etl
UNION ALL
SELECT 'dim_provider_etl', COUNT(*) FROM dim_provider_etl
UNION ALL
SELECT 'dim_patient_etl', COUNT(*) FROM dim_patient_etl
UNION ALL
SELECT 'dim_diagnosis_etl', COUNT(*) FROM dim_diagnosis_etl
UNION ALL
SELECT 'dim_procedure_etl', COUNT(*) FROM dim_procedure_etl
UNION ALL
SELECT 'fact_claims_etl', COUNT(*) FROM fact_claims_etl
UNION ALL
SELECT 'fact_provider_summary_etl', COUNT(*) FROM fact_provider_summary_etl
UNION ALL
SELECT 'fact_patient_claims_summary_etl', COUNT(*) FROM fact_patient_claims_summary_etl;
```

### Fraud Summary
```sql
SELECT 
    is_fraudulent,
    COUNT(*) as claim_count,
    ROUND(SUM(claim_amount), 2) as total_amount,
    ROUND(AVG(claim_amount), 2) as avg_amount
FROM fact_claims_etl
GROUP BY is_fraudulent;
```

### Top 10 Fraudulent Providers
```sql
SELECT TOP 10
    p.provider_id,
    p.total_claims,
    ROUND(SUM(CASE WHEN fc.is_fraudulent THEN fc.claim_amount ELSE 0 END), 2) as fraud_exposure
FROM dim_provider_etl p
LEFT JOIN fact_claims_etl fc ON p.provider_sk = fc.provider_sk
WHERE p.is_fraudulent = TRUE
GROUP BY p.provider_id, p.total_claims
ORDER BY fraud_exposure DESC;
```

### Patient Risk Distribution
```sql
SELECT
    dp.risk_category,
    COUNT(DISTINCT dp.patient_sk) as patient_count,
    COUNT(*) as total_claims,
    ROUND(AVG(fc.claim_amount), 2) as avg_claim_amount
FROM dim_patient_etl dp
LEFT JOIN fact_claims_etl fc ON dp.patient_sk = fc.patient_sk
GROUP BY dp.risk_category
ORDER BY patient_count DESC;
```

---

## File Manifest

| File | Lines | Purpose | Rows Output | Time |
|------|-------|---------|------------|------|
| v_providers_etl.sql | 15 | Standardize fraud flags | 5.4K | 0.5m |
| v_patients_etl.sql | 45 | Decode demographics | 138K | 0.5m |
| v_inpatient_claims_etl.sql | 35 | Clean inpatient | 40K | 1m |
| v_outpatient_claims_etl.sql | 25 | Clean outpatient | 517K | 1m |
| v_all_claims_etl.sql | 20 | Union all claims | 558K | 1m |
| dim_date_etl.sql | 25 | Date dimension | 1K | 1m |
| dim_provider_etl.sql | 50 | Provider dimension | 5.4K | 1m |
| dim_patient_etl.sql | 40 | Patient dimension | 138K | 0.5m |
| dim_diagnosis_etl.sql | 80 | Diagnosis dimension | 1.2K | 1m |
| dim_procedure_etl.sql | 70 | Procedure dimension | 320 | 0.5m |
| fact_claims_etl.sql | 30 | Claims fact table | 558K | 1m |
| fact_provider_summary_etl.sql | 25 | Provider summary | 70K | 0.5m |
| fact_patient_claims_summary_etl.sql | 25 | Patient summary | 500K | 0.5m |
| validation_queries.sql | 20 | QA checks | N/A | 0.5m |

---

## Next Steps

1. ✅ **Copy all SQL files** to your `sql/` folder (organized by subdirectory)
2. ✅ **Run via Lambda** (automated) or **Athena Console** (manual)
3. ✅ **Validate** using `04-validate/validation_queries.sql`
4. 🔄 **Schedule** for monthly runs (use Lambda CloudWatch triggers)
5. 📊 **Connect** Tableau/QuickSight to Redshift for dashboards

---

## Resources

- **Athena Documentation:** [AWS Athena Docs](https://docs.aws.amazon.com/athena/)
- **SQL Reference:** [Presto SQL Functions](https://prestodb.io/docs/current/functions.html)
- **Data Dictionary:** See `docs/data-dictionary.md` for full schema
- **Architecture:** See `ARCHITECTURE.md` for system design

---

## Support

**Questions?**
1. Check `04-validate/validation_queries.sql` for data verification
2. Review `docs/data-dictionary.md` for column definitions
3. See "Troubleshooting" section above
4. Check Lambda logs for execution errors

---

**Last Updated:** 2025.12.31  
**Maintained By:** Qi An  
**Version:** 1.0  
**Status:** Production Ready ✅
