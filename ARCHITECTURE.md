# Architecture — Medical Claims Analytics ETL Pipeline on AWS

## 1. Overview

This project builds an end-to-end ETL pipeline for healthcare insurance
claims data and loads it into a star-schema data warehouse for analytics
(fraud risk, provider behavior, patient risk). The pipeline is implemented
entirely on AWS, serverless, using:

- **Amazon S3** — raw data storage and warehouse output (Parquet)
- **Amazon Athena** — SQL-based transformations and `CREATE TABLE AS SELECT`
  (CTAS) table creation
- **AWS Lambda** (Python) — orchestrates every ETL step
- **Amazon EventBridge** — schedules the pipeline
- **Amazon CloudWatch Logs + IAM** — monitoring and permissions

The final warehouse consists of:

- **5 dimension tables**: `dim_date_etl`, `dim_patient_etl`,
  `dim_provider_etl`, `dim_diagnosis_etl`, `dim_procedure_etl`
- **3 fact tables**: `fact_claims_etl`, `fact_provider_summary_etl`,
  `fact_patient_claims_summary_etl`

All tables are stored in S3 under the `lambda_etl` prefix in Parquet format
and queried through Athena in the `insurance_claim_db` database.

---

## 2. Data Sources

The pipeline integrates four raw CSV files stored in S3 in the
`insurance-claim-qian-2025` bucket:

| Source | Example file | Contents |
|--------|-------------|----------|
| Beneficiary | `data/raw/Train_Beneficiarydata.csv` | Member demographics, race, chronic condition flags, coverage months, annual reimbursement/deductible amounts |
| Provider | `data/raw/Train_Provider.csv` | Provider IDs and a potential-fraud flag |
| Inpatient claims | `data/raw/Train_Inpatient.csv` | Claim IDs, beneficiary IDs, providers, dates (admission, discharge, claim start/end), diagnosis codes, procedure codes, amounts |
| Outpatient claims | `data/raw/Train_Outpatient.csv` | Same as inpatient, without length-of-stay fields |

These files are loaded into base Athena tables (`beneficiary`, `provider`,
`inpatient`, `outpatient`), which are the starting point for every
transformation below.

---

## 3. Architecture & Tools

### 3.1 High-level layers

```
S3 (CSV) → Athena base tables → _etl views → dim/fact CTAS (Parquet in S3) → Athena analytics / dashboards
                                                    ▲
                                        Lambda + EventBridge (orchestration)
```

1. **Raw layer** (S3 + base Athena tables) — raw CSVs live in S3; Athena
   external tables (`beneficiary`, `provider`, `inpatient`, `outpatient`)
   point directly at those CSVs.
2. **Transformation layer** (Athena views) — business-friendly views with
   an `_etl` suffix clean and standardize raw data: `v_providers_etl`,
   `v_patients_etl`, `v_inpatient_claims_etl`, `v_outpatient_claims_etl`,
   `v_all_claims_etl` (inpatient + outpatient combined).
3. **Warehouse layer** (Athena CTAS → S3 Parquet) — Lambda executes
   `CREATE TABLE ... WITH (format='PARQUET', external_location='s3://.../lambda_etl/...') AS SELECT ...`
   to generate the 5 dimension and 3 fact tables listed in §1.
4. **Orchestration & scheduling** — a Lambda function (`lambda_handler`)
   orchestrates each step (views, dims, facts, validate, or all); an
   EventBridge rule triggers the Lambda on a schedule with event payload
   `{"step": "all"}`.
5. **Monitoring** — CloudWatch Logs capture detailed logs for each Lambda
   run and each Athena query execution.

### 3.2 Programming choices

Two technologies do the work, each for what it's best at:

- **Python (AWS Lambda)** — `lambda_handler(event, context)` uses the
  `boto3` Athena client to start queries and poll for completion, and
  implements step-based orchestration via a `"step"` parameter in the
  event (imperative control flow).
- **SQL (Athena / Presto dialect)** — defines every view, transformation,
  and CTAS statement, using casting, `CASE` expressions, date functions
  (`TRY_CAST`, `DATE_DIFF`, `DATE_FORMAT`, `DAY_OF_WEEK`, …), and window
  functions (`ROW_NUMBER()`) (declarative data logic).

---

## 4. Detailed Pipeline Steps

### 4.1 Lambda orchestration design

The Lambda function accepts an event with a `"step"` value, e.g.
`{ "step": "views" }`. Valid steps:

| Step | Behavior |
|------|----------|
| `raw` | Placeholder — raw tables are already created in Athena |
| `views` | (Re)creates all `_etl` views |
| `dims` | Builds all five dimension tables |
| `facts` | Builds all three fact tables |
| `validate` | Runs `COUNT(*)` queries on all dims and facts |
| `all` | Executes `raw → views → dims → facts → validate` end-to-end |

Internally, a helper `execute_athena_query(query, database, label)`:
calls `start_query_execution` on Athena, polls `get_query_execution` until
the query is `SUCCEEDED` or `FAILED`, and logs all SQL and errors to
CloudWatch — making the pipeline easy to debug and re-run step by step.

### 4.2 Transformation views (`*_etl`)

| View | Input | What it does |
|------|-------|---------------|
| `v_providers_etl` | `provider` | Cleans the fraud flag into a boolean `is_fraudulent` (treats `"Yes"`, `"Y"`, `"1"` as true); outputs `provider_id`, `is_fraudulent`, `raw_fraud_value` |
| `v_patients_etl` | `beneficiary` | Cleans dates (`DOB`, `DOD`, `'NA'` → `NULL`); maps gender/race codes to descriptive text; converts chronic-condition columns to booleans; computes `chronic_condition_count`; derives `is_deceased` |
| `v_inpatient_claims_etl` | `inpatient` | Casts date strings to `DATE`; computes `length_of_stay` via `DATE_DIFF`; converts claim/deductible amounts to `DOUBLE`; carries diagnosis/procedure codes; sets `claim_type = 'Inpatient'` |
| `v_outpatient_claims_etl` | `outpatient` | Same cleaning as inpatient, without admission/discharge dates; sets `claim_type = 'Outpatient'` |
| `v_all_claims_etl` | inpatient + outpatient | `UNION ALL` into one schema; for outpatient rows, `admission_date`, `discharge_date`, `length_of_stay` are normalized to `NULL`/`0` |

These views form a transform layer that separates raw complexity from the
star-schema definitions built on top of them.

### 4.3 Dimension tables

All dimension tables are created with CTAS and stored as Parquet under
`data/warehouse/lambda_etl/dim_tables/…`.

- **`dim_date_etl`** — built from all unique dates present in
  `v_all_claims_etl` (start, end, admission, discharge). Attributes:
  `date_key` (`YYYYMMDD` int), `full_date`, `year`, `quarter`, `month`,
  `day_of_month`, `day_of_week`, `day_name`, `month_name`, `is_weekend`,
  `year_month`. Shared time dimension for all fact tables.
- **`dim_patient_etl`** — from `v_patients_etl`; adds a surrogate key
  `patient_sk` via `ROW_NUMBER()`. Attributes: demographics (gender,
  race), geography (`state_code`, `county_code`), coverage, chronic
  condition flags, `chronic_condition_count`, `is_deceased`, and a derived
  `risk_category` (Low / Medium / High / Very High).
- **`dim_provider_etl`** — from `v_providers_etl` and `v_all_claims_etl`;
  uses a CTE (`provider_stats`) to compute aggregate claim statistics per
  provider (total claims, total patients, total amount, avg/max/min claim
  amount, inpatient vs. outpatient counts, average length of stay). Adds
  derived `provider_type` (Hospital Only / Clinic Only / Mixed / etc.),
  `risk_level` (Confirmed Fraud / High / Medium / Low, based on the fraud
  flag and outlier claim amounts), and `activity_level` (Very High Volume
  / High / Medium / Low / Inactive).
- **`dim_diagnosis_etl`** — collects all diagnosis and admit codes from
  both inpatient and outpatient views; cleans codes (removes quotes,
  invalid values). Derives `code_category` (first 3 characters),
  `icd9_chapter` (mapped from ICD-9 ranges), `subcategory` (e.g.
  "Diabetes Mellitus", "Heart Failure"), `is_chronic`, `risk_level`,
  `is_common_code`, `cost_category`.
- **`dim_procedure_etl`** — collects all procedure codes from inpatient
  and outpatient views; cleans and validates codes, keeping only numeric
  ICD-9 procedure codes. Attributes: `procedure_code`, `code_category`
  (first 2 digits), `procedure_category`, `is_major_procedure`,
  `risk_level`, `cost_category`.

### 4.4 Fact tables

All fact tables are stored under `data/warehouse/lambda_etl/fact_tables/…`.

- **`fact_claims_etl`** — the main transaction fact. Source:
  `v_all_claims_etl` joined to `dim_patient_etl` and `dim_provider_etl`.
  Keys: `claim_sk` (surrogate), `patient_sk`, `provider_sk`, date keys for
  start/end/admission/discharge (`YYYYMMDD` ints). Measures:
  `claim_amount`, `deductible_amount`, `length_of_stay`, `total_amount`.
  Also carries `claim_type`, `is_fraudulent` (from provider), diagnosis
  and procedure codes, and physician IDs.
- **`fact_provider_summary_etl`** — provider-month summary. Grouped by
  `provider_sk` and `month_key` (`YYYYMM`). Metrics: total claims,
  inpatient vs. outpatient counts, unique patients, `total_claimed`,
  `avg_claim_amount`, `fraud_exposure_amount` (sum of claim amounts for
  fraudulent providers).
- **`fact_patient_claims_summary_etl`** — patient-month summary. Grouped
  by `patient_sk` and `month_key`. Metrics: total claims, inpatient vs.
  outpatient visits, total claimed and deductible amounts, fraudulent
  provider visits and exposure amount. Includes patient risk attributes
  to support cohort analysis.

---

## 5. Scheduling with EventBridge

An Amazon EventBridge rule automates the pipeline:

- **Schedule**: e.g. `rate(7 day)` (runs once per week)
- **Target**: the ETL Lambda function
- **Input**: constant JSON `{ "step": "all" }`
- **Role**: `LabRole`

Each run appears in CloudWatch Logs with the requested step and detailed
query logs.

---

## 6. Validation & Monitoring

### 6.1 Validation queries

The `validate` step runs a series of `COUNT(*)` queries on every dim and
fact table, e.g.:

```sql
SELECT 'dim_patient_etl' AS table_name, COUNT(*) AS cnt FROM dim_patient_etl;
SELECT 'fact_claims_etl' AS table_name, COUNT(*) AS cnt FROM fact_claims_etl;
```

These checks confirm each table populated successfully and quickly flag
any empty or unexpectedly small table.

### 6.2 Monitoring

CloudWatch Logs capture: which ETL step was requested, every major action
(dropping/creating views, dims, facts), and success/failure for each
Athena query with error messages on failure. This logging made it
possible to debug early issues (e.g. empty tables, incorrect S3 locations)
and verify the pipeline runs end-to-end without errors.
