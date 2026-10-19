# Power BI Semantic Model Design

This document describes the semantic model (tables, relationships, DAX
measures) built on top of the star-schema CSV extracts produced by
[`data_prep.py`](./data_prep.py).

**The model is defined as code.** [`tmdl/build_tmdl.py`](./tmdl/build_tmdl.py)
generates [`tmdl/FraudModel.tmdl`](./tmdl/FraudModel.tmdl), a TMDL
`createOrReplace` script containing every table (with its Power Query M
import), relationship, DAX measure, and RLS role. Pasting it into Power BI
Desktop's TMDL view and clicking **Apply** builds the whole model in one step.
The script is the source of truth; this document explains the design.

All figures quoted below were computed directly from the extracts. They are
not placeholders.

---

## 1. Tables & Grain

| Table | Type | Grain | Row Count |
|-------|------|-------|-----------|
| `fact_claims` | Fact | One row per claim (inpatient + outpatient) | 558,211 |
| `fact_patient_claims_summary` | Fact | One row per patient × month | 445,765 |
| `dim_patient` | Dimension | One row per patient (`BeneID`) | 138,556 |
| `dim_provider` | Dimension | One row per provider | 5,410 |
| `dim_date` | Dimension (date table) | One row per calendar day, 2008-01-01 to 2009-12-31 | 731 |
| `dim_diagnosis` | Dimension | One row per distinct ICD-9 diagnosis code | 11,040 |
| `dim_procedure` | Dimension | One row per distinct ICD-9 procedure code | 1,324 |

**Design decisions:**
- **`dim_date` is a contiguous calendar.** The Athena `dim_date` only keeps
  dates that appear in claims (398 rows with gaps). Power BI's time
  intelligence functions need a gap-free date table, so `data_prep.py` builds
  every day of each claim year and the table is marked as a date table
  (`dataCategory: Time`, key `full_date`).
- **`fact_provider_summary` is left out of the Power BI model.** It has no
  path to `dim_patient`, so the state-based RLS roles could not filter it and
  it would leak every state's totals to a restricted analyst. Provider-level
  numbers are computed from `fact_claims` through measures instead, which RLS
  does filter.
- **Code columns are text.** ICD-9 diagnosis and procedure codes keep leading
  zeros and include alphanumeric `V`/`E` codes, so both sides of those
  relationships are typed as text in Power Query.
- **Measures live on `fact_claims`**, grouped into display folders (Core,
  Fraud Exposure, Provider Risk, Trend), and implicit aggregation is kept to
  the amount columns.

---

## 2. Relationships

| From (many) | To (one) | Active | Notes |
|-------------|----------|--------|-------|
| `fact_claims[patient_sk]` | `dim_patient[patient_sk]` | Yes | RLS filters flow through this relationship |
| `fact_claims[provider_sk]` | `dim_provider[provider_sk]` | Yes | |
| `fact_claims[claim_start_date_key]` | `dim_date[date_key]` | Yes | Primary date for all time intelligence |
| `fact_claims[claim_end_date_key]` | `dim_date[date_key]` | No | Use `USERELATIONSHIP()` |
| `fact_claims[admission_date_key]` | `dim_date[date_key]` | No | Inpatient only (0 for outpatient) |
| `fact_claims[discharge_date_key]` | `dim_date[date_key]` | No | Inpatient only |
| `fact_claims[diagnosis_code_1]` | `dim_diagnosis[diagnosis_code]` | Yes | Primary diagnosis only; 98.1% of claims match, the rest have no primary code |
| `fact_claims[procedure_code_1]` | `dim_procedure[procedure_code]` | Yes | Most claims have no procedure (blank), which is expected |
| `fact_patient_claims_summary[patient_sk]` | `dim_patient[patient_sk]` | Yes | |

All relationships are many-to-one and filter in a single direction
(dimension to fact), consistent with Kimball dimensional modeling and required
for the RLS design in [`RLS_DESIGN.md`](./RLS_DESIGN.md).

---

## 3. DAX Measures

Full definitions are in [`tmdl/FraudModel.tmdl`](./tmdl/FraudModel.tmdl).
Expected values are for the full dataset with no filters applied.

| Folder | Measure | Logic | Expected value |
|--------|---------|-------|----------------|
| Core | `Total Claims` | `COUNTROWS(fact_claims)` | 558,211 |
| Core | `Total Claim Amount $` | Sum of reimbursed amount | $556,543,140 |
| Core | `Avg Claim Amount` | Average reimbursed amount per claim | $997 |
| Core | `Inpatient Share %` | Inpatient claims / all claims | 7.3% |
| Fraud Exposure | `Fraud Claim Count` | Claims filed with a provider flagged as potentially fraudulent | 212,796 |
| Fraud Exposure | `Fraud Rate %` | Fraud claim count / total claims (by count) | 38.1% |
| Fraud Exposure | `Total Fraud Exposure $` | Reimbursed dollars on claims from flagged providers | $295,681,120 |
| Fraud Exposure | `Fraud Exposure % of Total $` | Fraud exposure / all reimbursed dollars (by value) | 53.1% |
| Fraud Exposure | `Fraud Rate Lift (x)` | Fraud rate in the current diagnosis/procedure slice ÷ the rate with those filters removed | 1.00 overall; about 1.6 for the highest-risk diagnosis/procedure pairs |
| Provider Risk | `Distinct Fraudulent Providers` | Distinct flagged providers with claims in the current filter context | 506 |
| Provider Risk | `Fraud Exposure per Fraudulent Provider $` | Exposure ÷ distinct flagged providers | $584,350 |
| Provider Risk | `Top 50 Provider Fraud Share %` | Share of fraud exposure held by the 50 highest-exposure providers (`TOPN` over `ADDCOLUMNS`) | 35.3% |
| Trend | `Claim Amount Prior Month $` | `DATEADD(dim_date[full_date], -1, MONTH)` | |
| Trend | `Claim Amount MoM %` | Month-over-month change in reimbursed amount | |
| Trend | `Claim Count QoQ %` | Quarter-over-quarter change in claim volume | |

**Why two fraud percentages:** 38.1% of claims (by count) come from flagged
providers, but those claims carry 53.1% of reimbursed dollars, because flagged
providers bill a higher share of expensive inpatient claims. The dashboard
shows both so the headline is not misread.

**No year-over-year measures:** the claims cover roughly 13 months
(late Nov 2008 to Dec 2009), so a prior-year comparison would be empty for
almost every period. Month-over-month and quarter-over-quarter trends are used
instead.

---

## 4. Notes on `state_code`

`dim_patient[state_code]` is a raw numeric SSA/CMS state code inherited from
the source dataset (see [`docs/data-dictionary.md`](../docs/data-dictionary.md)).
No verified code-to-name mapping is included in the ETL, so the model keeps
the code numeric and does not plot it on a map. It is still a valid key for
row-level security, which is how it is used in
[`RLS_DESIGN.md`](./RLS_DESIGN.md).
