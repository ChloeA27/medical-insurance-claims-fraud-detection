# Power BI Semantic Model Design

This document specifies the semantic model (relationships + DAX measures) built
on top of the star-schema CSV extracts produced by [`data_prep.py`](./data_prep.py).
Import the eight CSVs in `powerbi/extracts/` into Power BI Desktop (**Get Data →
Text/CSV**) and configure the model as described below.

All figures quoted here as examples were computed directly from the extracts
(see the row counts in the "Grain & Row Counts" table) — they are not
placeholders.

---

## 1. Tables & Grain

| Table | Type | Grain | Row Count |
|-------|------|-------|-----------|
| `fact_claims` | Fact | One row per claim (inpatient + outpatient) | 558,211 |
| `fact_provider_summary` | Fact | One row per provider × month | 50,997 |
| `fact_patient_claims_summary` | Fact | One row per patient × month | 445,765 |
| `dim_patient` | Dimension | One row per patient (`BeneID`) | 138,556 |
| `dim_provider` | Dimension | One row per provider | 5,410 |
| `dim_date` | Dimension | One row per calendar date seen in claims | 398 |
| `dim_diagnosis` | Dimension | One row per distinct ICD-9 diagnosis code | 11,040 |
| `dim_procedure` | Dimension | One row per distinct ICD-9 procedure code | 1,324 |

---

## 2. Relationships

| From (many) | To (one) | Key | Cardinality | Cross-filter | Notes |
|--------------|----------|-----|-------------|---------------|-------|
| `fact_claims[patient_sk]` | `dim_patient[patient_sk]` | `patient_sk` | Many-to-one | Single | Active |
| `fact_claims[provider_sk]` | `dim_provider[provider_sk]` | `provider_sk` | Many-to-one | Single | Active |
| `fact_claims[claim_start_date_key]` | `dim_date[date_key]` | `date_key` | Many-to-one | Single | **Active** — primary date relationship for time intelligence |
| `fact_claims[claim_end_date_key]` | `dim_date[date_key]` | `date_key` | Many-to-one | Single | Inactive — use `USERELATIONSHIP()` |
| `fact_claims[admission_date_key]` | `dim_date[date_key]` | `date_key` | Many-to-one | Single | Inactive — inpatient claims only (0 for outpatient) |
| `fact_claims[discharge_date_key]` | `dim_date[date_key]` | `date_key` | Many-to-one | Single | Inactive — inpatient claims only |
| `fact_claims[diagnosis_code_1]` | `dim_diagnosis[diagnosis_code]` | text | Many-to-one | Single | Only the primary diagnosis is modeled as a relationship; codes 2–10 are attribute columns on `fact_claims`, not separately joined (matches the source ETL's `diagnosis_code_1`-only join in `fact_claims_etl.sql`) |
| `fact_claims[procedure_code_1]` | `dim_procedure[procedure_code]` | text | Many-to-one | Single | Many claims have a blank `procedure_code_1` (no procedure performed) — expect unmatched rows, this is expected |
| `fact_provider_summary[provider_sk]` | `dim_provider[provider_sk]` | `provider_sk` | Many-to-one | Single | Active |
| `fact_patient_claims_summary[patient_sk]` | `dim_patient[patient_sk]` | `patient_sk` | Many-to-one | Single | Active |

**Not related to `dim_date`:** `fact_provider_summary[month_key]` and
`fact_patient_claims_summary[month_key]` are `YYYYMM` strings (monthly grain),
a different grain from `dim_date[date_key]` (daily grain). No relationship is
defined between them — trend measures at monthly grain use `fact_claims` +
`dim_date` directly instead (see §4).

**Star schema, single direction:** all relationships filter from dimension →
fact only (no bidirectional cross-filtering), consistent with Kimball
dimensional modeling and required for the RLS design in
[`RLS_DESIGN.md`](./RLS_DESIGN.md) to filter correctly through `fact_claims`.

---

## 3. Model Diagram (logical)

```
                      ┌───────────────┐
                      │  dim_date     │
                      └───────┬───────┘
                              │ date_key (active: claim_start_date_key)
                              │
┌───────────────┐     ┌───────▼────────┐     ┌───────────────┐
│  dim_patient  │────▶│  fact_claims   │◀────│  dim_provider │
└───────┬───────┘     └───────┬────────┘     └───────┬───────┘
        │                     │                        │
        │             ┌───────┴────────┐               │
        │             │                │               │
        ▼        ┌────▼─────┐    ┌─────▼──────┐         ▼
fact_patient_    │dim_diag- │    │dim_procedure│  fact_provider_
claims_summary   │  nosis   │    │            │  summary
                  └──────────┘    └────────────┘
```

---

## 4. DAX Measures

All measures below are written against the table/column names produced by
`data_prep.py`. Create a dedicated **Measures** table in Power BI to host
these rather than scattering them across the fact tables.

### Fraud exposure

```dax
Fraud Claim Count :=
CALCULATE(
    COUNTROWS(fact_claims),
    fact_claims[is_fraudulent] = TRUE()
)
```
> Number of claims filed with a provider flagged as potentially fraudulent.

```dax
Fraud Rate % :=
DIVIDE([Fraud Claim Count], COUNTROWS(fact_claims))
```
> Share of all claims tied to a flagged provider (≈ 38.1% in the current extract).

```dax
Total Fraud Exposure $ :=
CALCULATE(
    SUM(fact_claims[claim_amount]),
    fact_claims[is_fraudulent] = TRUE()
)
```
> Total reimbursed dollar amount on claims from flagged providers (≈ $295.7M in the current extract).

```dax
Fraud Exposure % of Total :=
DIVIDE([Total Fraud Exposure $], SUM(fact_claims[claim_amount]))
```
> Fraud exposure as a share of every dollar reimbursed across the dataset.

### Provider risk concentration

```dax
Distinct Fraudulent Providers :=
CALCULATE(
    DISTINCTCOUNT(dim_provider[provider_id]),
    dim_provider[is_fraudulent] = TRUE()
)
```
> Count of unique providers flagged as potentially fraudulent.

```dax
Top 50 Provider Fraud Share % :=
VAR ProviderFraud =
    SUMMARIZE(
        FILTER(fact_claims, fact_claims[is_fraudulent] = TRUE()),
        fact_claims[provider_sk],
        "FraudAmt", SUM(fact_claims[claim_amount])
    )
VAR Top50 =
    TOPN(50, ProviderFraud, [FraudAmt], DESC)
VAR Top50Total =
    SUMX(Top50, [FraudAmt])
VAR AllFraudTotal =
    SUMX(ProviderFraud, [FraudAmt])
RETURN
    DIVIDE(Top50Total, AllFraudTotal)
```
> Share of total fraud exposure attributable to the 50 highest fraud-exposure providers — a risk-concentration indicator (≈ 35% in the current extract).

### Trend measures

```dax
Claim Amount PY :=
CALCULATE(
    SUM(fact_claims[claim_amount]),
    SAMEPERIODLASTYEAR(dim_date[full_date])
)
```
> Prior-year total claim amount, for year-over-year comparison.

```dax
Claim Amount YoY % :=
DIVIDE(
    SUM(fact_claims[claim_amount]) - [Claim Amount PY],
    [Claim Amount PY]
)
```
> Year-over-year growth in total reimbursed claim amount.

```dax
Claim Count QoQ % :=
VAR CurrentQ = COUNTROWS(fact_claims)
VAR PriorQ =
    CALCULATE(
        COUNTROWS(fact_claims),
        DATEADD(dim_date[full_date], -1, QUARTER)
    )
RETURN
    DIVIDE(CurrentQ - PriorQ, PriorQ)
```
> Quarter-over-quarter change in claim volume.

### Geographic risk

```dax
State Fraud Density $ :=
CALCULATE(
    SUM(fact_claims[claim_amount]),
    fact_claims[is_fraudulent] = TRUE()
)
```
> Fraud exposure dollars, sliced by `dim_patient[state_code]` on a map/matrix visual — surfaces geographic concentration (top states in the current extract cluster around state codes 5, 10, 33, 36, 45).

### Provider volume

```dax
Avg Claim Amount :=
AVERAGE(fact_claims[claim_amount])
```
> Average reimbursed amount per claim, useful as a provider-level outlier signal (compared against `dim_provider[risk_level]`, itself derived from ± std-dev bands around this same average).

```dax
Inpatient Share % :=
DIVIDE(
    CALCULATE(COUNTROWS(fact_claims), fact_claims[claim_type] = "Inpatient"),
    COUNTROWS(fact_claims)
)
```
> Share of claims that are inpatient (vs. outpatient) — inpatient claims carry materially higher average amounts.

---

## 5. Notes on `state_code`

`dim_patient[state_code]` is a raw numeric SSA/CMS state code inherited
directly from the source dataset (see
[`docs/data-dictionary.md`](../docs/data-dictionary.md), "State/County
Codes" data-quality note — a human-readable state-name mapping was
explicitly called out there as **not included** in the current ETL). The
Power BI layer keeps `state_code` numeric for the same reason rather than
guessing at a mapping; a state name/abbreviation lookup table can be added
later as a proper dimension if one is sourced and verified.
