# 🚨 Fraud Patterns Analysis - Medical Insurance Claims

> **Revision note:** an earlier version of this document contained several
> figures (geographic breakdown by state, claim-submission-timing
> patterns, specific diagnosis/procedure combination frequencies, and
> business-impact/ROI projections) that could not be reproduced against
> the actual dataset — some depended on fields the source data doesn't
> contain (e.g. a claim-submission timestamp, or a patient state-name
> field), others were simply inconsistent with a direct recomputation.
> This version keeps only figures that were freshly recomputed and
> verified directly from the star-schema extract in
> [`powerbi/extracts/`](../powerbi/extracts/) (reproducible via
> [`powerbi/data_prep.py`](../powerbi/data_prep.py)).

## Executive Summary

This analysis identifies **$295.68 million in fraud exposure** across
558,211 Medicare claims, representing **38.12% of total claims**.
Fraudulent claims are concentrated among a small number of providers: the
top 50 fraud-exposure providers (0.92% of all 5,410 providers) account for
**35.3% of total fraud exposure**.

### Key Metrics

| Metric | Value |
|--------|-------|
| **Total Claims Analyzed** | 558,211 |
| **Fraudulent Claims** | 212,796 (38.12% of total) |
| **Fraud Exposure** | $295.68M |
| **Fraudulent Providers** | 506 (9.35% of 5,410 total providers) |
| **Top 50 Providers' Share of Fraud** | 35.3% |
| **Avg Fraudulent Claim** | $1,389.51 |
| **Avg Legitimate Claim** | $755.21 |

---

## 1. Fraud by Claim Type

| Claim Type | Total Claims | Fraudulent | Fraud Rate | Total Claimed | Fraud Exposure |
|------------|--------------|-----------|------------|----------------|------------------|
| **Inpatient** | 40,474 | 23,402 | **57.8%** | $408.3M | $241.3M |
| **Outpatient** | 517,737 | 189,394 | **36.6%** | $148.2M | $54.4M |
| **Total** | 558,211 | 212,796 | 38.1% | $556.5M | $295.7M |

**Key Insight:** Inpatient claims have a substantially higher fraud rate
(57.8%) than outpatient claims (36.6%) — the opposite of what a naive
"outpatient fraud is easier" assumption might suggest, and worth flagging
for provider-type-specific review policies.

---

## 2. Provider-Level Fraud Analysis

### 2.1 Fraud Concentration

**Total Providers:** 5,410
**Fraudulent Providers:** 506 (9.35%)

| Provider Group | Count | % of Providers | Fraud Exposure | % of Total Fraud |
|-----------------|-------|------------------|-----------------|--------------------|
| Top 10 | 10 | 0.18% | $33.7M | 11.4% |
| Top 20 | 20 | 0.37% | $55.1M | 18.6% |
| Top 50 | 50 | 0.92% | $104.3M | 35.3% |
| Top 100 | 100 | 1.85% | $160.5M | 54.3% |
| All fraudulent providers | 506 | 9.35% | $295.7M | 100% |

**Critical Finding:** just 100 providers (1.85% of all providers) account
for over half of total fraud exposure — fraud risk is heavily concentrated
rather than evenly distributed.

### 2.2 Top 20 Fraudulent Providers (by fraud exposure)

| Rank | Provider ID | Fraudulent Claims | Fraud Exposure | Avg Claim Amount |
|------|------------|---------------------|-----------------|--------------------|
| 1 | PRV52019 | 1,961 | $5,996,050 | $3,057.65 |
| 2 | PRV55462 | 1,907 | $4,713,830 | $2,471.86 |
| 3 | PRV56560 | 2,313 | $3,212,000 | $1,388.67 |
| 4 | PRV54367 | 636 | $3,133,880 | $4,927.48 |
| 5 | PRV54742 | 1,892 | $2,969,530 | $1,569.52 |

*(Verified directly from `fact_claims` + `dim_provider` — see
`powerbi/data_prep.py`.)*

### 2.3 Fraud Rate by Provider Type

| Provider Type | # Providers | Fraud Rate |
|----------------|--------------|------------|
| Clinic Only | 3,318 | 2.0% |
| Hospital Only | 398 | 11.1% |
| Mixed | 27 | 11.1% |
| Primarily Clinic | 1,478 | 20.8% |
| Primarily Hospital | 189 | 45.0% |

**Key Insight:** providers that are *primarily* hospital-based (but also
see some outpatient volume) have by far the highest fraud rate (45.0%) —
much higher than either pure "Hospital Only" or pure "Clinic Only"
providers. This is a genuinely interesting, verified pattern worth
investigating further.

---

## 3. Claim Amount Distribution

| Metric | Fraudulent | Legitimate |
|--------|-----------|-------------|
| Mean | $1,389.51 | $755.21 |
| Median | $90.00 | $80.00 |
| 95th Percentile | $8,000.00 | $3,300.00 |
| 99th Percentile | $22,000.00 | $13,000.00 |

**Interpretation:** fraudulent claims are higher on average and at every
percentile, but the *median* difference is small ($90 vs. $80) — the mean
is pulled up by a heavier tail of large fraudulent claims (visible in the
95th/99th percentile gap), not by every fraudulent claim being larger.

---

## 4. Diagnosis-Procedure Combinations

Restricting to diagnosis/procedure pairs with at least 100 claims, the
highest fraud-rate combinations are:

| Diagnosis Code | Procedure Code | Claims (n) | Fraud Rate |
|-----------------|------------------|------------|------------|
| 41401 | 3722 | 177 | 64.4% |
| 71536 | 8154 | 422 | 61.4% |
| 82021 | 7935 | 137 | 59.9% |
| 41401 | 66 | 443 | 59.8% |
| 71536 | 8151 | 186 | 59.7% |

Versus the 38.12% dataset-wide baseline, this is roughly a **1.6x**
enrichment — a real but modest effect, not an order-of-magnitude anomaly.

---

## 5. Seasonal Pattern

| Quarter | Total Claims | Fraud Rate |
|---------|--------------|------------|
| Q1 | 144,007 | 38.14% |
| Q2 | 144,743 | 38.22% |
| Q3 | 140,458 | 38.15% |
| Q4 | 129,003 | 37.95% |

**Finding:** fraud rate is flat across all four quarters (37.95%-38.22%).
**This dataset shows no seasonal fraud pattern.**

---

## 6. What Was Removed From This Analysis, and Why

The following sections appeared in an earlier draft and have been
**removed** because they could not be verified against the actual data:

- **Geographic breakdown by state** (e.g. "FL", "TX", "NY") — the dataset
  only contains a numeric SSA/CMS `state_code` on `dim_patient`, with no
  name mapping in the ETL (see the "State/County Codes" note in
  [`data-dictionary.md`](./data-dictionary.md)). Any state-name-level
  breakdown is not currently derivable from this pipeline.
- **Claim-submission-timing patterns** (same-day submission rate, weekend
  submissions, bulk-submission detection) — the source data has no
  submission-timestamp field distinct from `claim_start_date`/
  `claim_end_date`; there's nothing in the schema to compute this from.
- **Business-impact/ROI projections** (investigation cost-benefit,
  patient-impact counts) — these would require external cost data and
  patient-outcome data that isn't part of this dataset.

If any of these analyses are added in the future, they should cite the
specific query used to produce each number.

---

## Appendix: Supporting Query

```sql
-- Top fraud-exposure providers (recomputed and verified against powerbi/extracts/)
SELECT
    p.provider_id,
    COUNT(fc.claim_sk) AS fraudulent_claims,
    ROUND(SUM(fc.claim_amount), 2) AS fraud_exposure,
    ROUND(AVG(fc.claim_amount), 2) AS avg_claim_amount
FROM dim_provider_etl p
JOIN fact_claims_etl fc ON p.provider_sk = fc.provider_sk
WHERE fc.is_fraudulent = TRUE
GROUP BY p.provider_id
ORDER BY fraud_exposure DESC
LIMIT 20;
```

---

**Data Period:** 2008-2010
**Source:** [`powerbi/extracts/`](../powerbi/extracts/), generated by
[`powerbi/data_prep.py`](../powerbi/data_prep.py) from the raw Kaggle CSVs
