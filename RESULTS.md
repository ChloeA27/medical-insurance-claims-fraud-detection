# Results: Medical Insurance Claims Fraud Detection

All figures below are computed directly from the star-schema extract
(`powerbi/extracts/`), reproducible via
[`powerbi/data_prep.py`](./powerbi/data_prep.py), and match the DAX measures
in the Power BI report. For removed or retracted figures from earlier
drafts, see [`docs/fraud-patterns.md`](./docs/fraud-patterns.md).

## Headline Numbers

| Metric | Value |
|--------|-------|
| Claims analyzed | 558,211 |
| Claims from flagged providers | 212,796 (38.1% of claims) |
| Total fraud exposure | $295.68M (53.1% of reimbursed dollars) |
| Flagged providers | 506 of 5,410 (9.35%) |
| Top-50-provider concentration | 35.3% of total fraud exposure |

## Findings

A payer's investigators can only review a limited number of providers, so the
practical question this analysis answers is **where to look first, and which
signals are worth monitoring**. Every number below is computed from the
star-schema extracts and matches the Power BI measures.

### 1. A short, ranked review list covers most of the exposure
Fraud exposure is concentrated in a small number of providers:

| Providers reviewed (ranked by exposure) | Share of total fraud exposure |
|---|---:|
| Top 10 | 11.4% |
| Top 50 | 35.3% |
| Top 87 (17% of flagged, 1.6% of all providers) | 50% |
| Top 100 | 54.3% |
| Top 221 (44% of flagged) | 80% |

The single highest-exposure provider accounts for $6.0M. A review queue ranked
by exposure, rather than by claim count or alphabetically, recovers half of
the exposure after fewer than 90 provider reviews.

### 2. The dollar gap comes from case mix, not from inflated prices
Flagged providers file **38.1% of claims but 53.1% of reimbursed dollars**.
Breaking that gap down shows where it comes from:

| | Flagged providers | Other providers |
|---|---:|---:|
| Inpatient share of claims | 11.0% | 4.9% |
| Inpatient share of dollars | 81.6% | 64.0% |
| Avg inpatient claim | $10,311 | $9,783 |
| Avg outpatient claim | $287 | $286 |
| Avg inpatient length of stay | 5.7 days | 5.6 days |
| Avg chronic conditions per patient | 4.5 | 4.5 |

Within each claim type, flagged providers bill almost the same amount per
claim (+5% for inpatient, +0.5% for outpatient) and treat patients with the
same burden of chronic conditions. What differs is that **they bill more than
twice as many of their claims as inpatient stays**. A rule that flags
unusually expensive claims would miss most of this exposure; tracking each
provider's inpatient share against peers is a much stronger signal.

### 3. Fraud *rate* and fraud *dollars* point to different provider types

| Provider type | Providers | Share flagged | Share of fraud exposure |
|---|---:|---:|---:|
| Primarily Hospital | 189 | 45.0% | 27.4% |
| Hospital Only | 398 | 11.1% | 4.2% |
| Primarily Clinic | 1,478 | 20.8% | 60.9% |
| Clinic Only | 3,318 | 2.0% | 7.4% |
| Mixed | 27 | 11.1% | 0.1% |

Mixed hospital/clinic practices are flagged far more often than pure ones.
But by dollars, "Primarily Clinic" providers hold the largest share of
exposure simply because there are many more of them. A rate-based view and a
dollar-based view would send investigators to different places, which is why
the dashboard shows both.

### 4. Larger providers are far more likely to be flagged
Splitting providers into five equal groups by claim volume, the share flagged
rises from **0.7%** in the lowest-volume group to **27.6%** in the highest.
This is a correlation, not proof that size causes fraud: larger providers may
also be audited more often, which would shape how the labels were assigned.

### 5. Diagnosis is a weak signal on its own
By ICD-9 diagnosis chapter, the fraud rate stays within 0.66x to 1.08x of the
baseline (`Fraud Rate Lift (x)` on page 2). The highest chapter, Infectious
and Parasitic Diseases, runs at 41.3% against a 38.1% baseline. Specific
diagnosis/procedure pairs with at least 100 claims reach about 1.6x. Diagnosis
is useful as a secondary filter once a provider is under review, but the
provider is the primary unit of risk.

### 6. Exposure is steady over time
Across the 12 months of 2009, the monthly fraud rate stays between 37.8% and
38.5% by count and between 50.8% and 54.7% by dollars. There is no seasonal
spike, so this is a steady-state problem rather than one that calls for
seasonal review staffing.

### 7. Regional views change priorities, but fraud crosses regions
Among the 32 state codes with at least 5,000 claims, the fraud rate ranges
from **12.3% to 59.1%**. Within each of the four largest state codes, about 20
providers cover 80% of that region's exposure (state code 5: 22 of 175
flagged providers; 10: 20 of 258; 45: 18 of 118; 33: 19 of 123). In state
code 5 the top 50 providers hold **98.2%** of regional exposure, compared with
35.3% nationally.

At the same time, flagged providers bill patients from a median of **4 state
codes**, and only 16.6% stay within one. Regional queues are efficient, but a
national team still needs to own providers that operate across regions. This
is the reasoning behind the RLS design: regional analyst roles plus an
unrestricted `National_Fraud_Team` role.

### Methodology and caveats
- **Labels are provider-level.** The Kaggle dataset flags providers, not
  individual claims. "Fraud exposure" therefore means all reimbursed dollars
  from flagged providers, which is an upper bound on actual fraudulent
  payments.
- **State codes are numeric SSA/CMS codes** with no verified name mapping, so
  regions are reported by code and never plotted on a map.
- **Coverage is about 13 months** (late November 2008 to December 2009); the
  2008 months are partial, so time comparisons use 2009.
- Correlations above are descriptive. They show where to look, not why
  providers commit fraud.

### Claim amount distribution
Mean flagged-provider claim is $1,389.51 vs. $755.21 for other providers, but
the median gap is small ($90 vs. $80). The mean is driven by a heavy tail
(95th percentile $8,000 vs. $3,300; 99th percentile $22,000 vs. $13,000),
which finding 2 traces back to the higher inpatient share rather than higher
prices within each claim type.

## What's Not Included Here

State-name breakdowns and claim-submission-timing analysis are **not included**: the dataset has no state-name mapping (regions are reported by numeric state code only, see finding 7) and no submission-timestamp field. See [`docs/fraud-patterns.md`](./docs/fraud-patterns.md#6-what-was-removed-from-this-analysis-and-why)
for details on what was previously (incorrectly) claimed here and why it
was removed.
