# Results — Medical Insurance Claims Fraud Detection

All figures below are computed directly from the star-schema extract
(`powerbi/extracts/`), reproducible via
[`powerbi/data_prep.py`](./powerbi/data_prep.py). For the full breakdown,
methodology, and removed/retracted figures, see
[`docs/fraud-patterns.md`](./docs/fraud-patterns.md).

> **Note on visualizations:** the original dashboards for this analysis
> were built in Tableau Desktop during a 14-day trial license that has
> since expired, and the underlying `.twbx` file is no longer available.
> The actively maintained BI layer for this project is now the **Power BI
> semantic model** in [`powerbi/`](./powerbi/) — see
> [`powerbi/DASHBOARD_SPEC.md`](./powerbi/DASHBOARD_SPEC.md) for the
> planned dashboard layout and [`powerbi/SEMANTIC_MODEL.md`](./powerbi/SEMANTIC_MODEL.md)
> for the DAX measures used to compute the figures below.

## Headline Numbers

| Metric | Value |
|--------|-------|
| Claims analyzed | 558,211 |
| Fraud rate | 38.12% (212,796 claims) |
| Total fraud exposure | $295.68M |
| Fraudulent providers | 506 of 5,410 (9.35%) |
| Top-50-provider fraud concentration | 35.3% of total fraud exposure |

## Findings

1. **Fraud is heavily concentrated among a small number of providers.**
   The top 100 providers (1.85% of all providers) account for over half
   (54.3%) of total fraud exposure; the top 10 alone account for 11.4%.

2. **Inpatient claims have a much higher fraud rate than outpatient
   claims** — 57.8% vs. 36.6% — despite outpatient claims being ~13x more
   numerous. Fraud review policy calibrated per claim type (rather than a
   single global threshold) would likely be more effective than a
   one-size-fits-all rule.

3. **Providers that are *primarily* hospital-based (mixed volume, mostly
   inpatient) have the highest fraud rate of any provider-type
   segment — 45.0%** — notably higher than either pure "Hospital Only"
   (11.1%) or pure "Clinic Only" (2.0%) providers.

4. **Fraudulent claims skew higher at every percentile, but the median
   difference is small.** Mean fraud claim is $1,389.51 vs. $755.21
   legitimate, but the median gap is much smaller ($90 vs. $80) — the
   mean is driven by a heavy tail of large fraudulent claims (95th
   percentile $8,000 vs. $3,300; 99th percentile $22,000 vs. $13,000),
   not by every fraudulent claim being uniformly larger.

5. **Certain diagnosis-procedure combinations show a real, modest fraud
   enrichment** (~1.6x baseline: ~60-64% fraud rate vs. 38.12% dataset
   average for combinations with ≥100 claims) — worth flagging for
   review, though not the dramatic anomaly sometimes claimed in earlier
   drafts of this analysis.

6. **No seasonal fraud pattern.** Quarterly fraud rate is flat
   (37.95%-38.22% across all four quarters).

## What's Not Included Here

Geographic (state-level) breakdowns and claim-submission-timing analysis
are **not included** — the dataset doesn't contain a state-name mapping
or a submission-timestamp field, so these can't currently be computed
from this pipeline. See [`docs/fraud-patterns.md`](./docs/fraud-patterns.md#6-what-was-removed-from-this-analysis-and-why)
for details on what was previously (incorrectly) claimed here and why it
was removed.
