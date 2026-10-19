# Dashboard Layout Specification

Two-page Power BI report built on the semantic model in
[`SEMANTIC_MODEL.md`](./SEMANTIC_MODEL.md). Page 1 is an executive summary;
page 2 drills into provider-level risk. Every visual uses measures from
`fact_claims` so that the RLS roles in [`RLS_DESIGN.md`](./RLS_DESIGN.md)
filter the whole report.

---

## Page 1: Executive Fraud Overview

**Audience:** leadership who need a fast read on fraud exposure and where it
is concentrated.

| # | Visual | Fields | Purpose |
|---|--------|--------|---------|
| 1 | Card | `[Total Claims]` | Headline volume |
| 2 | Card | `[Total Fraud Exposure $]` | Headline exposure |
| 3 | Card | `[Fraud Rate %]` | Share of claims from flagged providers (by count) |
| 4 | Card | `[Fraud Exposure % of Total $]` | Share of reimbursed dollars from flagged providers (by value) |
| 5 | Card | `[Top 50 Provider Fraud Share %]` | Risk-concentration headline |
| 6 | Line chart | X: `dim_date[year_month]`; Y: `[Total Claim Amount $]` and `[Total Fraud Exposure $]` | Claim volume vs. fraud exposure over time |
| 7 | Clustered bar | Axis: `dim_provider[provider_type]`; Values: `[Total Fraud Exposure $]` | Where exposure sits by provider type |
| 8 | Donut | Legend: `fact_claims[claim_type]`; Values: `[Total Claim Amount $]` | Inpatient vs. outpatient dollar mix |
| 9 | Clustered bar | Axis: `dim_provider[risk_level]`; Values: `[Total Claims]` | Claims by provider risk tier |

**Interactions:**
- A `dim_date[year]` slicer at the top filters the whole page.
- Selecting a provider type (visual 7) or claim type (visual 8) cross-filters
  the trend line and the risk-tier bar.
- Cards respond to the slicer and to selections but do not cross-filter.

---

## Page 2: Provider Risk Deep-Dive

**Audience:** fraud investigation analysts who need to identify and
prioritize providers for review.

| # | Visual | Fields | Purpose |
|---|--------|--------|---------|
| 1 | Table | `dim_provider[provider_id]`, `dim_provider[risk_level]`, `dim_provider[provider_type]`, `[Total Claims]`, `[Total Claim Amount $]`, `[Avg Claim Amount]`, `[Inpatient Share %]` | Sortable provider risk register, the main investigation tool. Uses measures so RLS applies |
| 2 | Card | `[Distinct Fraudulent Providers]` | Count of flagged providers in view |
| 3 | Card | `[Fraud Exposure per Fraudulent Provider $]` | Average exposure per flagged provider |
| 4 | Bar chart (Top N = 20) | Axis: `dim_provider[provider_id]`; Values: `[Total Fraud Exposure $]` | Highest-exposure flagged providers |
| 5 | Scatter | Details: `dim_provider[provider_id]`; X: `[Total Claims]`; Y: `[Avg Claim Amount]`; Legend: `dim_provider[risk_level]` | Volume vs. average amount outlier view |
| 6 | Table | `dim_diagnosis[icd9_chapter]`, `[Total Claims]`, `[Fraud Rate %]`, `[Fraud Rate Lift (x)]` | Which diagnosis chapters run above the baseline fraud rate |
| 7 | Slicer | `dim_provider[provider_type]` | Filter by provider type |
| 8 | Slicer | `dim_provider[risk_level]` | Filter by risk tier |

**Interactions:**
- Selecting a provider in the register (visual 1) filters the diagnosis table
  (visual 6) to that provider's claims. This is the main drill-down path.
- Selecting a bar in visual 4 or a point in visual 5 cross-filters the
  register.
- Slicers 7 and 8 filter visuals 1, 4, 5, and 6.
- A **Reset** bookmark button clears all page 2 selections, for use after
  testing roles with **View as**.

---

## Shared Design Notes

- Both pages share the `dim_date[year]` slicer through **Sync slicers**.
- Currency is formatted as `$#,##0` and percentages as `0.0%` (set in the
  model, so visuals inherit it).
- Risk-level colors are consistent across pages: Confirmed Fraud = red,
  High Risk = orange, Medium Risk = yellow, Low Risk = green.
- The original design included a state-level map. It was dropped because
  `state_code` is a numeric code with no verified state-name mapping in this
  dataset, so a map would have required guessing.
