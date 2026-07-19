# Dashboard Layout Specification

Two-page Power BI report built on the semantic model in
[`SEMANTIC_MODEL.md`](./SEMANTIC_MODEL.md). Page 1 is an executive summary;
Page 2 drills into provider-level risk.

---

## Page 1 — Executive Fraud Overview

**Audience:** Leadership / stakeholders who need a fast read on fraud
exposure and where it's concentrated. No drill-down expected — glanceable
KPIs plus two supporting trend/geo visuals.

| # | Visual | Fields | Purpose |
|---|--------|--------|---------|
| 1 | KPI card | `[Total Fraud Exposure $]` | Headline number |
| 2 | KPI card | `[Fraud Rate %]` | Headline number |
| 3 | KPI card | `[Distinct Fraudulent Providers]` | Headline number |
| 4 | KPI card | `COUNTROWS(fact_claims)` (Total Claims) | Headline number |
| 5 | Line chart | X: `dim_date[year_month]`; Y: `SUM(fact_claims[claim_amount])` and `[Total Fraud Exposure $]` (dual series) | Claim volume vs. fraud exposure trend over time |
| 6 | Filled map (or shape map) | Location: `dim_patient[state_code]`; Size/Color: `[Total Fraud Exposure $]` | Geographic concentration of fraud exposure |
| 7 | Donut chart | Legend: `fact_claims[claim_type]` (Inpatient/Outpatient); Values: `SUM(fact_claims[claim_amount])` | Claim-type mix |
| 8 | Bar chart | Axis: `dim_provider[risk_level]`; Values: `COUNTROWS(dim_provider)` | Provider risk-tier distribution (Confirmed Fraud / High / Medium / Low Risk) |

**Interactions:**
- Visual 5 (trend line) cross-filters visuals 6–8 on click-and-drag date range selection
- Visual 6 (map) cross-filters all other visuals on state click (single-select)
- A top-of-page slicer on `dim_date[year]` filters the whole page
- KPI cards (1–4) do not cross-filter (fixed headline context) but do respond to the year slicer and map selection

---

## Page 2 — Provider Risk Deep-Dive

**Audience:** Fraud investigation analysts who need to identify and
prioritize specific providers for review.

| # | Visual | Fields | Purpose |
|---|--------|--------|---------|
| 1 | Table | `dim_provider[provider_id]`, `dim_provider[risk_level]`, `dim_provider[total_claims]`, `dim_provider[total_amount]`, `dim_provider[avg_claim_amount]`, `dim_provider[provider_type]`, `dim_provider[activity_level]` | Sortable/filterable provider risk register — the primary investigation tool on this page |
| 2 | KPI card | `[Top 50 Provider Fraud Share %]` | Risk-concentration headline |
| 3 | Bar chart (top N) | Axis: `dim_provider[provider_id]` (Top 20 by `total_amount`, filtered to `is_fraudulent = TRUE`); Values: `dim_provider[total_amount]` | Highest-exposure fraudulent providers |
| 4 | Scatter chart | X: `dim_provider[total_claims]`; Y: `dim_provider[avg_claim_amount]`; Color: `dim_provider[risk_level]`; Size: `dim_provider[total_amount]` | Volume vs. average-amount outlier detection — flags providers billing unusually high average amounts relative to peers |
| 5 | Bar chart | Axis: `dim_diagnosis[icd9_chapter]`; Values: `COUNTROWS(fact_claims)`, filtered to selected provider | Diagnosis-mix profile for a selected provider (drill target from visual 1) |
| 6 | Slicer | `dim_provider[provider_type]` (Hospital Only / Clinic Only / Primarily Hospital / Primarily Clinic / Mixed) | Filter the register by provider type |
| 7 | Slicer | `dim_provider[risk_level]` | Filter the register by risk tier |

**Interactions:**
- Selecting a row in the provider table (visual 1) filters visual 5 (diagnosis mix) to that provider — this is the primary drill-down path on the page
- Visual 3 (top fraud providers bar) selection cross-filters the table (visual 1) and visual 5
- Visual 4 (scatter) selection cross-filters the table
- Slicers 6–7 filter visuals 1, 3, and 4 simultaneously
- A **"Reset to National view"** bookmark/button clears all page-2 selections, for use after `View As Role` testing per `RLS_DESIGN.md` §5

---

## Shared Design Notes

- Both pages share a common date slicer (`dim_date[year]`) synced via
  Power BI's Sync Slicers feature, so switching pages preserves the
  selected year
- Currency fields formatted as `$#,##0` (no decimals — claim amounts are
  large); percentages formatted as `0.0%`
- Color encoding for `risk_level` / fraud status kept consistent across
  both pages: Confirmed Fraud = red, High Risk = orange, Medium Risk =
  yellow, Low Risk = green
- Screenshots of the finished pages will be added to `powerbi/screenshots/`
  and linked from the main [README.md](../README.md) once built in Power BI
  Desktop
