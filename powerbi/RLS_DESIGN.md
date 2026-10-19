# Row-Level Security (RLS) Design

A state-based RLS demo for the semantic model in
[`SEMANTIC_MODEL.md`](./SEMANTIC_MODEL.md). It models a realistic access
pattern for a claims and fraud analytics workspace: regional analysts see only
claims for patients in their assigned state codes, while a national fraud team
sees everything.

The roles are defined in [`tmdl/FraudModel.tmdl`](./tmdl/FraudModel.tmdl)
and created automatically when that script is applied.

`dim_patient[state_code]` is a raw numeric SSA/CMS code with no verified name
mapping in this dataset (see §4 of `SEMANTIC_MODEL.md`), so roles are named by
code rather than by state name.

---

## 1. Filter Propagation

RLS is applied on `dim_patient` and relies on single-direction relationships
to propagate the filter:

```
dim_patient ──▶ fact_claims ──▶ every measure (claims, fraud exposure, provider counts)
     │
     └────────▶ fact_patient_claims_summary
```

Every relationship filters from dimension to fact, so one row filter on
`dim_patient` restricts every visual built on `fact_claims` or
`fact_patient_claims_summary`.

**Two consequences that shaped the model:**
- **Provider visuals use measures, not `dim_provider` attribute columns.**
  The RLS filter reaches `fact_claims` but does not flow back up to
  `dim_provider`. A provider table built from measures (`Total Claims`,
  `Total Fraud Exposure $`) only shows providers with claims from the
  analyst's states; one built from pre-aggregated provider columns would show
  every provider's national totals.
- **`fact_provider_summary` is excluded from the model.** It has no path to
  `dim_patient`, so RLS could not filter it and it would leak national
  numbers to restricted roles.

---

## 2. Roles

| Role | Filter on `dim_patient` | Intended audience |
|------|-------------------------|-------------------|
| `Analyst_State05` | `[state_code] = 5` | Regional analyst covering the highest-volume state code (12,052 patients) |
| `Analyst_State10` | `[state_code] = 10` | Regional analyst covering the second-highest-volume state code (9,771 patients) |
| `Analyst_MultiState` | `[state_code] IN { 5, 10, 45 }` | Regional manager overseeing three state codes |
| `National_Fraud_Team` | none (full access) | Central fraud investigation team, documented as an explicit role rather than a gap |

---

## 3. Dynamic RLS (production extension)

The static roles above are enough for a portfolio demo. A production
deployment would drive the filter from a `user_state_mapping` table keyed on
`USERPRINCIPALNAME()`, so a single role covers every analyst:

```dax
[state_code] =
    LOOKUPVALUE (
        user_state_mapping[state_code],
        user_state_mapping[user_email], USERPRINCIPALNAME ()
    )
```

This is not implemented because the dataset has no user directory table.

---

## 4. Testing RLS

In Power BI Desktop, use **Modeling → View as**, pick a role, and check the
KPI cards against these values computed independently from the extracts:

| Role | Total Claims | Total Fraud Exposure $ | Fraud Rate % | Distinct Fraudulent Providers |
|------|-------------:|-----------------------:|-------------:|------------------------------:|
| No role (full data) | 558,211 | $295,681,120 | 38.1% | 506 |
| `Analyst_State05` | 51,350 | $28,566,250 | 59.1% | 175 |
| `Analyst_MultiState` | 124,445 | $69,608,330 | 46.6% | 372 |

The provider table on page 2 should also shrink to providers with at least
one claim from the role's state codes.
