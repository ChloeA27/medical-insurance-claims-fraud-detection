# Row-Level Security (RLS) Design

A simple state-based RLS demo for the semantic model in
[`SEMANTIC_MODEL.md`](./SEMANTIC_MODEL.md). This models a realistic access
pattern for a claims/fraud analytics workspace: regional analysts should only
see claims for patients in their assigned state(s); a national/leadership
role sees everything.

`dim_patient[state_code]` is a raw numeric SSA/CMS code (see §5 of
`SEMANTIC_MODEL.md` for why it isn't mapped to a state name). RLS filters are
defined against that numeric column directly.

---

## 1. Filter Propagation

RLS is applied on `dim_patient` and relies on the model's single-direction
relationships to propagate the filter downstream:

```
dim_patient  ──▶  fact_claims  ──▶  dim_provider / dim_diagnosis / dim_procedure (via slicers)
     │
     └────────▶  fact_patient_claims_summary
```

Because every relationship in this model filters from dimension → fact
(never bidirectional), applying a row filter on `dim_patient` is sufficient
to restrict every visual built on `fact_claims` or
`fact_patient_claims_summary` — no separate rule is needed on the fact
tables themselves.

---

## 2. Roles

Defined in Power BI Desktop under **Modeling → Manage Roles**.

| Role | Filter applied to `dim_patient` | Intended audience |
|------|----------------------------------|--------------------|
| `Analyst_CA` (state_code = 5) | `[state_code] = 5` | Regional analyst covering the highest-volume state in the extract (12,052 patients) |
| `Analyst_FL` (state_code = 10) | `[state_code] = 10` | Regional analyst for the second-highest-volume state (9,771 patients) |
| `Analyst_MultiState` | `[state_code] IN {5, 10, 45}` | Regional manager overseeing three states |
| `National_Fraud_Team` | *(no filter — full access)* | Central fraud investigation team; see full dataset for cross-state pattern detection |

> The actual `state_code` → region mapping above is illustrative (state codes
> are numeric-only in this dataset, see §5 of `SEMANTIC_MODEL.md`); swap in
> whichever `state_code` values matter for a real deployment.

---

## 3. DAX Filter Expressions

Each role's filter expression is set on the `dim_patient` table.

**`Analyst_CA`**
```dax
[state_code] = 5
```

**`Analyst_FL`**
```dax
[state_code] = 10
```

**`Analyst_MultiState`**
```dax
[state_code] IN {5, 10, 45}
```

**`National_Fraud_Team`**
```dax
TRUE()
```
*(No restriction — used to explicitly document "full access" as a role rather than leaving a gap in the role list.)*

---

## 4. Dynamic RLS (production extension)

The static roles above are sufficient for a portfolio demo. A production
deployment would instead drive the filter from a `user_state_mapping` table
keyed on `USERPRINCIPALNAME()`, so a single role covers every analyst:

```dax
[state_code] =
    LOOKUPVALUE(
        user_state_mapping[state_code],
        user_state_mapping[user_email], USERPRINCIPALNAME()
    )
```

This isn't implemented in the current extract (no user-directory table
exists in this dataset), but is documented here as the intended production
pattern.

---

## 5. Testing RLS

In Power BI Desktop: **Modeling → View As**, select a role (e.g.
`Analyst_CA`), and confirm:
1. Page 1 KPI cards recalculate to reflect only `state_code = 5` patients
2. The provider table on Page 2 only shows providers who have at least one
   claim from a `state_code = 5` patient
3. Total claim count matches `dim_patient` filtered to `state_code = 5`
   (12,052 patients in the current extract) joined through to `fact_claims`
