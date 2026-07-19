# Project Proposal — Medical Insurance Claims Analytics System

> **Note:** This is the original project-initiation document written at
> the *start* of this project, before implementation. It records the
> initial goals, target architecture, and success criteria as planned.
> **The system actually delivered uses a simpler, serverless
> architecture** — Lambda + Athena + S3 (see [ARCHITECTURE.md](./ARCHITECTURE.md))
> — rather than the Glue/Redshift/QuickSight stack sketched below, and the
> percentages/dollar figures in §1.3–1.4 and §8 were **planning targets**,
> not measured outcomes. For actual results, see [RESULTS.md](./RESULTS.md)
> and the [README](./README.md).

*Milestone 5: Project Initiation Document*

## Executive Summary

This project implements a comprehensive Medical Insurance Claims Analytics
system designed to detect fraudulent billing patterns and optimize claims
processing workflows. Using healthcare claims data, we will build
cloud-based data pipelines that transform operational data into
actionable insights through dimensional modeling and advanced analytics.

## 1. Problem Statement

### 1.1 Business Problem

Healthcare insurance companies lose approximately $68 billion annually to
fraudulent claims, waste, and abuse in the United States. Current manual
review processes are inefficient, catching only 5-10% of fraudulent
claims while creating significant delays for legitimate claims
processing.

### 1.2 Technical Challenges

- **Scale**: Processing 500,000+ claims across multiple data sources
- **Complexity**: Integrating patient demographics, claims, providers,
  and diagnostic codes
- **Real-time Requirements**: Supporting both batch and real-time fraud
  detection (undetermined at proposal time)
- **Compliance**: Maintaining HIPAA compliance and data privacy
- **Analytics**: Enabling complex OLAP operations for business
  intelligence

### 1.3 Project Objectives (as set at kickoff)

1. Increase fraud detection rate from 5% to 25%
2. Reduce average claim processing time by 60%
3. Enable real-time fraud risk scoring for incoming claims
4. Build scalable data pipelines supporting 10,000+ transactions per day
5. Create interactive dashboards for operational monitoring

### 1.4 Expected Outcomes (as projected at kickoff)

- **Financial Impact**: Save $10-15 million annually in fraudulent
  payouts
- **Operational Efficiency**: Reduce processing time from 30 days to
  <7 days
- **Risk Management**: Proactive identification of high-risk providers
  and patients
- **Data-Driven Decisions**: Enable evidence-based policy pricing and
  network management

## 2. Dataset Identification & Sources

### 2.1 Primary Dataset

**Healthcare Provider Fraud Detection Analysis**
- Source: Kaggle
- Link: https://www.kaggle.com/datasets/rohitrox/healthcare-provider-fraud-detection-analysis
- Total Size: 39 MB (compressed)
- Total Records: 558,211 claims

### 2.2 Dataset Components

| File Name | Description | Records | Data Type | Purpose |
|-----------|-------------|---------|-----------|---------|
| `Train_Beneficiary.csv` | Patient demographics and chronic conditions | 138,556 | Master Data | Patient dimension |
| `Train_Inpatient.csv` | Hospital admission claims | 40,474 | Transactional | Inpatient facts |
| `Train_Outpatient.csv` | Clinic/outpatient visits | 517,737 | Transactional | Outpatient facts |
| `Train.csv` | Provider fraud labels | 5,410 | Reference Data | Provider dimension |

### 2.3 Supplementary Data Sources (proposed, not all implemented)

| Source | Type | Format | Purpose | Stage |
|--------|------|--------|---------|-------|
| ICD-10 Codes | Reference Data | CSV/TXT | Diagnosis enrichment | ETL Transform |
| Real-time Claims API | Streaming Data | JSON/REST | Live fraud scoring | Operations |
| Generated Provider Data | Synthetic | Python/Faker | Missing attributes | ETL Enhance |

## 3. Data Volume & Variety Validation

### 3.1 Volume Requirements

- **Actual Dataset**: 558,211 claims + 138,556 patients + 5,410 providers
- **Total Records**: 702,177

### 3.2 Variety Requirements

- **Minimum Required**: 2 types of files/sources
- **Actual Types (proposed)**: 6 different data sources
  1. Patient Master Data (demographics)
  2. Inpatient Transactional Data (hospital claims)
  3. Outpatient Transactional Data (clinic claims)
  4. Provider Reference Data (fraud labels)
  5. Diagnostic Reference Data (ICD codes)
  6. Real-time Streaming Data (API)

## 4. Technical Architecture Overview (as proposed at kickoff)

> ⚠️ This is the architecture sketched at proposal time. The system
> actually built uses AWS Lambda + Athena instead of Glue + Redshift +
> QuickSight — see [ARCHITECTURE.md](./ARCHITECTURE.md) for what was
> actually implemented.

### 4.1 Data Pipeline Phases (proposed)

| Phase | Service | Purpose |
|-------|---------|---------|
| 1 – Ingestion | S3 Buckets | Store raw CSVs (`/raw/claims/`, `/raw/providers/`) |
| 2 – Transformation | AWS Glue (Python PySpark) | Clean + join + derive metrics + flag fraudulent claims |
| 3 – Load | AWS Redshift | Build star schema (`fact_claims` + `dim_patient/provider/diagnosis/date`) |
| 4 – Analytics | QuickSight/Tableau | KPI dashboards and fraud trend visualizations |
| 5 – (Extension) | Lambda + API Gateway | Future operational layer for claim submission or API-triggered ETL |

### 4.2 Data Flow (proposed)

```
Kaggle CSVs → S3 (raw) → AWS Glue ETL → Amazon Redshift → AWS QuickSight
                                 ↓
                    (optional) Lambda + API Gateway
```

## 5. Dimensional Model Design (Star Schema) — as proposed

### 5.1 Fact Tables

1. **FACT_CLAIMS**
   - Grain: One row per claim
   - Measures: Claim amounts, processing days, fraud flags
   - Foreign Keys: Links to all dimension tables
2. **FACT_PROVIDER_SUMMARY**
   - Grain: One row per provider per month
   - Measures: Aggregated claims, fraud rates, denial rates
3. **FACT_PATIENT_CLAIMS_SUMMARY**
   - Grain: One row per patient per month
   - Measures: Total claims, costs, risk scores

### 5.2 Dimension Tables

1. **DIM_PATIENT** - Demographics, chronic conditions, risk tiers
2. **DIM_PROVIDER** - Specialty, location, fraud risk scores
3. **DIM_DIAGNOSIS** - ICD codes, categories, severity levels
4. **DIM_PROCEDURE** - CPT codes, types, typical costs
5. **DIM_DATE** - Calendar and fiscal hierarchies
6. **DIM_PHYSICIAN** - Individual doctor information (proposed; not
   implemented as a separate dimension in the delivered system)

> The delivered star schema (see [ARCHITECTURE.md](./ARCHITECTURE.md))
> matches this proposal closely but does not include a separate
> `DIM_PHYSICIAN` table — physician IDs are carried as attributes on
> `fact_claims_etl` instead.

## 6. Key Performance Indicators (KPIs) — as proposed

### 6.1 Operational KPIs
- Claims Processing Time (TAT)
- First Pass Approval Rate
- Denial Rate by Category
- Provider Network Utilization

### 6.2 Financial KPIs
- Claims Loss Ratio
- Average Claim Value
- Cost per Member per Month
- Fraud Prevention Savings

### 6.3 Clinical KPIs
- Readmission Rates
- Chronic Disease Prevalence
- Emergency Room Utilization
- Preventive Care Compliance

### 6.4 Fraud Detection KPIs (undetermined at proposal time)
- Fraud Detection Rate
- False Positive Rate
- Average Investigation Time

## 7. Risk Assessment & Mitigation

| Risk | Probability | Impact | Mitigation Strategy |
|------|-------------|--------|----------------------|
| Data Quality Issues | Medium | High | Implement validation checks |
| Performance Bottlenecks | Medium | Medium | Use indexing and partitioning |
| Tool Compatibility | Low | High | Test integrations early |
| Scope Creep | High | Medium | Define clear boundaries |

## 8. Success Criteria (as defined at proposal time)

> These were the targets set before implementation, not verified final
> results. See [RESULTS.md](./RESULTS.md) for actual findings.

### 8.1 Technical Success (targets)
- Process 500,000+ claims without errors
- Achieve <10 second query response time
- Maintain 99.5% pipeline uptime

### 8.2 Business Success (targets)
- Identify fraud patterns with 85% accuracy
- Reduce false positives by 50%
- Enable self-service analytics for business users

## 9. Deliverables (as planned)

### 9.1 Technical Deliverables
1. ETL pipeline code (Python/Spark)
2. Database schemas (DDL scripts)
3. API implementation (REST endpoints)
4. Data quality reports
5. Performance benchmarks

### 9.2 Business Deliverables
1. Executive dashboards (Tableau/QuickSight)
2. Fraud detection models
3. Operational reports
4. User documentation
5. Training materials

---

## Appendix A: Data Dictionary Sample

| Field Name | Source File | Data Type | Description | Transformation |
|------------|-------------|-----------|--------------|------------------|
| `BeneID` | `Train_Beneficiary.csv` | VARCHAR(20) | Beneficiary identifier | Hash for privacy |
| `ClaimID` | `Train_Inpatient.csv` | VARCHAR(20) | Unique claim identifier | Direct mapping |
| `Provider` | `Train.csv` | VARCHAR(20) | Provider identifier | Lookup dimension |
| `InscClaimAmtReimbursed` | Claims files | DECIMAL(12,2) | Insurance payment | Currency format |
| `PotentialFraud` | `Train.csv` | VARCHAR(3) | Fraud indicator | Convert to boolean |

See [docs/data-dictionary.md](./docs/data-dictionary.md) for the full,
as-built data dictionary.

## Appendix B: Reference Links

1. **Primary Dataset**: https://www.kaggle.com/datasets/rohitrox/healthcare-provider-fraud-detection-analysis
2. **ICD-10 Codes**: https://www.cms.gov/medicare/coding-billing/icd-10-codes
