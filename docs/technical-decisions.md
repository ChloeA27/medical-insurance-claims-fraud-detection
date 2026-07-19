# Technical Decisions

Why specific tools and patterns were chosen, and where the delivered
system diverged from the original plan.

## 1. Lambda + Athena instead of Glue + Redshift

The [original project proposal](../PROJECT_PROPOSAL.md) sketched a
pipeline using **AWS Glue (PySpark)** for transformation and **Amazon
Redshift** for the warehouse layer. The delivered system instead uses
**AWS Lambda (Python) + Amazon Athena** end-to-end (see
[`ARCHITECTURE.md`](../ARCHITECTURE.md)).

**Why the change:**
- **Cost** — Redshift requires a provisioned cluster running continuously;
  Athena is pay-per-query with no idle cost, which matters for a
  portfolio/coursework project with intermittent usage.
- **Simplicity** — Glue jobs need their own execution environment and
  Spark tuning; Lambda + `boto3` + plain SQL (via Athena) is a much
  smaller surface area to build, deploy, and debug for a star schema of
  this size (558K claims).
- **Serverless end-to-end** — S3 + Lambda + Athena has no servers to
  provision or patch at any layer, matching the project's actual scale
  (hundreds of thousands, not billions, of rows).

**Trade-off accepted:** Athena is slower per-query than a warehouse with
persistent compute (Redshift) at very large scale or high query
concurrency. For this dataset's size and access pattern (periodic batch
rebuild, not high-concurrency interactive queries), that trade-off is the
right one.

## 2. Athena Views as a Transform Layer (`*_etl` views)

Rather than writing the CTAS statements for dimension/fact tables
directly against the raw `beneficiary`/`provider`/`inpatient`/`outpatient`
tables, the pipeline first materializes cleaned views
(`v_patients_etl`, `v_providers_etl`, `v_inpatient_claims_etl`,
`v_outpatient_claims_etl`, `v_all_claims_etl`).

**Why:** this separates "cleaning raw, inconsistent source data" (type
casting, `'NA'` handling, code mapping) from "building the dimensional
model" (surrogate keys, derived risk categories, aggregations). Each
dim/fact CTAS statement can then assume clean, typed, well-named columns
— the `CASE`/`TRY_CAST` logic for handling messy source data lives in
exactly one place per source table, not duplicated across every
downstream dim/fact that touches it.

## 3. Parquet for the Warehouse Layer

All dimension and fact tables are stored as Parquet in S3
(`s3://.../lambda_etl/...`), not as views over the raw CSVs.

**Why:** Parquet's columnar format and compression significantly reduce
Athena scan costs and query time versus scanning raw CSV on every query
— important since Athena bills per byte scanned. Materializing to Parquet
also means downstream consumers (this project's `powerbi/data_prep.py`,
or a BI tool connecting to Athena) get a stable, typed schema rather than
re-deriving it from raw text on every read.

## 4. Step-Based Lambda Orchestration (not Step Functions)

The Lambda function accepts a `{"step": "views"|"dims"|"facts"|"validate"|"all"}`
event rather than being split across a Step Functions state machine.

**Why:** at this pipeline's size (one Lambda invocation processing a
sequence of Athena CTAS calls, each taking seconds to low minutes), a
single Lambda with internal step dispatch is simpler to deploy, test, and
re-run for a specific stage (e.g. re-run just `dims` after fixing a bug)
than provisioning and wiring a Step Functions workflow. Step Functions
would earn its complexity at a larger number of independent,
parallelizable stages — not the case here.

**Trade-off accepted:** a single Lambda invocation is bounded by Lambda's
maximum execution timeout (15 minutes). This is acceptable at the current
data volume; a larger dataset or a much larger number of dimension/fact
tables would eventually need Step Functions (or Glue) to fan out steps
in parallel.

## 5. SHA-based Idempotency Was Not Needed Here

Unlike an incremental/streaming pipeline, this project rebuilds each
table from scratch (`DROP` + `CREATE TABLE AS SELECT`) on every run
rather than tracking incremental changes or deduplicating by content
hash.

**Why:** the source data is a static Kaggle dataset snapshot (2008-2010
claims), not a live, continuously-updated feed. A full rebuild on each
scheduled run (via EventBridge, weekly) is simpler and correct by
construction — there's no risk of the dimensional model drifting out of
sync with the source, at the cost of redoing work that, at this data
volume, completes quickly anyway.

## 6. No Separate `DIM_PHYSICIAN` Table

The [original proposal](../PROJECT_PROPOSAL.md) included a standalone
`DIM_PHYSICIAN` dimension. The delivered schema instead carries
`attending_physician_id`, `operating_physician_id`, and
`other_physician_id` directly as attributes on `fact_claims_etl`.

**Why:** the source dataset doesn't include any physician attributes
beyond an ID (no specialty, name, or other descriptive fields) — a
separate dimension table would only ever contain the ID itself, adding a
join with no additional analytical value. If richer physician reference
data were available, promoting these to a proper dimension would be
worthwhile.
