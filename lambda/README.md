# Lambda ETL Orchestration

This directory contains the AWS Lambda code that orchestrates the full
ETL pipeline: creating the `_etl` views, building the dimension and fact
tables via Athena CTAS, and validating row counts. See
[`../ARCHITECTURE.md`](../ARCHITECTURE.md) for the full system design.

## Files

| File | Purpose |
|------|---------|
| `etl_pipeline.py` | `ClaimsETLPipeline` class + `lambda_handler(event, context)` entry point. Modular version — imports `config.py` and `athena_executor.py`. |
| `all-in-one-lambda-etl.py` | Same pipeline logic bundled into a single file, for a simpler Lambda deployment package (no multi-file zip needed). |
| `athena_executor.py` | `AthenaExecutor` — wraps `boto3`'s Athena client: `start_query_execution`, polls `get_query_execution` until `SUCCEEDED`/`FAILED`, logs all SQL and errors. |
| `config.py` | Reads `AWS_BUCKET`, `AWS_REGION`, `AWS_DATABASE` from environment variables (with defaults), and resolves paths to the SQL files under `../sql/`. |

## Event Format

```json
{ "step": "all" }
```

| Step | Behavior |
|------|----------|
| `views` | (Re)creates all `_etl` views from `../sql/01-views/` |
| `dims` | Builds all 5 dimension tables from `../sql/02-dims/` |
| `facts` | Builds all 3 fact tables from `../sql/03-facts/` |
| `validate` | Runs `COUNT(*)` on every dim/fact table |
| `all` | Runs `views → dims → facts → validate` end-to-end |

## Deploying

Both `etl_pipeline.py` (+ `athena_executor.py` + `config.py`) and
`all-in-one-lambda-etl.py` are plain `boto3`-based Python — no extra pip
dependencies are required beyond what's already in the AWS Lambda Python
runtime.

1. Zip `etl_pipeline.py`, `athena_executor.py`, `config.py` (or just
   `all-in-one-lambda-etl.py` alone) together with the `sql/` directory
   at the same relative path the code expects (`../sql/` from `config.py`'s
   perspective — adjust `SQL_DIR` if you restructure the deployment
   package).
2. Create the Lambda function with handler `etl_pipeline.lambda_handler`
   (or `all-in-one-lambda-etl.lambda_handler`).
3. Set environment variables `AWS_BUCKET`, `AWS_REGION`, `AWS_DATABASE` if
   your Athena setup differs from the defaults in `config.py`.
4. Grant the function's execution role Athena query + S3 read/write
   permissions on the target bucket/database.
5. Invoke with `{"step": "all"}`, or schedule via EventBridge
   (`rate(7 day)`) for periodic rebuilds — see
   [`../ARCHITECTURE.md`](../ARCHITECTURE.md#5-scheduling-with-eventbridge).

## Local Testing

`etl_pipeline.py` includes a `__main__` block that calls
`lambda_handler({'step': 'all'}, None)` directly, so it can be run locally
with valid AWS credentials configured (`aws configure`) without deploying
to Lambda first:

```bash
python etl_pipeline.py
```
