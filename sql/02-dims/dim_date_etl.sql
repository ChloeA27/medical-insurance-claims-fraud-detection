-- sql/02-dims/dim_date_etl.sql
--
-- TABLE: dim_date_etl
-- GRAIN: One row per unique date in claims
-- PURPOSE: Date dimension for OLAP queries
-- STORAGE: Parquet format in S3
--

CREATE TABLE dim_date_etl
WITH (
    format = 'PARQUET',
    external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_date/'
) AS
SELECT DISTINCT
    CAST(DATE_FORMAT(date_val, '%Y%m%d') AS INT) AS date_key,
    date_val AS full_date,
    YEAR(date_val) AS year,
    QUARTER(date_val) AS quarter,
    MONTH(date_val) AS month,
    DATE_FORMAT(date_val, '%M') AS month_name,
    DAY(date_val) AS day_of_month,
    DAY_OF_WEEK(date_val) AS day_of_week,
    DATE_FORMAT(date_val, '%W') AS day_name,
    CASE 
        WHEN DAY_OF_WEEK(date_val) IN (6,7) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend,
    DATE_FORMAT(date_val, '%Y-%m') AS year_month
FROM (
    SELECT claim_start_date AS date_val 
    FROM v_all_claims_etl 
    WHERE claim_start_date IS NOT NULL
    UNION
    SELECT claim_end_date 
    FROM v_all_claims_etl 
    WHERE claim_end_date IS NOT NULL
    UNION
    SELECT admission_date 
    FROM v_all_claims_etl 
    WHERE admission_date IS NOT NULL
    UNION
    SELECT discharge_date 
    FROM v_all_claims_etl 
    WHERE discharge_date IS NOT NULL
);