CREATE TABLE dim_provider_etl
    WITH (
        format = 'PARQUET',
        external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_provider/'
    ) AS
    WITH provider_stats AS (
        SELECT 
            provider_id,
            COUNT(DISTINCT patient_id) AS total_patients,
            COUNT(claim_id) AS total_claims,
            SUM(claim_amount) AS total_amount,
            AVG(claim_amount) AS avg_claim_amount,
            MAX(claim_amount) AS max_claim_amount,
            MIN(claim_amount) AS min_claim_amount,
            SUM(CASE WHEN claim_type = 'Inpatient' THEN 1 ELSE 0 END) AS inpatient_claims,
            SUM(CASE WHEN claim_type = 'Outpatient' THEN 1 ELSE 0 END) AS outpatient_claims,
            AVG(CASE WHEN claim_type = 'Inpatient' THEN length_of_stay ELSE NULL END) AS avg_length_of_stay
        FROM v_all_claims_etl
        GROUP BY provider_id
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY p.provider_id) AS provider_sk,
        p.provider_id,
        p.is_fraudulent,
        COALESCE(ps.total_patients, 0) AS total_patients,
        COALESCE(ps.total_claims, 0) AS total_claims,
        COALESCE(ps.total_amount, 0) AS total_amount,
        COALESCE(ps.avg_claim_amount, 0) AS avg_claim_amount,
        COALESCE(ps.max_claim_amount, 0) AS max_claim_amount,
        COALESCE(ps.min_claim_amount, 0) AS min_claim_amount,
        COALESCE(ps.inpatient_claims, 0) AS inpatient_claims,
        COALESCE(ps.outpatient_claims, 0) AS outpatient_claims,
        COALESCE(ps.avg_length_of_stay, 0) AS avg_length_of_stay,
        CASE 
            WHEN ps.inpatient_claims > 0 AND ps.outpatient_claims = 0 THEN 'Hospital Only'
            WHEN ps.inpatient_claims = 0 AND ps.outpatient_claims > 0 THEN 'Clinic Only'
            WHEN ps.inpatient_claims > ps.outpatient_claims THEN 'Primarily Hospital'
            WHEN ps.outpatient_claims > ps.inpatient_claims THEN 'Primarily Clinic'
            ELSE 'Mixed'
        END AS provider_type,
        CASE 
            WHEN p.is_fraudulent = TRUE THEN 'Confirmed Fraud'
            WHEN ps.avg_claim_amount > (
                SELECT AVG(claim_amount) + 2 * STDDEV(claim_amount) 
                FROM v_all_claims_etl
            ) THEN 'High Risk'
            WHEN ps.avg_claim_amount > (
                SELECT AVG(claim_amount) + STDDEV(claim_amount) 
                FROM v_all_claims_etl
            ) THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS risk_level,
        CASE 
            WHEN ps.total_claims > 1000 THEN 'Very High Volume'
            WHEN ps.total_claims > 500 THEN 'High Volume'
            WHEN ps.total_claims > 100 THEN 'Medium Volume'
            WHEN ps.total_claims > 0 THEN 'Low Volume'
            ELSE 'Inactive'
        END AS activity_level
    FROM v_providers_etl p
    LEFT JOIN provider_stats ps ON p.provider_id = ps.provider_id
    WHERE p.provider_id IS NOT NULL