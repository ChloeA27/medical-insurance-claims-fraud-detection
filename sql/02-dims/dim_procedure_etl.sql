CREATE TABLE dim_procedure_etl
    WITH (
        format = 'PARQUET',
        external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_procedure/'
    ) AS
    WITH all_procedure_codes AS (
        SELECT DISTINCT procedure_code
        FROM (
            SELECT procedure_code_1 AS procedure_code FROM v_inpatient_claims_etl 
            WHERE procedure_code_1 IS NOT NULL AND procedure_code_1 != '' AND procedure_code_1 != 'NA'
            UNION SELECT procedure_code_2 FROM v_inpatient_claims_etl 
            WHERE procedure_code_2 IS NOT NULL AND procedure_code_2 != '' AND procedure_code_2 != 'NA'
            UNION SELECT procedure_code_3 FROM v_inpatient_claims_etl 
            WHERE procedure_code_3 IS NOT NULL AND procedure_code_3 != '' AND procedure_code_3 != 'NA'
            UNION SELECT procedure_code_4 FROM v_inpatient_claims_etl 
            WHERE procedure_code_4 IS NOT NULL AND procedure_code_4 != '' AND procedure_code_4 != 'NA'
            UNION SELECT procedure_code_5 FROM v_inpatient_claims_etl 
            WHERE procedure_code_5 IS NOT NULL AND procedure_code_5 != '' AND procedure_code_5 != 'NA'
            UNION SELECT procedure_code_6 FROM v_inpatient_claims_etl 
            WHERE procedure_code_6 IS NOT NULL AND procedure_code_6 != '' AND procedure_code_6 != 'NA'
            UNION SELECT procedure_code_1 FROM v_outpatient_claims_etl 
            WHERE procedure_code_1 IS NOT NULL AND procedure_code_1 != '' AND procedure_code_1 != 'NA'
            UNION SELECT procedure_code_2 FROM v_outpatient_claims_etl 
            WHERE procedure_code_2 IS NOT NULL AND procedure_code_2 != '' AND procedure_code_2 != 'NA'
            UNION SELECT procedure_code_3 FROM v_outpatient_claims_etl 
            WHERE procedure_code_3 IS NOT NULL AND procedure_code_3 != '' AND procedure_code_3 != 'NA'
        )
    ),
    cleaned_codes AS (
        SELECT 
            REPLACE(REPLACE(REPLACE(procedure_code, '\"', ''), '''', ''), ' ', '') AS procedure_code_clean
        FROM all_procedure_codes
        WHERE procedure_code IS NOT NULL
    ),
    validated_codes AS (
        SELECT DISTINCT procedure_code_clean
        FROM cleaned_codes
        WHERE 
            procedure_code_clean NOT LIKE 'V%%'
            AND procedure_code_clean NOT LIKE 'E%%'
            AND LENGTH(procedure_code_clean) <= 4
            AND procedure_code_clean NOT IN ('0', '00', '000', '0000')
            AND TRY_CAST(procedure_code_clean AS INT) IS NOT NULL
            AND TRY_CAST(procedure_code_clean AS INT) > 0
            AND TRY_CAST(procedure_code_clean AS INT) <= 9999
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY procedure_code_clean) AS procedure_sk,
        procedure_code_clean AS procedure_code,
        SUBSTRING(procedure_code_clean, 1, 2) AS code_category,
        'ICD-9 Procedure' AS code_type,
        CASE 
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 5 THEN 'Operations on Nervous System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 7 THEN 'Operations on Endocrine System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 16 THEN 'Operations on Eye'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 20 THEN 'Operations on Ear'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 29 THEN 'Operations on Nose, Mouth, and Pharynx'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 34 THEN 'Operations on Respiratory System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 39 THEN 'Operations on Cardiovascular System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 41 THEN 'Operations on Hemic and Lymphatic System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 54 THEN 'Operations on Digestive System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 59 THEN 'Operations on Urinary System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 64 THEN 'Operations on Male Genital Organs'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 71 THEN 'Operations on Female Genital Organs'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 75 THEN 'Obstetrical Procedures'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 84 THEN 'Operations on Musculoskeletal System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) <= 86 THEN 'Operations on Integumentary System'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) >= 87 THEN 'Miscellaneous Diagnostic and Therapeutic Procedures'
            ELSE 'Other/Unknown'
        END AS procedure_category,
        CASE 
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 35 AND 39 THEN TRUE
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 1 AND 5 THEN TRUE
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 60 AND 71 THEN TRUE
            ELSE FALSE
        END AS is_major_procedure,
        CASE 
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 1 AND 5 THEN 'High'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 35 AND 39 THEN 'High'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) >= 87 THEN 'Low'
            ELSE 'Medium'
        END AS risk_level,
        CASE 
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 35 AND 39 THEN 'Very High'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 1 AND 5 THEN 'Very High'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) BETWEEN 76 AND 84 THEN 'High'
            WHEN CAST(SUBSTRING(procedure_code_clean, 1, 2) AS INT) >= 87 THEN 'Low'
            ELSE 'Medium'
        END AS cost_category
    FROM validated_codes
    ORDER BY procedure_code_clean