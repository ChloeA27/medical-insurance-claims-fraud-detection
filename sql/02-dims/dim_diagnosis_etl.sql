CREATE TABLE dim_diagnosis_etl
    WITH (
        format = 'PARQUET',
        external_location = 's3://insurance-claim-qian-2025/data/warehouse/lambda_etl/dim_tables/dim_diagnosis/'
    ) AS
    WITH all_diagnosis_codes AS (
        SELECT DISTINCT diagnosis_code
        FROM (
            SELECT diagnosis_code_1 AS diagnosis_code FROM v_inpatient_claims_etl WHERE diagnosis_code_1 IS NOT NULL
            UNION SELECT diagnosis_code_2 FROM v_inpatient_claims_etl WHERE diagnosis_code_2 IS NOT NULL
            UNION SELECT diagnosis_code_3 FROM v_inpatient_claims_etl WHERE diagnosis_code_3 IS NOT NULL
            UNION SELECT diagnosis_code_4 FROM v_inpatient_claims_etl WHERE diagnosis_code_4 IS NOT NULL
            UNION SELECT diagnosis_code_5 FROM v_inpatient_claims_etl WHERE diagnosis_code_5 IS NOT NULL
            UNION SELECT diagnosis_code_6 FROM v_inpatient_claims_etl WHERE diagnosis_code_6 IS NOT NULL
            UNION SELECT diagnosis_code_7 FROM v_inpatient_claims_etl WHERE diagnosis_code_7 IS NOT NULL
            UNION SELECT diagnosis_code_8 FROM v_inpatient_claims_etl WHERE diagnosis_code_8 IS NOT NULL
            UNION SELECT diagnosis_code_9 FROM v_inpatient_claims_etl WHERE diagnosis_code_9 IS NOT NULL
            UNION SELECT diagnosis_code_10 FROM v_inpatient_claims_etl WHERE diagnosis_code_10 IS NOT NULL
            UNION SELECT admit_diagnosis_code FROM v_inpatient_claims_etl WHERE admit_diagnosis_code IS NOT NULL

            UNION SELECT diagnosis_code_1 FROM v_outpatient_claims_etl WHERE diagnosis_code_1 IS NOT NULL
            UNION SELECT diagnosis_code_2 FROM v_outpatient_claims_etl WHERE diagnosis_code_2 IS NOT NULL
            UNION SELECT diagnosis_code_3 FROM v_outpatient_claims_etl WHERE diagnosis_code_3 IS NOT NULL
            UNION SELECT diagnosis_code_4 FROM v_outpatient_claims_etl WHERE diagnosis_code_4 IS NOT NULL
            UNION SELECT diagnosis_code_5 FROM v_outpatient_claims_etl WHERE diagnosis_code_5 IS NOT NULL
            UNION SELECT diagnosis_code_6 FROM v_outpatient_claims_etl WHERE diagnosis_code_6 IS NOT NULL
            UNION SELECT diagnosis_code_7 FROM v_outpatient_claims_etl WHERE diagnosis_code_7 IS NOT NULL
            UNION SELECT diagnosis_code_8 FROM v_outpatient_claims_etl WHERE diagnosis_code_8 IS NOT NULL
            UNION SELECT diagnosis_code_9 FROM v_outpatient_claims_etl WHERE diagnosis_code_9 IS NOT NULL
            UNION SELECT diagnosis_code_10 FROM v_outpatient_claims_etl WHERE diagnosis_code_10 IS NOT NULL
            UNION SELECT admit_diagnosis_code FROM v_outpatient_claims_etl WHERE admit_diagnosis_code IS NOT NULL
        )
    ),
    cleaned_codes AS (
        SELECT 
            REPLACE(REPLACE(diagnosis_code, '\"', ''), '''', '') AS diagnosis_code_clean,
            diagnosis_code AS diagnosis_code_original
        FROM all_diagnosis_codes
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY diagnosis_code_clean) AS diagnosis_sk,
        diagnosis_code_clean AS diagnosis_code,
        SUBSTRING(diagnosis_code_clean, 1, 3) AS code_category,
        CASE 
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = 'V' THEN 'Supplementary Classification (V Codes)'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = 'E' THEN 'External Causes of Injury (E Codes)'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '0' THEN 'Infectious and Parasitic Diseases'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '1' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '139' THEN 'Infectious and Parasitic Diseases'
                    ELSE 'Neoplasms'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '2' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '239' THEN 'Neoplasms'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '279' THEN 'Endocrine, Nutritional and Metabolic'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '289' THEN 'Blood and Blood-Forming Organs'
                    ELSE 'Mental Disorders'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '3' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '319' THEN 'Mental Disorders'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '389' THEN 'Nervous System and Sense Organs'
                    ELSE 'Circulatory System'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '4' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '459' THEN 'Circulatory System'
                    ELSE 'Respiratory System'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '5' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '579' THEN 'Digestive System'
                    ELSE 'Genitourinary System'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '6' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '629' THEN 'Genitourinary System'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '679' THEN 'Pregnancy, Childbirth and Puerperium'
                    ELSE 'Skin and Subcutaneous Tissue'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = '7' THEN 
                CASE
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '709' THEN 'Skin and Subcutaneous Tissue'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '739' THEN 'Musculoskeletal and Connective Tissue'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '759' THEN 'Congenital Anomalies'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '779' THEN 'Perinatal Period Conditions'
                    WHEN SUBSTRING(diagnosis_code_clean, 1, 3) <= '799' THEN 'Symptoms, Signs and Ill-Defined Conditions'
                    ELSE 'Injury and Poisoning'
                END
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) IN ('8', '9') THEN 'Injury and Poisoning'
            ELSE 'Other/Unknown'
        END AS icd9_chapter,
        CASE 
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) = '250' THEN 'Diabetes Mellitus'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) IN ('401', '402', '403', '404', '405') THEN 'Hypertensive Disease'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) = '428' THEN 'Heart Failure'
            WHEN diagnosis_code_clean = '4019' THEN 'Essential Hypertension'
            WHEN diagnosis_code_clean IN ('2720', '2724') THEN 'Hyperlipidemia'
            ELSE NULL
        END AS subcategory,
        CASE 
            WHEN diagnosis_code_clean IN ('4019', '25000', '2724', '496', '4280', '42731', '2859') THEN TRUE
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) IN ('250', '401', '402', '403', '404', '405', '428', '496') THEN TRUE
            ELSE FALSE
        END AS is_chronic,
        CASE 
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) IN ('1', '2') AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) >= '140' AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) <= '239' THEN 'High'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) >= '390' AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) <= '459' THEN 'High'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 3) >= '580' AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) <= '629' THEN 'High'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) = 'V' THEN 'Low'
            ELSE 'Standard'
        END AS risk_level,
        CASE 
            WHEN diagnosis_code_clean IN ('4019', '2724', '25000', '4280', '2859', '496') THEN TRUE
            ELSE FALSE
        END AS is_common_code,
        CASE 
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) IN ('1', '2') AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) >= '140' AND 
                 SUBSTRING(diagnosis_code_clean, 1, 3) <= '239' THEN 'Very High'
            WHEN SUBSTRING(diagnosis_code_clean, 1, 1) IN ('8', '9') THEN 'High'
            ELSE 'Standard'
        END AS cost_category
    FROM cleaned_codes
    WHERE diagnosis_code_clean IS NOT NULL 
      AND diagnosis_code_clean != ''
      AND diagnosis_code_clean != 'NA'