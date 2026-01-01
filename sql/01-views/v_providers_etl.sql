-- sql/01-views/v_providers_etl.sql
--
-- VIEW: v_providers_etl
-- PURPOSE: Clean provider data with fraud flag mapping
-- SOURCE: provider table
-- OUTPUT: provider_id, is_fraudulent (boolean), raw_fraud_value
--
-- LOGIC:
-- - Maps various fraud indicators (Yes, Y, 1) to boolean true
-- - Preserves raw value for debugging

CREATE OR REPLACE VIEW v_providers_etl AS
SELECT DISTINCT
    Provider AS provider_id,
    CASE 
        WHEN PotentialFraud LIKE '%es%' THEN true  -- matches "Yes", "YES", etc.
        WHEN PotentialFraud = 'Y' THEN true
        WHEN PotentialFraud = '1' THEN true
        ELSE false 
    END AS is_fraudulent,
    PotentialFraud AS raw_fraud_value
FROM provider
WHERE Provider IS NOT NULL;