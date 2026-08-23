-- 1. Exactly one current SCD2 row per natural key

SELECT
    source_key,
    city_key,
    issued_at,
    forecast_timestamp,
    COUNT(*) AS current_rows
FROM weather.fact_forecast_revision
WHERE is_current = TRUE
GROUP BY 1,2,3,4
HAVING COUNT(*) > 1;

-- 2. No invalid SCD2 intervals

SELECT *
FROM weather.fact_forecast_revision
WHERE effective_to IS NOT NULL
AND effective_to <= effective_from;

-- 3. Forecast target cannot precede issuance

SELECT *
FROM weather.fact_forecast_revision
WHERE forecast_timestamp < issued_at;

-- 4. Duplicate observations

SELECT
    source_key,
    city_key,
    observation_timestamp,
    COUNT(*) AS cnt
FROM weather.fact_observation
GROUP BY 1,2,3
HAVING COUNT(*) > 1;

-- 5. Forecasts whose target hour already passed
-- but still have no matching observation

SELECT COUNT(*) AS missing_observation_count
FROM weather.fact_forecast_revision f
LEFT JOIN weather.fact_observation o
    ON o.city_key = f.city_key
    AND o.observation_timestamp = f.forecast_timestamp
WHERE f.forecast_timestamp < GETDATE()
AND o.city_key IS NULL;
