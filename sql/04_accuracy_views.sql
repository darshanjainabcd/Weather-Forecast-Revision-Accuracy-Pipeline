CREATE OR REPLACE VIEW weather.v_current_forecasts AS
SELECT
    f.forecast_revision_key,
    s.source_name,
    c.city_name,
    f.issued_at,
    f.forecast_timestamp,
    DATEDIFF(hour, f.issued_at, f.forecast_timestamp) AS lead_hours,
    f.temperature_2m_c,
    f.precipitation_probability_pct,
    f.wind_speed_10m_kmh
FROM weather.fact_forecast_revision f
JOIN weather.dim_city c ON c.city_key = f.city_key
JOIN weather.dim_weather_source s ON s.source_key = f.source_key
WHERE f.is_current = TRUE;

CREATE OR REPLACE VIEW weather.v_temperature_accuracy AS
SELECT
    s.source_name,
    c.city_name,
    f.issued_at,
    f.forecast_timestamp,
    DATEDIFF(hour, f.issued_at, f.forecast_timestamp) AS lead_hours,
    f.temperature_2m_c AS forecast_temperature_c,
    o.temperature_2m_c AS observed_temperature_c,
    f.temperature_2m_c - o.temperature_2m_c AS error_c,
    ABS(f.temperature_2m_c - o.temperature_2m_c) AS absolute_error_c
FROM weather.fact_forecast_revision f
JOIN weather.fact_observation o
    ON o.city_key = f.city_key
    AND o.observation_timestamp = f.forecast_timestamp
JOIN weather.dim_city c ON c.city_key = f.city_key
JOIN weather.dim_weather_source s ON s.source_key = f.source_key;

CREATE OR REPLACE VIEW weather.v_accuracy_by_city_lead AS
SELECT
    city_name,
    CASE
        WHEN lead_hours <= 6 THEN '00-06h'
        WHEN lead_hours <= 12 THEN '07-12h'
        WHEN lead_hours <= 24 THEN '13-24h'
        WHEN lead_hours <= 48 THEN '25-48h'
        WHEN lead_hours <= 72 THEN '49-72h'
        ELSE '72h+'
    END AS lead_time_bucket,
    COUNT(*) AS sample_count,
    AVG(absolute_error_c) AS mae_c,
    SQRT(AVG(error_c * error_c)) AS rmse_c,
    AVG(error_c) AS bias_c
FROM weather.v_temperature_accuracy
GROUP BY 1, 2;
