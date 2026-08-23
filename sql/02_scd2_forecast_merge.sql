BEGIN;

-- Upsert dimensions first

MERGE INTO weather.dim_city d
USING (
    SELECT DISTINCT city, latitude, longitude
    FROM weather.stg_forecast
) s
ON d.city_name = s.city
WHEN NOT MATCHED THEN
    INSERT (city_name, latitude, longitude)
    VALUES (s.city, s.latitude, s.longitude);

MERGE INTO weather.dim_weather_source d
USING (
    SELECT DISTINCT source
    FROM weather.stg_forecast
) s
ON d.source_name = s.source
WHEN NOT MATCHED THEN
    INSERT (source_name)
    VALUES (s.source);

DROP TABLE IF EXISTS weather.tmp_forecast_incoming;

CREATE TEMP TABLE weather.tmp_forecast_incoming AS
SELECT
    ds.source_key,
    dc.city_key,
    s.issued_at,
    s.forecast_timestamp,
    s.temperature_2m_c,
    s.precipitation_probability_pct,
    s.wind_speed_10m_kmh,
    s.event_id AS source_event_id,
    s.ingested_at AS effective_from,
    SHA2(
        COALESCE(s.temperature_2m_c::VARCHAR, '') || '|' ||
        COALESCE(s.precipitation_probability_pct::VARCHAR, '') || '|' ||
        COALESCE(s.wind_speed_10m_kmh::VARCHAR, ''),
        256
    ) AS record_hash
FROM weather.stg_forecast s
JOIN weather.dim_city dc
    ON dc.city_name = s.city
JOIN weather.dim_weather_source ds
    ON ds.source_name = s.source;

-- Natural key:
-- source + city + issued_at + forecast_timestamp

UPDATE weather.fact_forecast_revision t
SET effective_to = i.effective_from,
    is_current = FALSE
FROM weather.tmp_forecast_incoming i
WHERE t.source_key = i.source_key
AND t.city_key = i.city_key
AND t.issued_at = i.issued_at
AND t.forecast_timestamp = i.forecast_timestamp
AND t.is_current = TRUE
AND t.record_hash <> i.record_hash;

INSERT INTO weather.fact_forecast_revision (
    source_key,
    city_key,
    issued_at,
    forecast_timestamp,
    temperature_2m_c,
    precipitation_probability_pct,
    wind_speed_10m_kmh,
    source_event_id,
    effective_from,
    effective_to,
    is_current,
    record_hash
)
SELECT
    i.source_key,
    i.city_key,
    i.issued_at,
    i.forecast_timestamp,
    i.temperature_2m_c,
    i.precipitation_probability_pct,
    i.wind_speed_10m_kmh,
    i.source_event_id,
    i.effective_from,
    NULL,
    TRUE,
    i.record_hash
FROM weather.tmp_forecast_incoming i
LEFT JOIN weather.fact_forecast_revision t
    ON t.source_key = i.source_key
    AND t.city_key = i.city_key
    AND t.issued_at = i.issued_at
    AND t.forecast_timestamp = i.forecast_timestamp
    AND t.is_current = TRUE
WHERE t.forecast_revision_key IS NULL
OR t.record_hash <> i.record_hash;

TRUNCATE TABLE weather.stg_forecast;

COMMIT;
