BEGIN;

MERGE INTO weather.dim_city d
USING (
    SELECT DISTINCT city, latitude, longitude
    FROM weather.stg_observation
) s
ON d.city_name = s.city
WHEN NOT MATCHED THEN
    INSERT (city_name, latitude, longitude)
    VALUES (s.city, s.latitude, s.longitude);

MERGE INTO weather.dim_weather_source d
USING (
    SELECT DISTINCT source
    FROM weather.stg_observation
) s
ON d.source_name = s.source
WHEN NOT MATCHED THEN
    INSERT (source_name)
    VALUES (s.source);

MERGE INTO weather.fact_observation t
USING (
    SELECT
        ds.source_key,
        dc.city_key,
        s.observation_timestamp,
        s.temperature_2m_c,
        s.precipitation_mm,
        s.wind_speed_10m_kmh,
        s.event_id AS source_event_id
    FROM weather.stg_observation s
    JOIN weather.dim_city dc ON dc.city_name = s.city
    JOIN weather.dim_weather_source ds ON ds.source_name = s.source
) s
ON t.source_key = s.source_key
AND t.city_key = s.city_key
AND t.observation_timestamp = s.observation_timestamp

WHEN MATCHED THEN
    UPDATE SET
        temperature_2m_c = s.temperature_2m_c,
        precipitation_mm = s.precipitation_mm,
        wind_speed_10m_kmh = s.wind_speed_10m_kmh,
        source_event_id = s.source_event_id,
        updated_at = GETDATE()

WHEN NOT MATCHED THEN
    INSERT (
        source_key,
        city_key,
        observation_timestamp,
        temperature_2m_c,
        precipitation_mm,
        wind_speed_10m_kmh,
        source_event_id
    )
    VALUES (
        s.source_key,
        s.city_key,
        s.observation_timestamp,
        s.temperature_2m_c,
        s.precipitation_mm,
        s.wind_speed_10m_kmh,
        s.source_event_id
    );

TRUNCATE TABLE weather.stg_observation;

COMMIT;
