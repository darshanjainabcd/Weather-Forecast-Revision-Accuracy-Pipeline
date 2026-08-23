CREATE SCHEMA IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.dim_city (
    city_key BIGINT IDENTITY(1,1),
    city_name VARCHAR(100) NOT NULL,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    created_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (city_key)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS weather.dim_weather_source (
    source_key BIGINT IDENTITY(1,1),
    source_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (source_key)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS weather.dim_time (
    time_key BIGINT,
    full_timestamp TIMESTAMP NOT NULL,
    calendar_date DATE NOT NULL,
    year_num SMALLINT,
    month_num SMALLINT,
    day_num SMALLINT,
    hour_num SMALLINT,
    day_of_week SMALLINT,
    PRIMARY KEY (time_key)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS weather.stg_forecast (
    event_id VARCHAR(64),
    source VARCHAR(100),
    city VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    issued_at TIMESTAMP,
    forecast_timestamp TIMESTAMP,
    temperature_2m_c DOUBLE PRECISION,
    precipitation_probability_pct DOUBLE PRECISION,
    wind_speed_10m_kmh DOUBLE PRECISION,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS weather.stg_observation (
    event_id VARCHAR(64),
    source VARCHAR(100),
    city VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    observation_timestamp TIMESTAMP,
    temperature_2m_c DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    wind_speed_10m_kmh DOUBLE PRECISION,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS weather.fact_forecast_revision (
    forecast_revision_key BIGINT IDENTITY(1,1),
    source_key BIGINT NOT NULL,
    city_key BIGINT NOT NULL,
    issued_at TIMESTAMP NOT NULL,
    forecast_timestamp TIMESTAMP NOT NULL,
    temperature_2m_c DOUBLE PRECISION,
    precipitation_probability_pct DOUBLE PRECISION,
    wind_speed_10m_kmh DOUBLE PRECISION,
    source_event_id VARCHAR(64),
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    record_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(city_key)
SORTKEY(forecast_timestamp, city_key, issued_at);

CREATE TABLE IF NOT EXISTS weather.fact_observation (
    source_key BIGINT NOT NULL,
    city_key BIGINT NOT NULL,
    observation_timestamp TIMESTAMP NOT NULL,
    temperature_2m_c DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    wind_speed_10m_kmh DOUBLE PRECISION,
    source_event_id VARCHAR(64),
    updated_at TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(city_key)
SORTKEY(observation_timestamp, city_key);
