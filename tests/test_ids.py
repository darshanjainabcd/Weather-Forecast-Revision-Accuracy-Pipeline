from src.common.ids import stable_event_id

def test_forecast_event_id_is_stable():
    event = {
        "event_type": "forecast",
        "source": "open_meteo",
        "city": "Pune",
        "issued_at": "2026-08-24T04:00:00+00:00",
        "forecast_timestamp": "2026-08-25T04:00:00+00:00",
        "temperature_2m_c": 26.4,
        "precipitation_probability_pct": 35.0,
        "wind_speed_10m_kmh": 13.1
    }

    assert stable_event_id(event) == stable_event_id(dict(event))

def test_forecast_id_changes_when_value_changes():
    a = {
        "event_type": "forecast",
        "source": "open_meteo",
        "city": "Pune",
        "issued_at": "2026-08-24T04:00:00+00:00",
        "forecast_timestamp": "2026-08-25T04:00:00+00:00",
        "temperature_2m_c": 26.4,
        "precipitation_probability_pct": 35.0,
        "wind_speed_10m_kmh": 13.1
    }

    b = dict(a)
    b["temperature_2m_c"] = 27.0

    assert stable_event_id(a) != stable_event_id(b)
