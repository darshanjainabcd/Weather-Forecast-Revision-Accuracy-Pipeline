from __future__ import annotations
import hashlib
import json

def stable_event_id(event: dict) -> str:
    event_type = event["event_type"]
    if event_type == "forecast":
        identity = {"event_type": event_type,
            "source": event["source"],
            "city": event["city"],
            "issued_at": event["issued_at"],
            "forecast_timestamp": event["forecast_timestamp"],
            "temperature_2m_c": event.get("temperature_2m_c"),
            "precipitation_probability_pct": event.get(
                "precipitation_probability_pct"),
            "wind_speed_10m_kmh": event.get("wind_speed_10m_kmh"),}
    elif event_type == "observation":
        identity = {
            "event_type": event_type,
            "source": event["source"],
            "city": event["city"],
            "observation_timestamp": event["observation_timestamp"],
            "temperature_2m_c": event.get("temperature_2m_c"),
            "precipitation_mm": event.get("precipitation_mm"),
            "wind_speed_10m_kmh": event.get("wind_speed_10m_kmh"),}
    else:
        raise ValueError(f"Unsupported event_type={event_type}")
    payload = json.dumps(identity, sort_keys=True,separators=(",", ":"), default=str, )

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
