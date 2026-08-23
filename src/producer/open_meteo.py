from __future__ import annotations
from datetime import datetime, timezone
from typing import Any
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from src.common.ids import stable_event_id

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


class OpenMeteoClient:
    def __init__(self, timeout_seconds: int = 30):
        self.timeout_seconds = timeout_seconds

    @retry(stop=stop_after_attempt(3),wait=wait_exponential(multiplier=1, min=1, max=8))
    def _get(self, params: dict[str, Any]) -> dict:
        response = requests.get(FORECAST_URL,params=params,timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.json()

    def fetch_city(self, city: dict, forecast_days: int = 3) -> list[arams = {"latitude": city["latitude"],"longitude": city["longitude"],"hourly": ",".join(["temperature_2m","precipitation_probability","precipitation","wind_speed_10m"]),"forecast_days": forecast_days,"past_days": 1,"timezone": "UTC"}

        payload = self._get(params)
        hourly = payload["hourly"]

        now = datetime.now(timezone.utc)
        issued_at = now.replace(minute=0, second=0, microsecond=0)
        ingested_at = now.isoformat()

        result = []

        for i, ts in enumerate(hourly["time"]):
            event_ts = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)

            common = {
                "source": "open_meteo",
                "city": city["city"],
                "latitude": float(city["latitude"]),
                "longitude": float(city["longitude"]),
                "ingested_at": ingested_at
            }

            if event_ts >= issued_at:
                event = {
                    **common,
                    "event_type": "forecast",
                    "issued_at": issued_at.isoformat(),
                    "forecast_timestamp": event_ts.isoformat(),
                    "temperature_2m_c": _item(hourly, "temperature_2m", i),
                    "precipitation_probability_pct": _item(
                        hourly, "precipitation_probability", i
                    ),
                    "wind_speed_10m_kmh": _item(hourly, "wind_speed_10m", i)
                }
            else:
                event = {
                    **common,
                    "event_type": "observation",
                    "observation_timestamp": event_ts.isoformat(),
                    "temperature_2m_c": _item(hourly, "temperature_2m", i),
                    "precipitation_mm": _item(hourly, "precipitation", i),
                    "wind_speed_10m_kmh": _item(hourly, "wind_speed_10m", i)
                }

            event["event_id"] = stable_event_id(event)
            result.append(event)

        return result


def _item(d: dict, key: str, i: int):
    values = d.get(key) or []
    return values[i] if i < len(values) else None
