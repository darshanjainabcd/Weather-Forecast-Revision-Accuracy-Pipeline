import base64
import json
from unittest.mock import patch

from src.lambda_consumer import handler

def test_lambda_writes_bronze(monkeypatch):
    monkeypatch.setattr(handler, "DATA_BUCKET", "test-bucket")

    event = {
        "event_id": "abc",
        "event_type": "forecast",
        "source": "open_meteo",
        "city": "Pune",
        "latitude": 18.5204,
        "longitude": 73.8567,
        "issued_at": "2026-08-24T04:00:00+00:00",
        "forecast_timestamp": "2026-08-25T04:00:00+00:00",
        "temperature_2m_c": 26.4,
        "precipitation_probability_pct": 35.0,
        "wind_speed_10m_kmh": 13.1,
        "ingested_at": "2026-08-24T04:05:30+00:00"
    }

    kinesis_event = {
        "Records": [{
            "eventID": "1",
            "kinesis": {
                "data": base64.b64encode(
                    json.dumps(event).encode()
                ).decode()
            }
        }]
    }

    with patch.object(handler.s3, "put_object") as put:
        response = handler.lambda_handler(kinesis_event, None)

        assert response == {"batchItemFailures": []}
        assert put.call_count == 1

        kwargs = put.call_args.kwargs

        assert kwargs["Bucket"] == "test-bucket"

        assert (
            "event_type=forecast/year=2026/month=08/day=24/hour=04"
            in kwargs["Key"]
        )
`
