from __future__ import annotations
import json
import logging
from collections.abc import Iterable
import boto3
from src.common.config import Settings, load_cities
from src.common.logging_utils import configure_logging
from src.producer.open_meteo import OpenMeteoClient

logger = logging.getLogger(__name__)

def chunks(items: list[dict], size: int) -> Iterable[list[dict]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]

def publish_events(events: list[dict], mode: str = "kinesis", settings: Settings | None = None) -> int:
    settings = settings or Settings()
    if mode == "stdout":
        for event in events:
            print(json.dumps(event, separators=(",", ":"), default=str))
        return len(events)

    client = boto3.client("kinesis", region_name=settings.aws_region)

    sent = 0
    for batch in chunks(events, 500):
        records = [{"Data": json.dumps(e,separators=(",", ":"),default=str).encode("utf-8"),"PartitionKey": e["city"]} for e in batch]
        response = client.put_records(StreamName=settings.kinesis_stream, Records=records)
        failed = response.get("FailedRecordCount", 0)

        if failed:
            failures = [r for r in response["Records"]if r.get("ErrorCode") ]

            raise RuntimeError(f"Kinesis put_records failed for {failed} records: {failures}")

        sent += len(batch)

    return sent


def run(forecast_days: int = 3, mode: str = "kinesis") -> int:
    configure_logging()
    client = OpenMeteoClient()
    cities = load_cities()

    all_events: list[dict] = []
    for city in cities:
        events = client.fetch_city(city, forecast_days=forecast_days)
        logger.info("city=%s events=%s", city["city"], len(events))
        all_events.extend(events)

    sent = publish_events(all_events, mode=mode)
    logger.info("published_events=%s mode=%s", sent, mode)
    return sent


if __name__ == "__main__":
    run()
