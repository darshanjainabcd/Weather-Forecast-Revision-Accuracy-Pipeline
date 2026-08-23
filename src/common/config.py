from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    aws_region: str = os.getenv("AWS_REGION", "ap-south-1")
    kinesis_stream: str = os.getenv("KINESIS_STREAM", "weather-events")
    data_bucket: str = os.getenv("DATA_BUCKET", "")
    artifact_bucket: str = os.getenv("ARTIFACT_BUCKET", "")
    glue_database: str = os.getenv("GLUE_DATABASE", "weather_accuracy")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


def load_cities(path: str | Path = "config/cities.json") -> listwith open(path, "r", encoding="utf-8") as f:
        cities = json.load(f)

    required = {"city", "latitude", "longitude", "timezone"}

    for row in cities:
        missing = required - row.keys()
        if missing:
            raise ValueError(
                f"City config missing fields {sorted(missing)}: {row}"
            )

    return cities
