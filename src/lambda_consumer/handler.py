from __future__ import annotations
import base64
import json
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone
import boto3

s3 = boto3.client("s3")
DATA_BUCKET = os.environ["DATA_BUCKET"]

def _partition_key(event: dict) -> tuple[str, str]:
    ts = datetime.fromisoformat(
        event["ingested_at"].replace("Z", "+00:00")
    )

    prefix = (
        f"bronze/weather/event_type={event['event_type']}/"
        f"year={ts:%Y}/month={ts:%m}/day={ts:%d}/hour={ts:%H}"
    )

    return event["event_type"], prefix

def lambda_handler(event, context):
    grouped: dict[str, list[str]] = defaultdict(list)
    failures = []

    for record in event.get("Records", []):
        event_id = record.get("eventID", str(uuid.uuid4()))

        try:
            raw = base64.b64decode(record["kinesis"]["data"])
            payload = json.loads(raw)

            _, prefix = _partition_key(payload)

            grouped[prefix].append(
                json.dumps(payload, separators=(",", ":"))
            )

        except Exception:
            failures.append({"itemIdentifier": event_id})

    for prefix, lines in grouped.items():
        key = (
            f"{prefix}/"
            f"batch-{datetime.now(timezone.utc):%Y%m%dT%H%M%S%fZ}-"
            f"{uuid.uuid4().hex}.json"
        )

        s3.put_object(
            Bucket=DATA_BUCKET,
            Key=key,
            Body=("\n".join(lines) + "\n").encode("utf-8"),
            ContentType="application/x-ndjson",
            ServerSideEncryption="AES256"
        )

    # Partial batch response lets Lambda retry only failed Kinesis records.
    return {"batchItemFailures": failures}
``
