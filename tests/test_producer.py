from unittest.mock import Mock, patch

from src.producer.main import publish_events
from src.common.config import Settings

def test_publish_stdout(capsys):
    events = [{"city": "Pune", "x": 1}]

    count = publish_events(events, mode="stdout")

    assert count == 1
    assert '"city":"Pune"' in capsys.readouterr().out

@patch("src.producer.main.boto3.client")
def test_publish_kinesis(mock_client):
    kinesis = Mock()

    kinesis.put_records.return_value = {
        "FailedRecordCount": 0,
        "Records": [{
            "SequenceNumber": "1",
            "ShardId": "shardId-000"
        }]
    }

    mock_client.return_value = kinesis

    settings = Settings(
        aws_region="ap-south-1",
        kinesis_stream="weather-events",
        data_bucket="x",
        artifact_bucket="y",
        glue_database="weather_accuracy",
        log_level="INFO"
    )

    count = publish_events(
        [{"city": "Pune", "x": 1}],
        "kinesis",
        settings
    )

    assert count == 1
    assert kinesis.put_records.call_count == 1
