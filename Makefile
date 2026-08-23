install:

Weather Forecast Revision &amp; Accuracy Pipeline

Complete GitHub Project Guide

pip install -r requirements-dev.txt
test:
pytest -q
lint:
ruff check .
producer-dry:
python scripts/run_producer.py --mode stdout --forecast-days 3
producer-kinesis:
python scripts/run_producer.py --mode kinesis --forecast-days 3
