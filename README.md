# Weather Forecast Revision &amp; Accuracy Pipeline
Production-style AWS Data Engineering project that captures hourly weather forecasts and observations, retains
every forecast revision, and measures forecast accuracy by city and lead time.
## Tech Stack
Amazon Kinesis Data Streams, AWS Lambda, Amazon S3, AWS Glue, PySpark, Amazon EMR Serverless, Amazon Redshift,
Amazon MWAA, Amazon Athena, AWS Glue Data Catalog, AWS Lake Formation, Amazon CloudWatch, Python, SQL, Terraform,
GitHub Actions.
## Business Goal
Weather forecasts are revised repeatedly before the target hour. This project preserves every revision using SCD
Type 2 and compares each revision against the actual observed reading, making it possible to measure how forecast
accuracy changes as the target hour approaches.
## Architecture
Open-Meteo -&gt; Python Producer -&gt; Kinesis -&gt; Lambda -&gt; S3 Bronze
-&gt; Glue PySpark -&gt; S3 Silver -&gt; Redshift
-&gt; EMR PySpark -&gt; S3 Gold -&gt; Athena / BI
MWAA orchestrates the workflow. CloudWatch provides logging/metrics/alarms. Glue Catalog and Lake Formation govern
the lake.
## Core Engineering Features
- Streaming ingestion with Kinesis.
- Serverless raw landing with Lambda.
- Bronze/Silver/Gold lake design.
- Incremental PySpark transformations.
- Deterministic deduplication and idempotency.
- Parquet + partition pruning.
- Redshift star schema.
- SCD Type 2 forecast revision history.
- MERGE/UPSERT for late or corrected observations.
- Nightly forecast-vs-actual accuracy computation.
- MWAA retries and dependency management.
- CloudWatch data-quality and ingestion-lag monitoring.
- Glue Catalog + Lake Formation governance.

Weather Forecast Revision &amp; Accuracy Pipeline

Complete GitHub Project Guide

- Unit tests and GitHub Actions CI.
- Infrastructure as Code with Terraform.
## Data Source
The example uses Open-Meteo because it is simple for a portfolio project and does not require embedding an API key.
The producer is isolated behind a client class, so another provider can be substituted without changing the
downstream architecture.
## Local Setup
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate
pip install -r requirements-dev.txt
copy .env.example .env
pytest -q
```
Dry-run producer:
```bash
python scripts/run_producer.py --mode stdout --forecast-days 3
```
Publish to Kinesis:
```bash
python scripts/run_producer.py --mode kinesis --forecast-days 3
```
## AWS Deployment
```bash
cd infrastructure/terraform
terraform init
terraform plan
terraform apply
```
Upload Spark artifacts:
```bash
python scripts/upload_artifacts.py
```
Create/configure an MWAA environment and upload `dags/weather_pipeline_dag.py` into the configured DAG S3 location.
## S3 Layout

Weather Forecast Revision &amp; Accuracy Pipeline

Complete GitHub Project Guide

```text
bronze/weather/event_type=forecast/year=YYYY/month=MM/day=DD/hour=HH/
bronze/weather/event_type=observation/year=YYYY/month=MM/day=DD/hour=HH/
silver/forecast/forecast_date=YYYY-MM-DD/
silver/observation/observation_date=YYYY-MM-DD/
gold/forecast_accuracy/target_date=YYYY-MM-DD/
```
## Redshift Model
Dimensions:
- dim_city
- dim_time
- dim_weather_source
Facts:
- fact_forecast_revision
- fact_observation
The forecast fact is SCD Type 2 and includes `effective_from`, `effective_to`, and `is_current`.
## Accuracy Metrics
For each forecast revision:
```text
lead_hours = forecast_timestamp - issued_at
temperature_error = forecast_temperature - observed_temperature
absolute_error = abs(temperature_error)
squared_error = temperature_error^2
```
Aggregate measures:
- MAE
- RMSE
- Mean Bias Error
- Accuracy by city
- Accuracy by source
- Accuracy by lead-time bucket
## Data Quality Rules
- city cannot be null
- timestamp must parse successfully
- forecast target must not be before issued_at
- temperature expected within configured physical range
- event_id must be unique after Silver deduplication
- forecast-to-observation join coverage must exceed configured threshold
- SCD2 table may contain only one current row per natural key
## Security

Weather Forecast Revision &amp; Accuracy Pipeline

Complete GitHub Project Guide

- No credentials are committed.
- IAM roles use least-privilege access.
- S3 encryption and public-access block are enabled.
- Lake Formation governs curated datasets.
- Secrets such as Redshift credentials should be kept in AWS Secrets Manager.
- MWAA/Glue/EMR use execution roles rather than static keys.
## Testing
```bash
pytest -q
ruff check .
```
CI runs on every pull request and push to `main`.
## Cost Control
For a portfolio deployment:
- keep Kinesis at one shard or use on-demand briefly;
- use short Glue/EMR Serverless runs;
- stop/delete Redshift resources when not testing;
- apply S3 lifecycle rules;
- use a small city list and limited forecast horizon.
