import os
from pathlib import Path
import boto3
from dotenv import load_dotenv

load_dotenv()

bucket = os.environ["ARTIFACT_BUCKET"]
s3 = boto3.client("s3")

files = [
    "jobs/glue/silver_etl.py",
    "jobs/emr/forecast_accuracy.py",
    "dags/weather_pipeline_dag.py"
]

for file in files:
    key = f"artifacts/{file}"

    print(f"upload {file} -> s3://{bucket}/{key}")

    s3.upload_file(
        file,
        bucket,
        key
    )
