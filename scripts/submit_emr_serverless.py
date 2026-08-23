from __future__ import annotations
import argparse
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

def main():
    p = argparse.ArgumentParser()

    p.add_argument("--application-id", required=True)
    p.add_argument("--execution-role-arn", required=True)
    p.add_argument("--artifact-bucket", required=True)
    p.add_argument("--data-bucket", required=True)
    p.add_argument("--target-date", required=True)

    a = p.parse_args()

    client = boto3.client(
        "emr-serverless",
        region_name=os.getenv("AWS_REGION")
    )

    response = client.start_job_run(
        applicationId=a.application_id,
        executionRoleArn=a.execution_role_arn,
        name=f"weather-accuracy-{a.target_date}",
        jobDriver={
            "sparkSubmit": {
                "entryPoint": (
                    f"s3://{a.artifact_bucket}/"
                    "artifacts/jobs/emr/forecast_accuracy.py"
                ),
                "entryPointArguments": [
                    "--bucket", a.data_bucket,
                    "--target-date", a.target_date
                ],
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=2 "
                    "--conf spark.executor.memory=4g "
                    "--conf spark.driver.cores=1 "
                    "--conf spark.driver.memory=2g"
                )
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{a.data_bucket}/logs/emr/"
                }
            }
        }
    )

    print(response["jobRunId"])

if __name__ == "__main__":
    main()
