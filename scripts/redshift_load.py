from __future__ import annotations
import argparse
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

def execute(sql: str, database: str, workgroup: str) -> str:
    client = boto3.client(
        "redshift-data",
        region_name=os.getenv("AWS_REGION")
    )

    response = client.execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        Sql=sql
    )

    return response["Id"]

def main():
    p = argparse.ArgumentParser()

    p.add_argument("--bucket", required=True)
    p.add_argument("--iam-role", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--workgroup", required=True)
    p.add_argument("--process-date", required=True)
    p.add_argument(
        "--event-type",
        choices=["forecast", "observation"],
        required=True
    )

    a = p.parse_args()

    if a.event_type == "forecast":
        table = "weather.stg_forecast"
        path = f"s3://{a.bucket}/silver/forecast/forecast_date={a.process_date}/"
    else:
        table = "weather.stg_observation"
        path = f"s3://{a.bucket}/silver/observation/observation_date={a.process_date}/"

    sql = f"""
    TRUNCATE TABLE {table};

    COPY {table}
    FROM '{path}'
    IAM_ROLE '{a.iam_role}'
    FORMAT AS PARQUET;
    """

    statement_id = execute(sql, a.database, a.workgroup)
    print(statement_id)

if __name__ == "__main__":
    main()
