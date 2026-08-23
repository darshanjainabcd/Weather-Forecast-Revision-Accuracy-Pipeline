from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator

DATA_BUCKET = Variable.get("weather_data_bucket")
ARTIFACT_BUCKET = Variable.get("weather_artifact_bucket")
REDSHIFT_WORKGROUP = Variable.get("redshift_workgroup")
REDSHIFT_DATABASE = Variable.get("redshift_database")
REDSHIFT_COPY_ROLE_ARN = Variable.get("redshift_copy_role_arn")
EMR_APPLICATION_ID = Variable.get("emr_serverless_application_id")
EMR_EXECUTION_ROLE_ARN = Variable.get("emr_serverless_execution_role_arn")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="weather_forecast_accuracy",
    description="Forecast revision and accuracy pipeline",
    schedule="20 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "aws", "pyspark"]
) as dag:

    process_date = "{{ ds }}"

    wait_for_forecast_bronze = S3KeySensor(
        task_id="wait_for_forecast_bronze",
        bucket_name=DATA_BUCKET,
        bucket_key=(
            "bronze/weather/event_type=forecast/"
            "year={{ data_interval_start.strftime('%Y') }}/"
            "month={{ data_interval_start.strftime('%m') }}/"
            "day={{ data_interval_start.strftime('%d') }}/*"
        ),
        wildcard_match=True,
        timeout=900,
        poke_interval=60
    )

    forecast_silver = GlueJobOperator(
        task_id="forecast_silver",
        job_name="weather-silver-etl",
        script_args={
            "--DATA_BUCKET": DATA_BUCKET,
            "--PROCESS_DATE": process_date,
            "--EVENT_TYPE": "forecast"
        },
        wait_for_completion=True
    )

    observation_silver = GlueJobOperator(
        task_id="observation_silver",
        job_name="weather-silver-etl",
        script_args={
            "--DATA_BUCKET": DATA_BUCKET,
            "--PROCESS_DATE": process_date,
            "--EVENT_TYPE": "observation"
        },
        wait_for_completion=True
    )

    copy_forecast = RedshiftDataOperator(
        task_id="copy_forecast_to_stage",
        workgroup_name=REDSHIFT_WORKGROUP,
        database=REDSHIFT_DATABASE,
        sql=f"""
        TRUNCATE TABLE weather.stg_forecast;

        COPY weather.stg_forecast
        FROM 's3://{DATA_BUCKET}/silver/forecast/forecast_date={process_date}/'
        IAM_ROLE '{REDSHIFT_COPY_ROLE_ARN}'
        FORMAT AS PARQUET;
        """
    )

    merge_forecast = RedshiftDataOperator(
        task_id="merge_forecast_scd2",
        workgroup_name=REDSHIFT_WORKGROUP,
        database=REDSHIFT_DATABASE,
        sql="sql/02_scd2_forecast_merge.sql"
    )

    copy_observation = RedshiftDataOperator(
        task_id="copy_observation_to_stage",
        workgroup_name=REDSHIFT_WORKGROUP,
        database=REDSHIFT_DATABASE,
        sql=f"""
        TRUNCATE TABLE weather.stg_observation;

        COPY weather.stg_observation
        FROM 's3://{DATA_BUCKET}/silver/observation/observation_date={process_date}/'
        IAM_ROLE '{REDSHIFT_COPY_ROLE_ARN}'
        FORMAT AS PARQUET;
        """
    )

    merge_observation = RedshiftDataOperator(
        task_id="merge_observation",
        workgroup_name=REDSHIFT_WORKGROUP,
        database=REDSHIFT_DATABASE,
        sql="sql/03_observation_merge.sql"
    )

    accuracy = EmrServerlessStartJobOperator(
        task_id="calculate_accuracy",
        application_id=EMR_APPLICATION_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": (
                    f"s3://{ARTIFACT_BUCKET}/"
                    "artifacts/jobs/emr/forecast_accuracy.py"
                ),
                "entryPointArguments": [
                    "--bucket", DATA_BUCKET,
                    "--target-date", process_date
                ]
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{DATA_BUCKET}/logs/emr/"
                }
            }
        },
        wait_for_completion=True
    )

    quality_check = RedshiftDataOperator(
        task_id="quality_check",
        workgroup_name=REDSHIFT_WORKGROUP,
        database=REDSHIFT_DATABASE,
        sql="""
        SELECT 1 / CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
        FROM (
            SELECT source_key, city_key, issued_at, forecast_timestamp
            FROM weather.fact_forecast_revision
            WHERE is_current = TRUE
            GROUP BY 1,2,3,4
            HAVING COUNT(*) > 1
        );
        """
    )

    wait_for_forecast_bronze >> [forecast_silver, observation_silver]

    forecast_silver >> copy_forecast >> merge_forecast

    observation_silver >> copy_observation >> merge_observation

    [merge_forecast, merge_observation] >> accuracy >> quality_check
