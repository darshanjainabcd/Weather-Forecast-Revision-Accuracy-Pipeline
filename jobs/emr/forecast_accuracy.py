import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def args():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--target-date", required=True)  # YYYY-MM-DD
    return p.parse_args()

a = args()

spark = SparkSession.builder.appName("weather-forecast-accuracy").getOrCreate()

forecast_path = f"s3://{a.bucket}/silver/forecast/"
observation_path = f"s3://{a.bucket}/silver/observation/"
gold_path = f"s3://{a.bucket}/gold/forecast_accuracy/"

forecast = (
    spark.read.parquet(forecast_path)
    .filter(F.to_date("forecast_timestamp") == F.lit(a.target_date))
    .select(
        "event_id",
        "source",
        "city",
        "issued_at",
        "forecast_timestamp",
        F.col("temperature_2m_c").alias("forecast_temperature_c"),
        "precipitation_probability_pct",
        F.col("wind_speed_10m_kmh").alias("forecast_wind_kmh"),
    )
)

actual = (
    spark.read.parquet(observation_path)
    .filter(F.to_date("observation_timestamp") == F.lit(a.target_date))
    .select(
        F.col("source").alias("observation_source"),
        "city",
        "observation_timestamp",
        F.col("temperature_2m_c").alias("observed_temperature_c"),
        "precipitation_mm",
        F.col("wind_speed_10m_kmh").alias("observed_wind_kmh"),
    )
)

joined = forecast.join(
    actual,
    on=[
        forecast.city == actual.city,
        forecast.forecast_timestamp == actual.observation_timestamp
    ],
    how="inner"
)

result = (
    joined
    .withColumn(
        "lead_hours",
        (F.unix_timestamp("forecast_timestamp") - F.unix_timestamp("issued_at")) / F.lit(3600.0)
    )
    .withColumn(
        "temperature_error_c",
        F.col("forecast_temperature_c") - F.col("observed_temperature_c")
    )
    .withColumn(
        "temperature_absolute_error_c",
        F.abs(F.col("temperature_error_c"))
    )
    .withColumn(
        "temperature_squared_error",
        F.pow(F.col("temperature_error_c"), 2)
    )
    .withColumn(
        "lead_time_bucket",
        F.when(F.col("lead_hours") <= 6, "00-06h")
         .when(F.col("lead_hours") <= 12, "07-12h")
         .when(F.col("lead_hours") <= 24, "13-24h")
         .when(F.col("lead_hours") <= 48, "25-48h")
         .when(F.col("lead_hours") <= 72, "49-72h")
         .otherwise("72h+")
    )
    .withColumn("target_date", F.to_date("forecast_timestamp"))
    .select(
        "event_id",
        "source",
        "observation_source",
        "city",
        "issued_at",
        "forecast_timestamp",
        "lead_hours",
        "lead_time_bucket",
        "forecast_temperature_c",
        "observed_temperature_c",
        "temperature_error_c",
        "temperature_absolute_error_c",
        "temperature_squared_error",
        "precipitation_probability_pct",
        "precipitation_mm",
        "forecast_wind_kmh",
        "observed_wind_kmh",
        "target_date",
    )
)

result.repartition("target_date") \
      .write.mode("overwrite") \
      .partitionBy("target_date") \
      .parquet(gold_path)

summary = (
    result.groupBy("city", "lead_time_bucket")
    .agg(
        F.count("*").alias("matched_forecasts"),
        F.avg("temperature_absolute_error_c").alias("temperature_mae_c"),
        F.sqrt(F.avg("temperature_squared_error")).alias("temperature_rmse_c"),
        F.avg("temperature_error_c").alias("temperature_bias_c")
    )
    .orderBy("city", "lead_time_bucket")
)

summary.show(200, truncate=False)
