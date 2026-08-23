from __future__ import annotations
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATA_BUCKET", "PROCESS_DATE", "EVENT_TYPE"])

spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

bucket = args["DATA_BUCKET"]
process_date = args["PROCESS_DATE"]
event_type = args["EVENT_TYPE"]

y, m, d = process_date.split("-")

source = (
    f"s3://{bucket}/bronze/weather/event_type={event_type}/"
    f"year={y}/month={m}/day={d}/"
)

common_fields = [
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("source", StringType()),
    StructField("city", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("temperature_2m_c", DoubleType()),
    StructField("wind_speed_10m_kmh", DoubleType()),
    StructField("ingested_at", StringType())
]

if event_type == "forecast":
    schema = StructType(common_fields + [
        StructField("issued_at", StringType()),
        StructField("forecast_timestamp", StringType()),
        StructField("precipitation_probability_pct", DoubleType())
    ])
else:
    schema = StructType(common_fields + [
        StructField("observation_timestamp", StringType()),
        StructField("precipitation_mm", DoubleType())
    ])

df = spark.read.schema(schema).json(source)

df = df.withColumn("city", F.trim(F.col("city"))) \
       .withColumn("source", F.lower(F.trim(F.col("source")))) \
       .withColumn("ingested_at", F.to_timestamp("ingested_at"))

if event_type == "forecast":
    df = df.withColumn("issued_at", F.to_timestamp("issued_at")) \
           .withColumn("forecast_timestamp", F.to_timestamp("forecast_timestamp")) \
           .withColumn("forecast_date", F.to_date("forecast_timestamp"))

    valid = (
        F.col("event_id").isNotNull() &
        F.col("city").isNotNull() &
        F.col("issued_at").isNotNull() &
        F.col("forecast_timestamp").isNotNull() &
        (F.col("forecast_timestamp") >= F.col("issued_at")) &
        F.col("temperature_2m_c").between(-100.0, 70.0) &
        (
            F.col("precipitation_probability_pct").isNull() |
            F.col("precipitation_probability_pct").between(0.0, 100.0)
        )
    )
else:
    df = df.withColumn("observation_timestamp", F.to_timestamp("observation_timestamp")) \
           .withColumn("observation_date", F.to_date("observation_timestamp"))

    valid = (
        F.col("event_id").isNotNull() &
        F.col("city").isNotNull() &
        F.col("observation_timestamp").isNotNull() &
        F.col("temperature_2m_c").between(-100.0, 70.0)
    )

good = df.filter(valid)
bad = df.filter(~valid)

w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())

good = good.withColumn("_rn", F.row_number().over(w)) \
           .filter(F.col("_rn") == 1) \
           .drop("_rn")

if event_type == "forecast":
    target = f"s3://{bucket}/silver/forecast/"
    good.repartition("forecast_date") \
        .write.mode("overwrite") \
        .partitionBy("forecast_date") \
        .parquet(target)
else:
    target = f"s3://{bucket}/silver/observation/"
    good.repartition("observation_date") \
        .write.mode("overwrite") \
        .partitionBy("observation_date") \
        .parquet(target)

if bad.limit(1).count() > 0:
    bad.write.mode("append").json(
        f"s3://{bucket}/quarantine/{event_type}/process_date={process_date}/"
    )

metrics = {
    "input_count": df.count(),
    "valid_count": good.count(),
    "invalid_count": bad.count()
}

print(f"DATA_QUALITY_METRICS={metrics}")
