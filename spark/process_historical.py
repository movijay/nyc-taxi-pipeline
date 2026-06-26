"""
process_historical.py
---------------------
PySpark script to process NYC TLC Yellow Taxi data for all years (2009–2023),
apply the same cleaning logic as the dbt staging model, compute daily revenue
aggregations, and write output as Parquet partitioned by year/month.

Deployment notes (AWS):
  EMR: spark-submit --deploy-mode cluster --master yarn process_historical.py
  Glue: Wrap as a Glue PySpark job; replace SparkSession with GlueContext.
        Use glueContext.create_dynamic_frame.from_options() for S3 reads.
        Output via glueContext.write_dynamic_frame_from_options() with
        connection_type='s3', format='parquet', partitionKeys=['year','month'].
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, TimestampType
)

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument(
    "--input-path",
    default="s3://nyc-tlc/trip data/yellow_tripdata_20*.parquet",
    help="S3 prefix or local glob for input Parquet files",
)
parser.add_argument(
    "--output-path",
    default="s3://your-bucket/nyc_taxi/agg_daily_revenue/",
    help="Output path for partitioned Parquet",
)
parser.add_argument(
    "--min-duration", type=int, default=1,
    help="Minimum valid trip duration in minutes",
)
parser.add_argument(
    "--max-duration", type=int, default=180,
    help="Maximum valid trip duration in minutes",
)
args, _ = parser.parse_known_args()

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("nyc_taxi_historical_processing")
    # Allow reading multiple Parquet schemas (columns vary across years)
    .config("spark.sql.parquet.mergeSchema", "true")
    # Use Kryo serializer for better performance on large shuffles
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Read all Parquet files
# ---------------------------------------------------------------------------
raw = spark.read.parquet(args.input_path)

# ---------------------------------------------------------------------------
# 2. Apply staging / cleaning logic (mirrors stg_yellow_trips.sql)
# ---------------------------------------------------------------------------
staged = (
    raw
    .withColumn("pickup_datetime",
                F.col("tpep_pickup_datetime").cast(TimestampType()))
    .withColumn("dropoff_datetime",
                F.col("tpep_dropoff_datetime").cast(TimestampType()))
    .withColumn("passenger_count",
                F.col("passenger_count").cast(IntegerType()))
    .withColumn("pickup_location_id",
                F.col("PULocationID").cast(IntegerType()))
    .withColumn("dropoff_location_id",
                F.col("DOLocationID").cast(IntegerType()))
    .withColumn("payment_type",
                F.col("payment_type").cast(IntegerType()))
    .withColumn("trip_distance_miles",
                F.col("trip_distance").cast(DoubleType()))
    .withColumn("fare_amount",
                F.col("fare_amount").cast(DoubleType()))
    .withColumn("tip_amount",
                F.col("tip_amount").cast(DoubleType()))
    .withColumn("total_amount",
                F.col("total_amount").cast(DoubleType()))
    .withColumn("trip_duration_minutes",
                (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0)
    # Filter invalid records (same rules as int_trips_enriched.sql)
    .filter(F.col("trip_distance_miles") > 0)
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("passenger_count") > 0)
    .filter(F.col("trip_duration_minutes") >= args.min_duration)
    .filter(F.col("trip_duration_minutes") <= args.max_duration)
    # Add partition columns
    .withColumn("year",  F.year("pickup_datetime"))
    .withColumn("month", F.month("pickup_datetime"))
    .withColumn("pickup_date", F.to_date("pickup_datetime"))
    .select(
        "pickup_datetime", "dropoff_datetime", "trip_duration_minutes",
        "passenger_count", "trip_distance_miles",
        "pickup_location_id", "dropoff_location_id",
        "fare_amount", "tip_amount", "total_amount", "payment_type",
        "pickup_date", "year", "month"
    )
)

# ---------------------------------------------------------------------------
# 3. Broadcast join with zone lookup table
#    Zone CSV is tiny (~265 rows) — broadcast avoids shuffle on the large table.
# ---------------------------------------------------------------------------
zone_lookup_path = "s3://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
# Uncomment below when running with S3 access; skip if not needed for aggregation
# zones = spark.read.option("header", "true").csv(zone_lookup_path)
# zones_b = F.broadcast(zones.select(
#     F.col("LocationID").cast(IntegerType()).alias("location_id"),
#     F.col("Zone").alias("zone_name"),
#     F.col("Borough").alias("borough")
# ))
# staged = staged.join(zones_b, staged.pickup_location_id == zones_b.location_id, "left")

# ---------------------------------------------------------------------------
# 4. Compute agg_daily_revenue (mirrors the dbt mart model)
#    Cache staged because we only aggregate once — cache prevents re-reading
#    the entire input when multiple downstream aggregations are needed.
# ---------------------------------------------------------------------------
staged.cache()
staged.count()  # materialise the cache

daily_revenue = (
    staged.groupBy("pickup_date", "year", "month")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("fare_amount"), 2).alias("total_fare"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.sum("tip_amount"), 2).alias("total_tips"),
        F.round(
            100.0 * F.sum("tip_amount") / F.nullif(F.sum("fare_amount"), F.lit(0)),
            2
        ).alias("tip_rate_pct"),
    )
)

# ---------------------------------------------------------------------------
# 5. Write partitioned Parquet output
#    repartition(1) per (year, month) keeps file count manageable;
#    for very large years use repartition(4) to stay under ~128MB per file.
# ---------------------------------------------------------------------------
(
    daily_revenue
    .repartition("year", "month")   # one task group per partition = clean files
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(args.output_path)
)

print(f"Done. Output written to {args.output_path}")
staged.unpersist()
spark.stop()
