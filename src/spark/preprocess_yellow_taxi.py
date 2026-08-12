from pyspark.sql import SparkSession, functions as F
import csv
import os
import subprocess

RAW = "hdfs:///data/nyc/raw/yellow"
LOOKUP = "hdfs:///data/nyc/reference/taxi_zone_lookup.csv"
STAGE = "/data/nyc/processed/yellow_clean_staging"
FINAL = "/data/nyc/processed/yellow_clean"
LOCAL = "/home/hadoop/nyc_results/preprocessing"
os.makedirs(LOCAL, exist_ok=True)

spark = (SparkSession.builder.appName("NYC-Taxi-Preprocessing")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "2").getOrCreate())

lookup = (spark.read.option("header", True).option("inferSchema", True).csv(LOOKUP)
    .select(F.col("LocationID").cast("int").alias("lookup_LocationID"),
            F.col("Borough").alias("pickup_Borough"),
            F.col("Zone").alias("pickup_Zone"),
            F.col("service_zone").alias("pickup_service_zone")))

def available(df, names):
    found = {c.lower(): c for c in df.columns}
    for name in names:
        if name.lower() in found:
            return found[name.lower()]
    return None

def typed(df, names, data_type):
    name = available(df, names)
    return F.col(name).cast(data_type) if name else F.lit(None).cast(data_type)

def metric(condition, name):
    return F.sum(F.when(condition, 1).otherwise(0)).cast("long").alias(name)

quality = []
totals = {"raw_rows": 0, "structurally_valid_rows": 0,
          "structurally_removed_rows": 0, "nonpositive_distance": 0,
          "nonpositive_fare": 0, "unmatched_pickup_lookup": 0}

for year in (2024, 2025):
    for month_num in range(1, 13):
        source_month = f"{year}-{month_num:02d}"
        print(f"PREPROCESSING_MONTH: {source_month}", flush=True)
        source = spark.read.parquet(f"{RAW}/yellow_tripdata_{source_month}.parquet")
        base = source.select(
            typed(source, ["VendorID"], "int").alias("VendorID"),
            typed(source, ["tpep_pickup_datetime"], "timestamp").alias("tpep_pickup_datetime"),
            typed(source, ["tpep_dropoff_datetime"], "timestamp").alias("tpep_dropoff_datetime"),
            typed(source, ["passenger_count"], "double").alias("passenger_count"),
            typed(source, ["trip_distance"], "double").alias("trip_distance"),
            typed(source, ["RatecodeID"], "int").alias("RatecodeID"),
            typed(source, ["store_and_fwd_flag"], "string").alias("store_and_fwd_flag"),
            typed(source, ["PULocationID"], "int").alias("PULocationID"),
            typed(source, ["DOLocationID"], "int").alias("DOLocationID"),
            typed(source, ["payment_type"], "int").alias("payment_type"),
            typed(source, ["fare_amount"], "double").alias("fare_amount"),
            typed(source, ["extra"], "double").alias("extra"),
            typed(source, ["mta_tax"], "double").alias("mta_tax"),
            typed(source, ["tip_amount"], "double").alias("tip_amount"),
            typed(source, ["tolls_amount"], "double").alias("tolls_amount"),
            typed(source, ["improvement_surcharge"], "double").alias("improvement_surcharge"),
            typed(source, ["total_amount"], "double").alias("total_amount"),
            typed(source, ["congestion_surcharge"], "double").alias("congestion_surcharge"),
            typed(source, ["Airport_fee", "airport_fee"], "double").alias("airport_fee"),
            typed(source, ["cbd_congestion_fee", "CBD_congestion_fee"], "double").alias("cbd_congestion_fee"))

        tagged = (base
            .withColumn("pickup_year", F.year("tpep_pickup_datetime").cast("int"))
            .withColumn("pickup_month", F.month("tpep_pickup_datetime").cast("int"))
            .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
            .withColumn("pickup_hour", F.hour("tpep_pickup_datetime").cast("int"))
            .withColumn("weekend", F.dayofweek("tpep_pickup_datetime").isin(1, 7))
            .withColumn("trip_duration_min",
                ((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / F.lit(60.0)).cast("double")))

        pickup = F.col("tpep_pickup_datetime")
        dropoff = F.col("tpep_dropoff_datetime")
        duration = F.col("trip_duration_min")
        month_match = ((F.col("pickup_year") == F.lit(year)) &
                       (F.col("pickup_month") == F.lit(month_num)))
        structural_valid = (pickup.isNotNull() & dropoff.isNotNull() &
            (dropoff > pickup) & (duration > 0) & (duration <= 240) &
            month_match & F.col("PULocationID").isNotNull() &
            F.col("DOLocationID").isNotNull())

        tagged = (tagged.withColumn("structurally_valid", structural_valid)
            .withColumn("valid_distance", F.col("trip_distance") > 0)
            .withColumn("valid_fare", F.col("fare_amount") > 0)
            .withColumn("distance_band",
                F.when((F.col("trip_distance") > 0) & (F.col("trip_distance") < 2), "0-<2")
                 .when((F.col("trip_distance") >= 2) & (F.col("trip_distance") < 5), "2-<5")
                 .when((F.col("trip_distance") >= 5) & (F.col("trip_distance") < 10), "5-<10")
                 .when((F.col("trip_distance") >= 10) & (F.col("trip_distance") < 20), "10-<20")
                 .when(F.col("trip_distance") >= 20, "20+")
                 .otherwise("Invalid/Non-positive"))
            .withColumn("passenger_group",
                F.when(F.col("passenger_count").isNull(), "Missing")
                 .when(F.col("passenger_count") == 0, "0")
                 .when((F.col("passenger_count") >= 1) & (F.col("passenger_count") <= 6), "1-6")
                 .otherwise("7+"))
            .withColumn("source_month", F.lit(source_month))
            .join(lookup, F.col("PULocationID") == F.col("lookup_LocationID"), "left"))

        row = tagged.agg(
            F.count(F.lit(1)).cast("long").alias("raw_rows"),
            metric(pickup.isNull(), "null_pickup_timestamp"),
            metric(dropoff.isNull(), "null_dropoff_timestamp"),
            metric(pickup.isNotNull() & dropoff.isNotNull() & ~(dropoff > pickup), "dropoff_not_after_pickup"),
            metric(duration.isNotNull() & (duration <= 0), "duration_nonpositive"),
            metric(duration > 240, "duration_over_240"),
            metric(pickup.isNotNull() & ~month_match, "source_month_mismatch"),
            metric(F.col("PULocationID").isNull(), "null_PULocationID"),
            metric(F.col("DOLocationID").isNull(), "null_DOLocationID"),
            metric((F.col("trip_distance").isNull()) | (F.col("trip_distance") <= 0), "nonpositive_distance"),
            metric((F.col("fare_amount").isNull()) | (F.col("fare_amount") <= 0), "nonpositive_fare"),
            metric(F.col("passenger_count").isNull(), "missing_passenger_count"),
            metric(F.col("structurally_valid"), "structurally_valid_rows"),
            metric(F.col("lookup_LocationID").isNull(), "unmatched_pickup_lookup")).collect()[0].asDict()
        row["month"] = source_month
        row["structurally_removed_rows"] = row["raw_rows"] - row["structurally_valid_rows"]
        quality.append(row)
        for key in totals:
            totals[key] += int(row[key] or 0)

        clean = tagged.filter(F.col("structurally_valid")).drop("structurally_valid", "lookup_LocationID")
        stage_month = f"{STAGE}/pickup_year={year}/pickup_month={month_num}"
        clean.write.mode("overwrite").parquet(stage_month)
        print(f"MONTH_PREPROCESS_RESULT: {source_month} raw={row['raw_rows']} valid={row['structurally_valid_rows']} removed={row['structurally_removed_rows']}", flush=True)
        spark.catalog.clearCache()

if subprocess.run(["hdfs", "dfs", "-test", "-e", FINAL]).returncode == 0:
    raise RuntimeError(f"Final output already exists: {FINAL}")
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", FINAL], check=True)
for year in (2024, 2025):
    parent = f"{FINAL}/pickup_year={year}"
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", parent], check=True)
    for month_num in range(1, 13):
        subprocess.run(["hdfs", "dfs", "-mv",
            f"{STAGE}/pickup_year={year}/pickup_month={month_num}", parent], check=True)

fields = ["month", "raw_rows", "null_pickup_timestamp", "null_dropoff_timestamp",
 "dropoff_not_after_pickup", "duration_nonpositive", "duration_over_240",
 "source_month_mismatch", "null_PULocationID", "null_DOLocationID",
 "nonpositive_distance", "nonpositive_fare", "missing_passenger_count",
 "structurally_valid_rows", "structurally_removed_rows", "unmatched_pickup_lookup"]
with open(f"{LOCAL}/monthly_quality.csv", "w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    writer.writerows(quality)
with open(f"{LOCAL}/preprocessing_summary.txt", "w", encoding="utf-8") as handle:
    handle.write(f"MONTHS_PROCESSED=24\n")
    handle.write(f"TOTAL_RAW_ROWS={totals['raw_rows']}\n")
    handle.write(f"TOTAL_STRUCTURALLY_VALID_ROWS={totals['structurally_valid_rows']}\n")
    handle.write(f"TOTAL_STRUCTURALLY_REMOVED_ROWS={totals['structurally_removed_rows']}\n")
    handle.write(f"TOTAL_NONPOSITIVE_DISTANCE={totals['nonpositive_distance']}\n")
    handle.write(f"TOTAL_NONPOSITIVE_FARE={totals['nonpositive_fare']}\n")
    handle.write(f"TOTAL_UNMATCHED_PICKUP_LOOKUP={totals['unmatched_pickup_lookup']}\n")

print("PART_7_8_PREPROCESS_SUCCESS", flush=True)
spark.stop()

