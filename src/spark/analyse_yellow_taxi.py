from pyspark.sql import SparkSession, functions as F
import csv, os, math

INPUT = "hdfs:///data/nyc/processed/yellow_clean"
OUT = "/home/hadoop/nyc_results/analysis"
os.makedirs(OUT, exist_ok=True)

spark = (SparkSession.builder.appName("NYC-Taxi-Analytics")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4").getOrCreate())

df = spark.read.parquet(INPUT)
df.createOrReplaceTempView("yellow_taxi")

def clean_value(value):
    if hasattr(value, "isoformat"): return value.isoformat()
    return value

def write_csv(name, frame, order_cols=None):
    if order_cols: frame = frame.orderBy(*order_cols)
    rows = [r.asDict(recursive=True) for r in frame.collect()]
    fields = list(frame.columns)
    with open(os.path.join(OUT, name), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: clean_value(row.get(k)) for k in fields})
    return frame, rows

def avg_if(flag, col, alias):
    return F.avg(F.when(F.col(flag) == True, F.col(col))).alias(alias)

# DataFrame analyses
a1 = df.groupBy("pickup_year","pickup_month").agg(F.count("*").alias("trip_count"))
a1, r1 = write_csv("monthly_trip_demand.csv", a1, ["pickup_year","pickup_month"])
print("ANALYSIS_COMPLETE: A1", flush=True)

a2 = df.groupBy("weekend","pickup_hour").agg(F.count("*").alias("trip_count"))
a2, r2 = write_csv("hourly_weekend_demand.csv", a2, ["weekend","pickup_hour"])
print("ANALYSIS_COMPLETE: A2", flush=True)

a3 = df.groupBy("pickup_Borough").agg(
    F.count("*").alias("trip_count"),
    avg_if("valid_distance","trip_distance","avg_valid_trip_distance"),
    avg_if("valid_fare","fare_amount","avg_valid_fare_amount"),
    F.avg("trip_duration_min").alias("avg_trip_duration_min"))
a3, r3 = write_csv("pickup_borough_summary.csv", a3, ["pickup_Borough"])
print("ANALYSIS_COMPLETE: A3", flush=True)

a4 = df.groupBy("pickup_Borough","pickup_Zone").agg(F.count("*").alias("trip_count")).orderBy(F.desc("trip_count"),F.asc("pickup_Borough"),F.asc("pickup_Zone")).limit(20)
a4, r4 = write_csv("top20_pickup_zones.csv", a4)
print("ANALYSIS_COMPLETE: A4", flush=True)

a5 = df.groupBy("distance_band").agg(
    F.count("*").alias("trip_count"),
    F.avg("trip_duration_min").alias("avg_trip_duration_min"),
    avg_if("valid_fare","fare_amount","avg_valid_fare_amount"))
a5, r5 = write_csv("distance_band_summary.csv", a5, ["distance_band"])
print("ANALYSIS_COMPLETE: A5", flush=True)

a6 = df.groupBy("passenger_group").agg(
    F.count("*").alias("trip_count"),
    avg_if("valid_distance","trip_distance","avg_valid_trip_distance"),
    avg_if("valid_fare","fare_amount","avg_valid_fare_amount"))
a6, r6 = write_csv("passenger_group_summary.csv", a6, ["passenger_group"])
print("ANALYSIS_COMPLETE: A6", flush=True)

a7 = df.groupBy("payment_type").agg(
    F.count("*").alias("trip_count"),
    avg_if("valid_fare","fare_amount","avg_valid_fare_amount"),
    F.avg(F.when(F.col("valid_fare") == True, F.col("tip_amount"))).alias("avg_tip_amount_valid_fare"),
    F.avg(F.when(F.col("valid_fare") == True, F.col("total_amount"))).alias("avg_total_amount_valid_fare"))
a7, r7 = write_csv("payment_type_summary.csv", a7, ["payment_type"])
print("ANALYSIS_COMPLETE: A7", flush=True)

# SQL equivalents
s1 = spark.sql("""SELECT pickup_year, pickup_month, count(*) AS trip_count
                  FROM yellow_taxi GROUP BY pickup_year, pickup_month""")
s1, sr1 = write_csv("sql_monthly_trip_demand.csv", s1, ["pickup_year","pickup_month"])
print("SQL_COMPLETE: SQL1", flush=True)

s2 = spark.sql("""SELECT weekend, pickup_hour, count(*) AS trip_count
                  FROM yellow_taxi GROUP BY weekend, pickup_hour""")
s2, sr2 = write_csv("sql_hourly_weekend_demand.csv", s2, ["weekend","pickup_hour"])
print("SQL_COMPLETE: SQL2", flush=True)

s3 = spark.sql("""SELECT pickup_Borough, count(*) AS trip_count,
                  avg(CASE WHEN valid_distance THEN trip_distance END) AS avg_valid_trip_distance,
                  avg(CASE WHEN valid_fare THEN fare_amount END) AS avg_valid_fare_amount,
                  avg(trip_duration_min) AS avg_trip_duration_min
                  FROM yellow_taxi GROUP BY pickup_Borough""")
s3, sr3 = write_csv("sql_pickup_borough_summary.csv", s3, ["pickup_Borough"])
print("SQL_COMPLETE: SQL3", flush=True)

s4 = spark.sql("""SELECT distance_band, count(*) AS trip_count,
                  avg(trip_duration_min) AS avg_trip_duration_min,
                  avg(CASE WHEN valid_fare THEN fare_amount END) AS avg_valid_fare_amount
                  FROM yellow_taxi GROUP BY distance_band""")
s4, sr4 = write_csv("sql_distance_band_summary.csv", s4, ["distance_band"])
print("SQL_COMPLETE: SQL4", flush=True)

def validate(name, drows, srows, key_cols, avg_cols):
    def key(row): return tuple(row.get(k) for k in key_cols)
    dm, sm = {key(r):r for r in drows}, {key(r):r for r in srows}
    count_ok = set(dm) == set(sm) and all(int(dm[k]["trip_count"]) == int(sm[k]["trip_count"]) for k in dm)
    numeric_ok = True
    for k in dm:
        for col in avg_cols:
            a, b = dm[k].get(col), sm[k].get(col)
            if a is None and b is None: continue
            if a is None or b is None or abs(float(a)-float(b)) > 1e-8:
                numeric_ok = False
    return {"analysis":name, "dataframe_row_count":len(drows), "sql_row_count":len(srows),
            "count_totals_match":count_ok, "numeric_validation":"PASS" if numeric_ok else "FAIL",
            "status":"PASS" if count_ok and numeric_ok else "FAIL"}

validations = [
    validate("monthly_trip_demand", r1, sr1, ["pickup_year","pickup_month"], []),
    validate("hourly_weekend_demand", r2, sr2, ["weekend","pickup_hour"], []),
    validate("pickup_borough_summary", r3, sr3, ["pickup_Borough"], ["avg_valid_trip_distance","avg_valid_fare_amount","avg_trip_duration_min"]),
    validate("distance_band_summary", r5, sr4, ["distance_band"], ["avg_trip_duration_min","avg_valid_fare_amount"])
]
with open(os.path.join(OUT,"dataframe_sql_validation.csv"),"w",newline="",encoding="utf-8") as f:
    fields=["analysis","dataframe_row_count","sql_row_count","count_totals_match","numeric_validation","status"]
    w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(validations)

clean_total = df.count()
monthly_total = sum(int(r["trip_count"]) for r in r1)
distance_total = sum(int(r["trip_count"]) for r in r5)
passenger_total = sum(int(r["trip_count"]) for r in r6)
print("CLEAN_INPUT_ROWS:", clean_total, flush=True)
print("MONTHLY_TOTAL:", monthly_total, flush=True)
print("DISTANCE_BAND_TOTAL:", distance_total, flush=True)
print("PASSENGER_GROUP_TOTAL:", passenger_total, flush=True)
print("DATAFRAME_SQL_ALL_PASS:", all(r["status"] == "PASS" for r in validations), flush=True)
if clean_total != 89291810 or monthly_total != clean_total or distance_total != clean_total or passenger_total != clean_total:
    raise RuntimeError("Global count validation failed")
if not all(r["status"] == "PASS" for r in validations):
    raise RuntimeError("DataFrame SQL validation failed")
print("PART_9_10_ANALYSIS_SUCCESS", flush=True)
spark.stop()

