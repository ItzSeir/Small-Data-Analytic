from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import csv,hashlib,json,os
spark=SparkSession.builder.appName("NYC-Taxi-Raw-Profiling").config("spark.sql.shuffle.partitions","2").config("spark.default.parallelism","2").getOrCreate()
OUT="/home/hadoop/nyc_results/profile"; os.makedirs(OUT,exist_ok=True)
monthly_rows=[]; null_rows=[]; schema_rows=[]; schema_groups={}; total_rows=0
for year in (2024,2025):
 for month_num in range(1,13):
  month=f"{year}-{month_num:02d}"; path=f"hdfs:///data/nyc/raw/yellow/yellow_tripdata_{month}.parquet"; print(f"PROFILING_MONTH: {month}",flush=True)
  try: df=spark.read.parquet(path)
  except Exception as exc: raise RuntimeError(f"PARQUET_READ_FAILED {month}: {exc}")
  columns=df.columns; schema_json=df.schema.json(); sig=hashlib.sha256(schema_json.encode()).hexdigest()[:16]; schema_groups.setdefault(sig,[]).append(month)
  for field in df.schema.fields: schema_rows.append({"month":month,"column":field.name,"data_type":field.dataType.simpleString(),"nullable":field.nullable,"schema_signature":sig})
  ex=[F.count(F.lit(1)).alias("row_count")]
  if "tpep_pickup_datetime" in columns: ex += [F.min("tpep_pickup_datetime").alias("pickup_min"),F.max("tpep_pickup_datetime").alias("pickup_max")]
  if "tpep_dropoff_datetime" in columns: ex += [F.min("tpep_dropoff_datetime").alias("dropoff_min"),F.max("tpep_dropoff_datetime").alias("dropoff_max")]
  ex += [F.sum(F.when(F.col(c).isNull(),1).otherwise(0)).alias("null__"+c) for c in columns]
  result=df.agg(*ex).collect()[0].asDict(); rc=int(result["row_count"]); total_rows+=rc
  monthly_rows.append({"month":month,"row_count":rc,"column_count":len(columns),"schema_signature":sig,"pickup_min":result.get("pickup_min"),"pickup_max":result.get("pickup_max"),"dropoff_min":result.get("dropoff_min"),"dropoff_max":result.get("dropoff_max")})
  null_rows += [{"month":month,"column":c,"null_count":int(result.get("null__"+c,0) or 0)} for c in columns]
  print(f"MONTH_RESULT: {month} rows={rc} columns={len(columns)} schema={sig}",flush=True); del df; spark.catalog.clearCache()
lookup=spark.read.option("header",True).option("inferSchema",True).csv("hdfs:///data/nyc/reference/taxi_zone_lookup.csv"); lookup_count=lookup.count()
if "LocationID" not in lookup.columns: raise RuntimeError("Lookup file does not contain LocationID")
st=lookup.agg(F.countDistinct("LocationID").alias("distinct_location_ids"),F.sum(F.when(F.col("LocationID").isNull(),1).otherwise(0)).alias("null_location_ids")).collect()[0].asDict(); distinct_ids=int(st["distinct_location_ids"]); null_ids=int(st["null_location_ids"] or 0); duplicate_ids=lookup_count-distinct_ids-null_ids
def write_csv(path,rows,fields):
 with open(path,"w",newline="",encoding="utf-8") as h:
  w=csv.DictWriter(h,fieldnames=fields); w.writeheader(); w.writerows(rows)
write_csv(os.path.join(OUT,"monthly_profile.csv"),monthly_rows,["month","row_count","column_count","schema_signature","pickup_min","pickup_max","dropoff_min","dropoff_max"])
write_csv(os.path.join(OUT,"monthly_null_counts.csv"),null_rows,["month","column","null_count"])
write_csv(os.path.join(OUT,"monthly_schema_columns.csv"),schema_rows,["month","column","data_type","nullable","schema_signature"])
with open(os.path.join(OUT,"schema_groups.json"),"w") as h: json.dump(schema_groups,h,indent=2)
lp=[{"row_count":lookup_count,"column_count":len(lookup.columns),"columns":"|".join(lookup.columns),"distinct_location_ids":distinct_ids,"duplicate_location_ids":duplicate_ids,"null_location_ids":null_ids}]
write_csv(os.path.join(OUT,"lookup_profile.csv"),lp,["row_count","column_count","columns","distinct_location_ids","duplicate_location_ids","null_location_ids"])
with open(os.path.join(OUT,"lookup_schema.json"),"w") as h: h.write(lookup.schema.json())
with open(os.path.join(OUT,"profile_summary.txt"),"w") as h: h.write(f"MONTHS_PROFILED=24\nTOTAL_RAW_ROWS={total_rows}\nSCHEMA_VARIANTS={len(schema_groups)}\nLOOKUP_ROWS={lookup_count}\nLOOKUP_DISTINCT_LOCATION_IDS={distinct_ids}\nLOOKUP_DUPLICATE_LOCATION_IDS={duplicate_ids}\nLOOKUP_NULL_LOCATION_IDS={null_ids}\n")
print("MONTHS_PROFILED: 24",flush=True); print("TOTAL_RAW_ROWS:",total_rows,flush=True); print("SCHEMA_VARIANTS:",len(schema_groups),flush=True); print("LOOKUP_ROWS:",lookup_count,flush=True); print("LOOKUP_DISTINCT_LOCATION_IDS:",distinct_ids,flush=True); print("LOOKUP_DUPLICATE_LOCATION_IDS:",duplicate_ids,flush=True); print("LOOKUP_NULL_LOCATION_IDS:",null_ids,flush=True); print("PART_6B_PROFILE_SUCCESS",flush=True); spark.stop()
