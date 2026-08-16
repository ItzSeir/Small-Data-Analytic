# IST3134 Big Data Analytics in the Cloud

## Cloud-Based Big Data Analysis of NYC TLC Yellow Taxi Trip Record Data

This repository supports the IST3134 group assignment using NYC Taxi and Limousine Commission (TLC) Yellow Taxi Trip Records from January 2024 to December 2025.

## Team

- Yeoh Jie Long (22071112) — AWS, Hadoop/HDFS/YARN, PySpark and Spark SQL
- Ooi Jin Yon (22039069) — Python pandas, visualisation and comparison

## Dataset

- NYC TLC Yellow Taxi Trip Records
- Period: January 2024 to December 2025
- 24 monthly Parquet files
- Taxi Zone Lookup CSV
- Official dataset source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

The raw monthly Parquet files are not stored in this repository. A verified dataset manifest and selected-file list are provided so the public dataset can be reproduced without duplicating the large raw files.

## Final Processing Architecture

Amazon S3 → EC2 Hadoop/HDFS/YARN → PySpark / Spark SQL → partitioned processed Parquet and compact analytical outputs.

The full-data Spark implementation was executed on a self-managed two-node EC2 Hadoop/HDFS/YARN cluster in AWS Academy Learner Lab.

The comparison implementation used local Python with pandas, PyArrow and Matplotlib.

## Key Results

- Raw records: 89,892,322
- Structurally valid records: 89,291,810
- Selected period: January 2024 to December 2025
- Busiest full-period pickup hour: 18:00
- Leading full-period pickup zone: JFK Airport
- January 2024 Spark-versus-pandas correctness validation: exact agreement for structurally valid rows and leading pickup zone
- Matched 1-, 3-, 6-, 12- and 24-month Spark-versus-pandas scalability benchmark: PASS at every scale for valid rows, leading pickup zone and top-zone trip count

## Repository Structure

- `config/` — preprocessing rules and example project configuration
- `data/` — dataset manifest and selected-file list; raw data excluded
- `src/spark/` — Spark profiling, preprocessing, analysis and benchmark scripts
- `src/pandas/` — pandas implementation
- `src/visualisation/` — visualisation scripts
- `tests/` — preprocessing boundary and output-schema tests
- `results/profile/` — compact Spark profiling outputs
- `results/preprocessing/` — compact preprocessing outputs
- `results/analysis/` — compact analytical outputs
- `results/benchmarks/` — Spark benchmark outputs, physical plans and matched comparison outputs
- `evidence/` — cleaned submission evidence and evidence index
- `docs/` — preprocessing documentation, execution log and evidence documentation
- `report/` — final submitted report when formatting is complete

## Spark Development Branch

The AWS/Spark implementation is maintained on the `jie-long-spark` branch.

Core Spark scripts include:

- `src/spark/profile_raw_data.py`
- `src/spark/preprocess_yellow_taxi.py`
- `src/spark/analyse_yellow_taxi.py`
- `src/spark/benchmark_spark.py`

## Reproducibility and Security

The repository intentionally excludes:

- raw Yellow Taxi Parquet files
- the large processed Parquet dataset
- AWS access keys, secret keys and session tokens
- PEM/private-key files
- large Spark/YARN runtime logs

Only source code, configuration, compact results, documentation and submission evidence are retained.
