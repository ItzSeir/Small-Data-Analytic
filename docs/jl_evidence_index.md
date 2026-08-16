# JL AWS / Spark Evidence Index

This index documents the final cleaned AWS/Spark evidence used to support the IST3134 report. Blank, misleading, duplicate and obsolete screenshots were removed before publication. No raw Parquet files, AWS credentials, session tokens or private-key files are included.

## Main report evidence

| Evidence | Report location | Purpose |
|---|---|---|
| E11_PROFILE_SUMMARY.png | Figure 2.2.1 | Spark raw-data profiling summary for the 24-month dataset |
| E12_SCHEMA_VARIANTS.png | Figure 2.2.2 | Two raw schema variants across 2024 and 2025 |
| E27_GITHUB_SPARK_SCRIPTS.png | Figure 4.1.1 | Core PySpark source scripts in the Spark development branch |
| E01_S3_BUCKET_RAW_DATA.png | Figure 4.2.1 | Private S3 raw layer containing the 24 monthly Parquet files |
| E03_S3_SECURITY.png | Figure 4.2.2 | S3 Block Public Access configuration |
| E05_EC2_CLUSTER.png | Figure 4.3.1 | Final two-node t3.small EC2 Spark cluster |
| E63_FINAL_CLUSTER_RESOURCE_HEALTH.png | Figure 4.3.2 | Final Hadoop/YARN master/worker daemon and node health |
| E13_PREPROCESSING_SUMMARY.png | Figure 4.4.1 | Spark preprocessing totals for all 24 months |
| E21_DATAFRAME_SQL_VALIDATION.png | Figure 4.5.1 | DataFrame-versus-Spark-SQL validation |
| E22_BENCHMARK_RESULTS.png | Figure 4.6.1 | Initial Spark benchmark runtimes |
| E23_PARTITION_PRUNING_PLAN.png | Figure 4.6.2 | January 2024 partition-pruning physical plan |
| E24_EXCHANGE_SHUFFLE_PLAN.png | Figure 4.6.3 | Exchange/shuffle physical-plan evidence |
| E58_FINAL_CLEAN_SPARK_BENCHMARK.png | Figure 4.6.4 | Clean matched Spark benchmark across five scales |
| E60_CLEAN_24_MONTH_REPLACEMENT.png | Figure 4.6.5 | Clean 24-month replacement run |
| E62_FINAL_SPARK_VALIDATION_SUMMARY.png | Figure 4.6.6 | Final Spark medians and validation summary |
| E32_GITHUB_DOCUMENTATION.png | Figure 4.9.1 | Repository execution/evidence documentation |
| E61_EXECUTOR137_FAULT_RECOVERY.png | Figure 4.9.2 | Executor-exit-137 recovery evidence |
| E18_TOP20_PICKUP_ZONES.png | Figure 5.4.1 | Spark top-20 pickup-zone output |
| E19_BOROUGH_SUMMARY.png | Figure 5.4.2 | Spark pickup-borough output |
| E59_SPARK_PANDAS_MATCHED_COMPARISON.png | Figure 6.2.1 | Final matched Spark-versus-pandas comparison |
| E57_CONTROLLED_SPARK_RESOURCE_CONFIGURATION.png | Figure 6.3.1 | Controlled Spark benchmark resource configuration |

## Supporting evidence

- E02_S3_REFERENCE_DATA.png — S3 reference layer containing `taxi_zone_lookup.csv`
- E04_S3_ENCRYPTION.png — default SSE-S3 bucket encryption
- E06_MASTER_JPS.png — master daemons
- E07_WORKER_JPS.png — worker daemons
- E20_DISTANCE_BANDS.png — Spark distance-band output
- E26_GITHUB_BRANCH.png — Spark branch context
- E28_GITHUB_PROFILE_RESULTS.png — repository profiling artifacts
- E29_GITHUB_PREPROCESSING_RESULTS.png — repository preprocessing artifacts
- E30_GITHUB_ANALYSIS_RESULTS.png — repository analytical artifacts
- E31_GITHUB_BENCHMARK_RESULTS.png — repository benchmark/physical-plan artifacts

## Compact matched-benchmark artifacts

- `results/benchmarks/spark_matching_pandas_final_clean.csv`
- `results/benchmarks/spark_pandas_matched_comparison.csv`
- `results/benchmarks/matched_benchmark_summary.txt`
