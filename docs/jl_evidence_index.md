# Yeoh Jie Long — AWS/Spark Evidence Index

Each entry records the project part, what the evidence proves, evidence type, and availability. No credentials or raw Parquet data are included.

## Part 4 — S3
| Evidence | Proves | Type | Availability |
|---|---|---|---|
| Current Part 15 AWS Console raw/yellow view | Bucket \`nyc-taxi-017950536622-jielong-20260805\`, raw/yellow prefix and 24 Parquet objects | Screenshot/output | AVAILABLE |
| Current Part 15 AWS Console reference view | \`taxi_zone_lookup.csv\` exists under reference | Screenshot/output | AVAILABLE |
| Current Part 15 S3 Permissions view | Block Public Access is On; bucket-owner enforcement blocks public access | Screenshot/output | AVAILABLE |
| Current Part 15 S3 Properties view | Default SSE-S3 encryption is enabled | Screenshot/output | AVAILABLE |

## Part 5 — Cluster / HDFS / YARN / Spark
| Evidence | Proves | Type | Availability |
|---|---|---|---|
| Current Part 15 EC2 Instances view | nyc-master \`i-0130507b6de6a79a3\` and nyc-slave1 \`i-048b900fcc5a38f15\` are Running, t3.small, 3/3 checks passed | Screenshot/output | AVAILABLE |
| Master and worker jps outputs | NameNode, SecondaryNameNode, ResourceManager, DataNode and NodeManager topology | Output | AVAILABLE |
| HDFS report and YARN node list | One live DataNode and one running YARN node | Output | AVAILABLE |
| HDFS/Spark smoke tests | HDFS text read, Spark master yarn, count 1000 | Output | AVAILABLE |
| EMR cluster j-2O93TR9KEVKN6 | Failed methodology attempt; TERMINATED_WITH_ERRORS / INTERNAL_ERROR | Output | AVAILABLE (failure evidence) |

## Part 6 — Profiling
| Evidence | Proves | Type | Availability |
|---|---|---|---|
| profile_summary.txt and profile CSV/JSON outputs | 24 months, 89,892,322 raw rows, 2 schema variants | File/output | AVAILABLE on EC2/HDFS; pending safe GitHub transfer |
| lookup_profile.csv and lookup_schema.json | 265 lookup rows/IDs, 0 duplicate, 0 null LocationID | File/output | AVAILABLE on EC2/HDFS; pending safe GitHub transfer |
| HDFS profile listing | Profile results stored under /data/nyc/results/profile | Output | AVAILABLE |

## Parts 7–8 — Preprocessing
| Evidence | Proves | Type | Availability |
|---|---|---|---|
| monthly_quality.csv and preprocessing_summary.txt | 24 months, 89,291,810 valid, 600,512 removed; retained non-positive flag counts; 0 unmatched pickups | File/output | AVAILABLE on EC2/HDFS; pending safe GitHub transfer |
| Clean HDFS listing and size | 24 year/month partitions and 2.1 GB logical / 4.2 GB replicated output | Output | AVAILABLE |
| Clean row validation | Output count equals 89,291,810 | Output | AVAILABLE |
| December executor recovery | Heartbeat timeout recovered by YARN replacement | Output | AVAILABLE |

## Parts 9–10 — Analytics / Spark SQL
| Evidence | Proves | Type | Availability |
|---|---|---|---|
| Analysis CSV tables | Monthly, hourly/weekend, borough, top zones, distance bands, passenger groups and payment types | File/output | AVAILABLE on EC2/HDFS; pending safe GitHub transfer |
| dataframe_sql_validation.csv | All four DataFrame-vs-SQL comparisons PASS; totals equal 89,291,810 | File/output | AVAILABLE on EC2/HDFS; pending safe GitHub transfer |
| HDFS analysis listing | Small analysis outputs persisted | Output | AVAILABLE |

## Part 11 — Benchmark
| Evidence | Proves | Type | Availability |
|---|---|---|---|
| benchmark_results.csv and summary | Single-run observed timings: Jan 2024 3.125253 s; 2024 1.108304 s; DataFrame 5.322686 s; SQL 3.345567 s | File/output | AVAILABLE on EC2/HDFS; pending safe GitHub transfer |
| Four formatted physical plan files | Partition-pruning and Exchange/shuffle evidence | File/output | AVAILABLE on EC2/HDFS; pending safe GitHub transfer |

Interpretation: timings are single-run observations on the two-node Learner Lab cluster; physical plans are stronger evidence than timing for partition pruning and shuffle behavior.

## Part 12 — Handover
| Evidence | Proves | Type | Availability |
|---|---|---|---|
| Handover folder, README and manifest | 16 small result files prepared for Jin Yon | File/output | AVAILABLE on EC2/HDFS |
| nyc_handover_to_jinyon.tar.gz | 5.9 KB archive; no raw Parquet or credentials | File/output | AVAILABLE on EC2 |

## Part 14 — Consolidation
| Evidence | Proves | Type | Availability |
|---|---|---|---|
| This index and Part 15 console captures | Completed evidence inventory, with Part 4/5 screenshot gaps closed | Documentation/screenshot | AVAILABLE |

## Missing / deferred GitHub transfers
The following safe, EC2-derived files require a later approved transfer method; no attempt should move raw data, credentials, PEM files, or runtime logs:
- Four Spark source scripts under \`src/spark/\`.
- Seven profile-result files under \`results/profile/\`.
- Two preprocessing-result files under \`results/preprocessing/\`.
- Eight analysis-result files under \`results/analysis/\`.
- Six benchmark and physical-plan files under \`results/benchmarks/\`.

