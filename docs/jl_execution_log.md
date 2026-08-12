# Yeoh Jie Long — Execution Log

## Part 1 — Existing Project Audit
Objective: Audit the existing project and preserve its established scope.
Actions performed: Reviewed the existing repository and assignment workflow.
Outputs/files created: Project plan and scope baseline.
Validation/results: Existing work was retained; no project rebuild occurred.
Problems encountered: None recorded.
Corrections/fixes: Not applicable.
Evidence: Existing repository scaffold and later execution artifacts.
Git commit: Pre-existing history.
Status: Completed.

## Part 2 — GitHub Scaffold
Objective: Establish the Spark project scaffold on branch \`jie-long-spark\`.
Actions performed: Created the repository structure for configuration, documentation, source, results, evidence and tests.
Outputs/files created: Project scaffold and protective .gitignore.
Validation/results: Branch scaffold exists and main was not used for JL implementation.
Problems encountered: None recorded.
Corrections/fixes: Not applicable.
Evidence: Commit \`ce00ad7\` and repository tree.
Git commit: Part 2: add Spark project scaffold and preprocessing rules.
Status: Completed.

## Part 3 — Dataset Verification
Objective: Verify the intended NYC Yellow Taxi source coverage.
Actions performed: Confirmed the monthly source period and taxi-zone lookup reference.
Outputs/files created: Dataset verification record.
Validation/results: 24 monthly files span 2024-01 through 2025-12; lookup has 265 rows.
Problems encountered: None recorded.
Corrections/fixes: Not applicable.
Evidence: S3 listings and Part 6 profiling outputs.
Git commit: Deferred.
Status: Completed.

## Part 4 — AWS S3 Setup
Objective: Use the private Learner Lab bucket as the project source.
Actions performed: Configured and inspected \`nyc-taxi-017950536622-jielong-20260805\` with raw/yellow and reference prefixes.
Outputs/files created: S3 source structure.
Validation/results: Raw/yellow contains 24 objects; reference contains \`taxi_zone_lookup.csv\`; Block Public Access is On; default SSE-S3 encryption is enabled.
Problems encountered: None recorded.
Corrections/fixes: Not applicable.
Evidence: Current Part 15 AWS Console screenshots.
Git commit: Deferred.
Status: Completed.

## Part 5A — EMR Attempt
Objective: Attempt managed-cluster execution.
Actions performed: Submitted EMR cluster \`j-2O93TR9KEVKN6\`.
Outputs/files created: EMR methodology record.
Validation/results: Final state \`TERMINATED_WITH_ERRORS\`; reason \`INTERNAL_ERROR\`.
Problems encountered: EMR internal error.
Corrections/fixes: Used the existing EC2 fallback, not a replacement EMR cluster.
Evidence: EMR termination record, preserved separately from successful execution evidence.
Git commit: Deferred.
Status: Completed as a documented failed attempt.

## Part 5B — EC2 Fallback
Objective: Establish the existing two-node EC2 fallback.
Actions performed: Recovered the existing master and worker without rebuilding, formatting HDFS, or replacing nodes.
Outputs/files created: Existing Hadoop/YARN/Spark EC2 cluster.
Validation/results: Master-to-worker passwordless SSH and daemon topology were established.
Problems encountered: Earlier t3.micro memory pressure.
Corrections/fixes: Existing instances were upgraded in place to t3.small.
Evidence: EC2 console and daemon evidence.
Git commit: Deferred.
Status: Completed.

## Part 5C — Working Two-Node Hadoop/YARN/Spark Cluster
Objective: Recover the existing cluster and prove Spark on YARN.
Actions performed: Started only missing daemons and ran HDFS and Spark smoke tests.
Outputs/files created: Working cluster evidence.
Validation/results: Two t3.small nodes; one live DataNode; one YARN worker; Spark master \`yarn\`; Spark count \`1000\`.
Problems encountered: Spark action stalled on t3.micro nodes.
Corrections/fixes: In-place memory upgrade to t3.small.
Evidence: Master/worker jps, HDFS report, YARN node list, smoke-test output.
Git commit: Deferred.
Status: Completed.

## Part 6A — Data Staging
Objective: Stage 24 private S3 Parquet files and lookup data into HDFS.
Actions performed: Tested installed access methods, then staged data with Python standard-library AWS SigV4.
Outputs/files created: \`/data/nyc/raw/yellow\` and \`/data/nyc/reference\`.
Validation/results: Exactly 24 monthly Parquet files (2024-01 to 2025-12), no duplicate months, lookup staged, one live DataNode; temporary credentials removed.
Problems encountered: AWS CLI, S3A and boto3 were unavailable.
Corrections/fixes: Used Python 3 standard-library SigV4 only.
Evidence: Staging counts, HDFS listing and report.
Git commit: Deferred.
Status: Completed.

## Part 6B — Spark Raw Data Profiling
Objective: Profile all raw months before preprocessing using Spark on YARN.
Actions performed: Read each monthly Parquet sequentially; captured counts, schemas, timestamp ranges, and null diagnostics; profiled lookup.
Outputs/files created: Profile CSV/JSON/text outputs locally and in \`/data/nyc/results/profile\`.
Validation/results: 24 months; 89,892,322 raw rows; two schema variants (2024: 19 columns; 2025: 20 columns); lookup 265 rows, 265 distinct IDs, 0 duplicate IDs, 0 null IDs.
Problems encountered: Timestamp anomalies outside source ranges were observed.
Corrections/fixes: Retained anomalies for structural validation later; no filtering at profiling stage.
Evidence: \`PART_6B_PROFILE_SUCCESS\`, summaries and HDFS listing.
Git commit: Deferred.
Status: Completed.

## Part 6C — Part 6 Finalisation
Objective: Validate profiling artifacts and preprocessing compatibility.
Actions performed: Verified profile outputs and documented schema/timestamp observations.
Outputs/files created: Part 6 validation notes.
Validation/results: 24 source files remain valid; lookup supports planned PULocationID left join; no preprocessing, joins, rankings or analytics occurred in profiling.
Problems encountered: None beyond documented timestamp anomalies.
Corrections/fixes: None required.
Evidence: Profile summary, schema groups, lookup profile.
Git commit: Deferred.
Status: Completed.

## Part 7 — PySpark Preprocessing
Objective: Apply the shared structural preprocessing contract sequentially by source month.
Actions performed: Safe-cast fields, derived time/quality columns, created pre-filter diagnostics, retained valid fare/distance flags, and left-joined pickup lookup.
Outputs/files created: Monthly quality diagnostics.
Validation/results: 89,291,810 structurally valid rows; 600,512 structurally removed rows; 2,179,263 non-positive-distance rows retained and flagged; 3,618,518 non-positive-fare rows retained and flagged; 0 unmatched pickup lookups.
Problems encountered: One executor heartbeat timeout during December write.
Corrections/fixes: YARN replaced the executor and the task completed successfully.
Evidence: \`PART_7_8_PREPROCESS_SUCCESS\`, \`monthly_quality.csv\`, preprocessing summary.
Git commit: Deferred.
Status: Completed.

## Part 8 — Clean Partitioned Parquet Output
Objective: Publish the clean structurally-valid output as partitioned Parquet.
Actions performed: Wrote month-specific staging outputs and finalized clean HDFS partitions.
Outputs/files created: \`/data/nyc/processed/yellow_clean\`.
Validation/results: 24 pickup-year/month partitions for 2024 and 2025; 2.1 GB logical / 4.2 GB replicated; clean row validation equals 89,291,810.
Problems encountered: December executor heartbeat timeout, recovered by YARN.
Corrections/fixes: No contract change.
Evidence: HDFS partitions, size and clean-row validation.
Git commit: Deferred.
Status: Completed.

## Part 9 — PySpark Analytics
Objective: Produce meaningful aggregate results with the DataFrame API.
Actions performed: Calculated monthly, hourly/weekend, borough, top-zone, distance-band, passenger-group and payment-type tables.
Outputs/files created: Small analysis CSV outputs.
Validation/results: Trip-count totals preserve all 89,291,810 clean rows where applicable.
Problems encountered: None recorded.
Corrections/fixes: Not applicable.
Evidence: Analysis CSVs and success marker.
Git commit: Deferred.
Status: Completed.

## Part 10 — Spark SQL
Objective: Validate equivalent cleaned-data aggregates using Spark SQL.
Actions performed: Registered \`yellow_taxi\` temporary view and executed four equivalent SQL analyses.
Outputs/files created: SQL result CSVs and DataFrame-vs-SQL validation CSV.
Validation/results: All four comparisons PASS; monthly, distance-band and passenger-group totals equal 89,291,810.
Problems encountered: None recorded.
Corrections/fixes: Not applicable.
Evidence: \`PART_9_10_ANALYSIS_SUCCESS\`, SQL outputs and validation CSV.
Git commit: Deferred.
Status: Completed.

## Part 11 — Benchmarking
Objective: Collect lightweight Spark performance evidence.
Actions performed: Ran partition-scope counts and equivalent DataFrame/SQL monthly aggregations once; captured formatted physical plans.
Outputs/files created: Benchmark CSV, summary and four plan files.
Validation/results: Jan 2024 2,961,858 rows / 3.125253 s; 2024 41,132,445 rows / 1.108304 s; DataFrame monthly 5.322686 s; SQL monthly 3.345567 s; both monthly results have 24 rows and total 89,291,810.
Problems encountered: None recorded.
Corrections/fixes: None required.
Evidence: Benchmark results, summary, partition-pruning and shuffle/Exchange plans.
Git commit: Deferred.
Status: Completed. Timings are single-run observations on the two-node Learner Lab cluster, not universal claims.

## Part 12 — Jin Yon Handover
Objective: Produce a small safe handover package for comparison, visualisation and report integration.
Actions performed: Packaged only small result files with README and manifest.
Outputs/files created: \`/home/hadoop/nyc_handover_to_jinyon\`, HDFS handover copy and archive.
Validation/results: 16 small files; README and manifest present; archive size 5.9 KB; no raw Parquet or credentials included.
Problems encountered: None recorded.
Corrections/fixes: Not applicable.
Evidence: Handover listing, manifest, README, archive and HDFS listing.
Git commit: Deferred.
Status: Completed.

## Part 14 — Evidence Consolidation
Objective: Consolidate AWS, Hadoop, HDFS, YARN, Spark, preprocessing, analysis, benchmark and handover evidence.
Actions performed: Mapped completed outputs to project stages and preserved EMR failure separately.
Outputs/files created: Evidence inventory and index draft.
Validation/results: Evidence reflects completed output; no credentials or raw data were included.
Problems encountered: Durable console screenshot references were initially missing.
Corrections/fixes: Part 15 captured the missing S3 and EC2 console views read-only.
Evidence: Evidence inventory, console screenshots and completed result files.
Git commit: Deferred until Part 15.
Status: Completed.
