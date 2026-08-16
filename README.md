# IST3134 Big Data Analytics in the Cloud

## Cloud-Based Big Data Analysis of NYC TLC Yellow Taxi Trip Record Data

This shared repository contains the two complementary implementations developed for the IST3134 group project.

## Team

- Yeoh Jie Long (22071112) — AWS, Hadoop/HDFS/YARN, PySpark and Spark SQL
- Ooi Jin Yon (22039069) — Python pandas, visualisation and comparison

## Dataset

- NYC TLC Yellow Taxi Trip Records
- Period: January 2024 – December 2025
- 24 monthly Parquet files plus the Taxi Zone Lookup CSV
- Official dataset: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Repository Branch Structure

- **main** — pandas / visualisation implementation and the group landing page
- **jie-long-spark** — AWS / PySpark / Spark SQL implementation

Direct Spark branch: [jie-long-spark](https://github.com/ItzSeir/Small-Data-Analytic/tree/jie-long-spark)

The pandas and Spark implementations were developed separately within the same shared repository. They were compared using common preprocessing rules, structural-validity definitions and validation outputs so that correctness and scalability results refer to matched workloads.

## Key Project Results

- Raw records: 89,892,322
- Structurally valid records: 89,291,810
- Full period: January 2024 to December 2025
- Busiest pickup hour: 18:00
- Leading pickup zone: JFK Airport
- Spark and pandas January 2024 correctness validation: exact agreement
- Matched 1-, 3-, 6-, 12- and 24-month scalability validation: PASS at all scales

## Locations
- NYC Taxi Dataset: [data/](data/)
- Final Report: [docs/](docs/)
- pandas: [code/](code/)
- pandas result: [results/](results/)
- Spark: [jie-long-spark/src/spark/](https://github.com/ItzSeir/Small-Data-Analytic/tree/jie-long-spark/src/spark)
- Spark evidence: [jie-long-spark/evidence/](https://github.com/ItzSeir/Small-Data-Analytic/tree/jie-long-spark/evidence)
- Spark results: [jie-long-spark/results/](https://github.com/ItzSeir/Small-Data-Analytic/tree/jie-long-spark/results)

## Security and Repository Scope

The repository intentionally excludes raw Parquet datasets, large processed datasets, AWS credentials, AWS session tokens, PEM files and private keys. Only source code, compact results, documentation and approved submission artifacts should be committed.
