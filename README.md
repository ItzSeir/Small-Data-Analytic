# IST3134 - NYC Taxi Big Data (Small-Data Analytic)

Project members
- Yeoh Jie Long (JieLong615)
- Ooi Jin Yon (ItzSeir)

Platforms
- Local development (Python, Jupyter)
- AWS (S3 for storage; EC2/EMR or Glue for processing)

Dataset (official TLC page)
- NYC TLC Trip Record Data: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Project structure
- config/        - configuration files
- data/          - small sample data used in repo (raw_data is ignored)
- src/spark/     - Spark notebooks / scripts
- src/pandas/    - pandas notebooks / scripts
- src/visualisation/ - visualisation scripts
- sql/           - SQL queries
- results/       - result folders (profile, cleaning, analysis, benchmarks, validation)
- evidence/      - evidence files (AWS screenshots, notebooks)
- docs/          - additional documentation

Security & excluded files
- Raw Parquet files, credentials and keys are excluded by .gitignore.
