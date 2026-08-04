# Approved preprocessing rules

These rules reproduce the preprocessing contract in `Code1.py` and the Amazon EMR handover. They apply to exactly 24 Yellow Taxi files, from January 2024 through December 2025, plus `taxi_zone_lookup.csv`.

## Inputs and safe conversion

- Require the eleven trip columns listed in `config/preprocessing_rules.yaml`.
- Safely convert the two timestamps and all listed numeric columns; unparseable values become missing.
- Normalise `Airport_fee` to `airport_fee`. If either optional fee (`airport_fee` or `cbd_congestion_fee`) is absent, create it as a typed missing-value column.
- Clean the lookup to numeric, non-missing, unique `LocationID` values and join it only to `PULocationID` as a many-to-one left join. Keep unmatched pickup rows and count them.

## Structural retention and quality evidence

Before filtering, record the monthly non-exclusive quality counts defined in the YAML. Retain a trip only when timestamps exist, drop-off is later than pickup, the pickup year and month match the source filename, both location IDs exist, and `0 < trip_duration_minutes <= 240`.

Non-positive fare or distance is counted but does not remove a structurally valid trip. Instead set `valid_fare` when both `fare_amount` and `total_amount` are positive, and set `valid_distance` when `trip_distance` is positive.

## Categories and output boundary

Create the five ordered distance bands and the passenger groups exactly as configured. Passenger counts are rounded to nullable integers: missing is `Missing`, zero is `0`, values one through six use their rounded integer label, and all other values are `7+`.

The processed data is Parquet partitioned by `pickup_year` and `pickup_month`. Preprocessing ends after the enriched structurally valid data and monthly quality record are ready; it does not include analytical aggregations, charts, visualisations, comparisons, reports, or AWS actions.
