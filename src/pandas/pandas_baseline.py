"""
Independent pandas baseline for January 2024.

Purpose
-------
Read the RAW January 2024 NYC Yellow Taxi Parquet file independently in pandas,
apply the same structural cleaning rules as the Spark pipeline, enrich pickup
locations with the official taxi-zone lookup, and save the four common-month
outputs required for Spark-vs-pandas validation.

Required outputs
----------------
1. common_metrics.csv
2. hourly_demand.csv
3. top_pickup_zones.csv
4. distance_fare_summary.csv

Extra evidence outputs
----------------------
5. quality_summary.csv
6. passenger_group_summary.csv
7. weekday_weekend_summary.csv
"""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


MAX_DURATION_MINUTES = 240

DISTANCE_BANDS = [
    "0-<2",
    "2-<5",
    "5-<10",
    "10-<20",
    "20+",
    "Invalid/Non-positive",
]


def parse_args():
    """
    Spyder direct-run configuration.

    No command-line arguments are required.
    Edit BASE_FOLDER below only if your NYC_Taxi folder moves.
    """
    class Args:
        pass

    args = Args()

    BASE_FOLDER = Path(
        r"C:\Users\Jin Yon\Downloads\NYC_Taxi"
    )

    args.input = str(
        BASE_FOLDER / "yellow_tripdata_2024-01.parquet"
    )

    args.lookup = str(
        BASE_FOLDER / "taxi_zone_lookup.csv"
    )

    args.output = str(
        BASE_FOLDER
        / "results"
        / "validation"
        / "pandas-jan-2024"
    )

    args.max_duration_minutes = MAX_DURATION_MINUTES

    return args


def parse_source_period(path: Path):
    match = re.search(
        r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$",
        path.name,
    )
    if not match:
        raise ValueError(
            "Input filename must follow yellow_tripdata_YYYY-MM.parquet"
        )
    return int(match.group(1)), int(match.group(2))


def load_lookup(path: Path):
    lookup = pd.read_csv(path)

    required = ["LocationID", "Borough", "Zone", "service_zone"]
    missing = [column for column in required if column not in lookup.columns]
    if missing:
        raise ValueError(
            f"Taxi zone lookup is missing required columns: {missing}"
        )

    lookup = lookup[required].copy()
    lookup["LocationID"] = pd.to_numeric(
        lookup["LocationID"],
        errors="coerce",
    )
    lookup = lookup.dropna(subset=["LocationID"])
    lookup["LocationID"] = lookup["LocationID"].astype("int64")

    duplicate_location_rows = int(
        lookup.duplicated("LocationID", keep=False).sum()
    )

    lookup = lookup.drop_duplicates(
        subset=["LocationID"],
        keep="first",
    )

    pickup_lookup = lookup.rename(
        columns={
            "LocationID": "PULocationID",
            "Borough": "pickup_Borough",
            "Zone": "pickup_Zone",
            "service_zone": "pickup_service_zone",
        }
    )

    return pickup_lookup, duplicate_location_rows


def load_raw_month(path: Path):
    available_columns = pq.read_schema(path).names

    required = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
        "congestion_surcharge",
    ]

    missing = [column for column in required if column not in available_columns]
    if missing:
        raise ValueError(
            f"{path.name} is missing required columns: {missing}"
        )

    # Handle TLC schema naming changes safely.
    if "airport_fee" in available_columns:
        required.append("airport_fee")
    elif "Airport_fee" in available_columns:
        required.append("Airport_fee")

    if "cbd_congestion_fee" in available_columns:
        required.append("cbd_congestion_fee")
    elif "CBD_congestion_fee" in available_columns:
        required.append("CBD_congestion_fee")

    df = pd.read_parquet(
        path,
        columns=required,
        engine="pyarrow",
    )

    df = df.rename(
        columns={
            "Airport_fee": "airport_fee",
            "CBD_congestion_fee": "cbd_congestion_fee",
        }
    )

    if "airport_fee" not in df.columns:
        df["airport_fee"] = np.nan

    if "cbd_congestion_fee" not in df.columns:
        df["cbd_congestion_fee"] = np.nan

    return df


def prepare_month(df, source_year, source_month, max_duration_minutes):
    raw_rows = len(df)

    df["pickup_ts"] = pd.to_datetime(
        df["tpep_pickup_datetime"],
        errors="coerce",
    )
    df["dropoff_ts"] = pd.to_datetime(
        df["tpep_dropoff_datetime"],
        errors="coerce",
    )

    numeric_columns = [
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    df["pickup_year"] = df["pickup_ts"].dt.year
    df["pickup_month"] = df["pickup_ts"].dt.month
    df["pickup_date"] = df["pickup_ts"].dt.date
    df["pickup_hour"] = df["pickup_ts"].dt.hour
    df["is_weekend"] = df["pickup_ts"].dt.dayofweek.isin([5, 6])

    df["trip_duration_minutes"] = (
        df["dropoff_ts"] - df["pickup_ts"]
    ).dt.total_seconds() / 60.0

    source_month_match = (
        (df["pickup_year"] == source_year)
        & (df["pickup_month"] == source_month)
    )

    # Raw-quality counts are measured BEFORE structural cleaning.
    quality = {
        "source_period": f"{source_year}-{source_month:02d}",
        "raw_rows": raw_rows,
        "null_pickup_timestamp": int(df["pickup_ts"].isna().sum()),
        "null_dropoff_timestamp": int(df["dropoff_ts"].isna().sum()),
        "dropoff_not_after_pickup": int(
            (
                df["pickup_ts"].notna()
                & df["dropoff_ts"].notna()
                & (df["dropoff_ts"] <= df["pickup_ts"])
            ).sum()
        ),
        "duration_nonpositive": int(
            (
                df["trip_duration_minutes"].notna()
                & (df["trip_duration_minutes"] <= 0)
            ).sum()
        ),
        "duration_over_240": int(
            (df["trip_duration_minutes"] > max_duration_minutes).sum()
        ),
        "source_month_mismatch": int(
            (
                df["pickup_ts"].notna()
                & ~source_month_match
            ).sum()
        ),
        "null_PULocationID": int(df["PULocationID"].isna().sum()),
        "null_DOLocationID": int(df["DOLocationID"].isna().sum()),
        "nonpositive_distance": int(
            (
                df["trip_distance"].isna()
                | (df["trip_distance"] <= 0)
            ).sum()
        ),
        "nonpositive_fare": int(
            (
                df["fare_amount"].isna()
                | (df["fare_amount"] <= 0)
            ).sum()
        ),
        "missing_passenger_count": int(
            df["passenger_count"].isna().sum()
        ),
    }

    structural_valid = (
        df["pickup_ts"].notna()
        & df["dropoff_ts"].notna()
        & (df["dropoff_ts"] > df["pickup_ts"])
        & source_month_match
        & df["PULocationID"].notna()
        & df["DOLocationID"].notna()
        & (df["trip_duration_minutes"] > 0)
        & (df["trip_duration_minutes"] <= max_duration_minutes)
    )

    df = df.loc[structural_valid].copy()

    # Analysis-specific validity flags: DO NOT use these to remove rows globally.
    df["valid_distance"] = df["trip_distance"] > 0
    df["valid_fare"] = df["fare_amount"] > 0

    distance_conditions = [
        (df["trip_distance"] > 0) & (df["trip_distance"] < 2),
        (df["trip_distance"] >= 2) & (df["trip_distance"] < 5),
        (df["trip_distance"] >= 5) & (df["trip_distance"] < 10),
        (df["trip_distance"] >= 10) & (df["trip_distance"] < 20),
        df["trip_distance"] >= 20,
    ]

    df["distance_band"] = np.select(
        distance_conditions,
        ["0-<2", "2-<5", "5-<10", "10-<20", "20+"],
        default="Invalid/Non-positive",
    )

    df["passenger_group"] = np.select(
        [
            df["passenger_count"].isna(),
            df["passenger_count"].eq(0),
            df["passenger_count"].between(1, 6),
        ],
        ["Missing", "0", "1-6"],
        default="7+",
    )

    quality["structurally_valid_rows"] = len(df)
    quality["structurally_removed_rows"] = raw_rows - len(df)

    return df, quality


def main():
    args = parse_args()

    input_file = Path(args.input)
    lookup_file = Path(args.lookup)
    output = Path(args.output)

    if not input_file.is_file():
        raise FileNotFoundError(input_file)

    if not lookup_file.is_file():
        raise FileNotFoundError(lookup_file)

    output.mkdir(parents=True, exist_ok=True)

    source_year, source_month = parse_source_period(input_file)

    start = time.perf_counter()

    raw = load_raw_month(input_file)

    df, quality = prepare_month(
        raw,
        source_year,
        source_month,
        args.max_duration_minutes,
    )

    pickup_lookup, duplicate_lookup_rows = load_lookup(lookup_file)

    df["PULocationID"] = df["PULocationID"].astype("int64")

    df = df.merge(
        pickup_lookup,
        how="left",
        on="PULocationID",
        validate="many_to_one",
    )

    quality["lookup_duplicate_location_rows"] = duplicate_lookup_rows
    quality["unmatched_pickup_lookup"] = int(
        df["pickup_Zone"].isna().sum()
    )

    # ---------------------------------------------------------
    # Output 1: hourly_demand.csv
    # ---------------------------------------------------------
    daily_hourly = (
        df.groupby(
            [
                "pickup_year",
                "is_weekend",
                "pickup_date",
                "pickup_hour",
            ],
            dropna=False,
        )
        .size()
        .rename("trips")
        .reset_index()
    )

    hourly_demand = (
        daily_hourly.groupby(
            ["pickup_year", "is_weekend", "pickup_hour"],
            dropna=False,
        )["trips"]
        .agg(
            avg_daily_trips="mean",
            total_trips="sum",
        )
        .reset_index()
        .sort_values(["is_weekend", "pickup_hour"])
    )

    hourly_demand.to_csv(
        output / "hourly_demand.csv",
        index=False,
    )

    # ---------------------------------------------------------
    # Output 2: top_pickup_zones.csv
    # ---------------------------------------------------------
    top_pickup_zones = (
        df.dropna(subset=["pickup_Zone"])
        .groupby(
            ["pickup_Borough", "pickup_Zone"],
            dropna=False,
        )
        .agg(
            trips=("pickup_Zone", "size"),
            avg_valid_fare=(
                "fare_amount",
                lambda values: values[
                    df.loc[values.index, "valid_fare"]
                ].mean(),
            ),
            avg_valid_distance=(
                "trip_distance",
                lambda values: values[
                    df.loc[values.index, "valid_distance"]
                ].mean(),
            ),
        )
        .reset_index()
        .sort_values(
            ["trips", "pickup_Borough", "pickup_Zone"],
            ascending=[False, True, True],
        )
    )

    top_pickup_zones.head(20).to_csv(
        output / "top_pickup_zones.csv",
        index=False,
    )

    # ---------------------------------------------------------
    # Output 3: distance_fare_summary.csv
    #
    # trip_count contains ALL structurally valid rows in the band.
    # avg_valid_fare_amount excludes only fare_amount <= 0 / missing.
    # This matches the Spark analysis-specific flag approach.
    # ---------------------------------------------------------
    distance_rows = []

    for band in DISTANCE_BANDS:
        group = df.loc[df["distance_band"].eq(band)]
        valid_fare_group = group.loc[group["valid_fare"]]
        valid_both_group = group.loc[
            group["valid_fare"] & group["valid_distance"]
        ]

        distance_rows.append(
            {
                "distance_band": band,
                "trip_count": int(len(group)),
                "avg_trip_duration_min": (
                    group["trip_duration_minutes"].mean()
                ),
                "avg_valid_fare_amount": (
                    valid_fare_group["fare_amount"].mean()
                ),
                # Extra report-support metrics; B3 only needs avg_valid_fare_amount.
                "median_valid_fare_amount": (
                    valid_fare_group["fare_amount"].median()
                ),
                "avg_total_amount_for_valid_fare": (
                    valid_fare_group["total_amount"].mean()
                ),
                "avg_fare_per_mile": (
                    (
                        valid_both_group["fare_amount"]
                        / valid_both_group["trip_distance"]
                    ).mean()
                ),
            }
        )

    distance_fare_summary = pd.DataFrame(distance_rows)

    distance_fare_summary.to_csv(
        output / "distance_fare_summary.csv",
        index=False,
    )

    # ---------------------------------------------------------
    # Output 4: common_metrics.csv
    # ---------------------------------------------------------
    busiest = (
        df.groupby("pickup_hour")
        .size()
        .reset_index(name="trips")
        .sort_values(
            ["trips", "pickup_hour"],
            ascending=[False, True],
        )
        .iloc[0]
    )

    top_zone = top_pickup_zones.iloc[0]

    common_metrics = pd.DataFrame(
        [
            {
                "source_period": f"{source_year}-{source_month:02d}",
                "raw_rows": int(quality["raw_rows"]),
                "valid_rows": int(len(df)),
                "busiest_pickup_hour": int(busiest["pickup_hour"]),
                "busiest_pickup_hour_trips": int(busiest["trips"]),
                "top_pickup_borough": top_zone["pickup_Borough"],
                "top_pickup_zone": top_zone["pickup_Zone"],
                "top_pickup_zone_trips": int(top_zone["trips"]),
            }
        ]
    )

    common_metrics.to_csv(
        output / "common_metrics.csv",
        index=False,
    )

    # ---------------------------------------------------------
    # Extra evidence outputs
    # ---------------------------------------------------------
    passenger_summary = (
        df.groupby(
            "passenger_group",
            dropna=False,
        )
        .agg(
            trip_count=("passenger_group", "size"),
            avg_valid_trip_distance=(
                "trip_distance",
                lambda values: values[
                    df.loc[values.index, "valid_distance"]
                ].mean(),
            ),
            avg_valid_fare_amount=(
                "fare_amount",
                lambda values: values[
                    df.loc[values.index, "valid_fare"]
                ].mean(),
            ),
        )
        .reset_index()
    )

    passenger_summary.to_csv(
        output / "passenger_group_summary.csv",
        index=False,
    )

    weekday_weekend_rows = []

    for is_weekend in [False, True]:
        group = df.loc[df["is_weekend"].eq(is_weekend)]
        valid_fare = group.loc[group["valid_fare"]]
        valid_distance = group.loc[group["valid_distance"]]

        daily = (
            group.groupby("pickup_date")
            .size()
            .reset_index(name="trips")
        )

        hourly = (
            group.groupby("pickup_hour")
            .size()
            .reset_index(name="trips")
            .sort_values(
                ["trips", "pickup_hour"],
                ascending=[False, True],
            )
        )

        weekday_weekend_rows.append(
            {
                "day_type": "Weekend" if is_weekend else "Weekday",
                "trip_count": len(group),
                "number_of_days": group["pickup_date"].nunique(),
                "avg_daily_trips": daily["trips"].mean(),
                "peak_hour": int(hourly.iloc[0]["pickup_hour"]),
                "avg_valid_fare_amount": valid_fare["fare_amount"].mean(),
                "avg_valid_trip_distance": valid_distance["trip_distance"].mean(),
                "avg_trip_duration_min": group["trip_duration_minutes"].mean(),
            }
        )

    pd.DataFrame(weekday_weekend_rows).to_csv(
        output / "weekday_weekend_summary.csv",
        index=False,
    )

    pd.DataFrame([quality]).to_csv(
        output / "quality_summary.csv",
        index=False,
    )

    elapsed = time.perf_counter() - start

    print("\nB2 COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(common_metrics.to_string(index=False))
    print(f"\nRuntime: {elapsed:.3f} seconds")
    print(f"Results: {output}")

    # Known Spark January valid-row target from the current handover.
    expected_valid_rows = 2_961_858

    if source_year == 2024 and source_month == 1:
        if len(df) == expected_valid_rows:
            print(
                "\nCHECK: PASS - January 2024 valid rows match "
                f"Spark ({expected_valid_rows:,})."
            )
        else:
            print(
                "\nCHECK: REVIEW REQUIRED - pandas valid rows are "
                f"{len(df):,}, while the current Spark handover reports "
                f"{expected_valid_rows:,}."
            )


if __name__ == "__main__":
    main()
