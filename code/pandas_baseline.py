#!/usr/bin/env python3
"""
pandas_baseline.py - pandas analysis for NYC Yellow Taxi, January 2024 to December 2025.

Purpose
-------
Process all 24 monthly raw Parquet files independently in pandas using the same
structural rules as the Spark workflow, while avoiding one giant in-memory
DataFrame. Each month is processed separately and only compact aggregates are
retained.

Main outputs
------------
results/pandas-full-2024-2025/
    common_metrics.csv
    monthly_quality.csv
    monthly_trip_demand.csv
    hourly_demand.csv
    top_pickup_zones.csv
    distance_fare_summary.csv
    passenger_count_summary.csv
    passenger_single_multiple_summary.csv
    weekday_weekend_summary.csv
    weekday_weekend_hourly.csv
    year_comparison_summary.csv
    monthly_year_comparison.csv

Important consistency rules
---------------------------
- Structurally valid records KEEP non-positive distance/fare values.
- valid_distance = trip_distance > 0
- valid_fare = fare_amount > 0
- Structural duration rule: 0 < duration <= 240 minutes.
- Pickup year/month must match the source filename.
- Distance bands:
  0-<2, 2-<5, 5-<10, 10-<20, 20+, Invalid/Non-positive
"""

from __future__ import annotations

import gc
import re
import time
from collections import defaultdict
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
PASSENGER_CATEGORIES = [
    "Missing", "0", "1", "2", "3", "4", "5", "6", "7+"
]


def resolve_input_folder(base_folder: Path) -> Path:
    candidates = [
        base_folder / "raw_data",
        base_folder,
    ]
    for folder in candidates:
        if (folder / "yellow_tripdata_2024-01.parquet").is_file():
            return folder

    checked = "\n".join(f"  - {p}" for p in candidates)
    raise FileNotFoundError(
        "Could not find yellow_tripdata_2024-01.parquet. Checked:\n"
        f"{checked}"
    )


def resolve_lookup(base_folder: Path) -> Path:
    candidates = [
        base_folder / "raw_data" / "taxi_zone_lookup.csv",
        base_folder / "taxi_zone_lookup.csv",
    ]
    for path in candidates:
        if path.is_file():
            return path

    checked = "\n".join(f"  - {p}" for p in candidates)
    raise FileNotFoundError(
        "Could not find taxi_zone_lookup.csv. Checked:\n"
        f"{checked}"
    )


def parse_args():
    """Spyder direct-run configuration: no command-line arguments required."""
    class Args:
        pass

    args = Args()

    BASE_FOLDER = Path(
        r"C:\Users\Jin Yon\Downloads\NYC_Taxi"
    )

    args.base_folder = str(BASE_FOLDER)
    args.input_folder = str(resolve_input_folder(BASE_FOLDER))
    args.lookup = str(resolve_lookup(BASE_FOLDER))
    args.output = str(
        BASE_FOLDER / "results" / "pandas-full-2024-2025"
    )

    # Section 6.1 common-month validation output required by the report plan.
    args.validation_output = str(
        BASE_FOLDER
        / "results"
        / "validation"
        / "pandas-jan-2024"
    )

    args.max_duration_minutes = MAX_DURATION_MINUTES

    return args


def all_month_files(folder: Path):
    files = []
    for year in [2024, 2025]:
        for month in range(1, 13):
            files.append(
                folder / f"yellow_tripdata_{year}-{month:02d}.parquet"
            )
    return files


def parse_source_period(path: Path):
    match = re.search(
        r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$",
        path.name,
    )
    if not match:
        raise ValueError(
            f"Unexpected monthly filename: {path.name}"
        )
    return int(match.group(1)), int(match.group(2))


def load_lookup(path: Path):
    lookup = pd.read_csv(path)

    required = ["LocationID", "Borough", "Zone", "service_zone"]
    missing = [c for c in required if c not in lookup.columns]
    if missing:
        raise ValueError(
            f"Taxi zone lookup is missing required columns: {missing}"
        )

    lookup = lookup[required].copy()
    lookup["LocationID"] = pd.to_numeric(
        lookup["LocationID"], errors="coerce"
    )
    lookup = lookup.dropna(subset=["LocationID"])
    lookup["LocationID"] = lookup["LocationID"].astype("int64")
    lookup = lookup.drop_duplicates(
        subset=["LocationID"], keep="first"
    )

    return lookup.rename(
        columns={
            "LocationID": "PULocationID",
            "Borough": "pickup_Borough",
            "Zone": "pickup_Zone",
            "service_zone": "pickup_service_zone",
        }
    )


def _case_insensitive_column(available_columns, wanted):
    mapping = {c.lower(): c for c in available_columns}
    return mapping.get(wanted.lower())


def load_raw_month(path: Path):
    available = pq.read_schema(path).names

    canonical_required = [
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

    selected = []
    rename_map = {}

    for canonical in canonical_required:
        actual = _case_insensitive_column(available, canonical)
        if actual is None:
            raise ValueError(
                f"{path.name} is missing required column: {canonical}"
            )
        selected.append(actual)
        if actual != canonical:
            rename_map[actual] = canonical

    optional = [
        "airport_fee",
        "cbd_congestion_fee",
    ]

    for canonical in optional:
        actual = _case_insensitive_column(available, canonical)
        if actual is not None:
            selected.append(actual)
            if actual != canonical:
                rename_map[actual] = canonical

    df = pd.read_parquet(
        path,
        columns=selected,
        engine="pyarrow",
    ).rename(columns=rename_map)

    for column in optional:
        if column not in df.columns:
            df[column] = np.nan

    return df


def prepare_month(
    df,
    source_year,
    source_month,
    max_duration_minutes,
):
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
    df["pickup_date"] = df["pickup_ts"].dt.normalize()
    df["pickup_hour"] = df["pickup_ts"].dt.hour
    df["is_weekend"] = df["pickup_ts"].dt.dayofweek.isin([5, 6])

    df["trip_duration_minutes"] = (
        df["dropoff_ts"] - df["pickup_ts"]
    ).dt.total_seconds() / 60.0

    source_month_match = (
        (df["pickup_year"] == source_year)
        & (df["pickup_month"] == source_month)
    )

    quality = {
        "month": f"{source_year}-{source_month:02d}",
        "raw_rows": int(raw_rows),
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

    df["passenger_category"] = np.select(
        [
            df["passenger_count"].isna(),
            df["passenger_count"].eq(0),
            df["passenger_count"].eq(1),
            df["passenger_count"].eq(2),
            df["passenger_count"].eq(3),
            df["passenger_count"].eq(4),
            df["passenger_count"].eq(5),
            df["passenger_count"].eq(6),
        ],
        ["Missing", "0", "1", "2", "3", "4", "5", "6"],
        default="7+",
    )

    quality["structurally_valid_rows"] = int(len(df))
    quality["structurally_removed_rows"] = int(raw_rows - len(df))

    return df, quality


def add_group_count(target, frame, keys, count_name="trip_count"):
    counts = (
        frame.groupby(keys, dropna=False)
        .size()
        .reset_index(name=count_name)
    )
    for _, row in counts.iterrows():
        if isinstance(keys, str):
            key = row[keys]
        else:
            key = tuple(row[k] for k in keys)
        target[key] += int(row[count_name])


def main():
    args = parse_args()

    input_folder = Path(args.input_folder)
    lookup_file = Path(args.lookup)
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    validation_output = Path(args.validation_output)
    validation_output.mkdir(parents=True, exist_ok=True)

    files = all_month_files(input_folder)
    missing = [str(p) for p in files if not p.is_file()]
    if missing:
        raise FileNotFoundError(
            "All 24 monthly files are required. First missing file:\n"
            f"{missing[0]}"
        )

    lookup = load_lookup(lookup_file)

    print("FULL 2024-2025 PANDAS ANALYSIS")
    print("=" * 72)
    print("Input folder:", input_folder)
    print("Lookup CSV:  ", lookup_file)
    print("Output:      ", output)
    print("Monthly files:", len(files))

    start_all = time.perf_counter()

    monthly_quality_rows = []
    monthly_rows = []

    # Compact cumulative counts.
    hour_counts = defaultdict(int)
    zone_counts = defaultdict(int)
    weekday_hour_counts = defaultdict(int)
    weekday_daily_counts = defaultdict(int)
    weekday_zone_counts = defaultdict(int)

    # Distance-band accumulators.
    distance_acc = {
        band: {
            "trip_count": 0,
            "duration_sum": 0.0,
            "duration_count": 0,
            "fare_sum": 0.0,
            "fare_count": 0,
            "total_sum_for_valid_fare": 0.0,
            "total_count_for_valid_fare": 0,
            "fare_per_mile_sum": 0.0,
            "fare_per_mile_count": 0,
            "fare_values": [],
        }
        for band in DISTANCE_BANDS
    }

    # Passenger accumulators.
    passenger_acc = {
        category: {
            "trip_count": 0,
            "fare_sum": 0.0,
            "fare_count": 0,
            "distance_sum": 0.0,
            "distance_count": 0,
            "duration_sum": 0.0,
            "duration_count": 0,
        }
        for category in PASSENGER_CATEGORIES
    }

    total_raw_rows = 0
    total_valid_rows = 0

    for index, file in enumerate(files, start=1):
        source_year, source_month = parse_source_period(file)

        month_start = time.perf_counter()
        print(
            f"\n[{index:02d}/24] Processing {source_year}-{source_month:02d}: "
            f"{file.name}"
        )

        raw = load_raw_month(file)
        df, quality = prepare_month(
            raw,
            source_year,
            source_month,
            args.max_duration_minutes,
        )
        del raw

        # Merge pickup lookup for location outputs.
        df["PULocationID"] = df["PULocationID"].astype("int64")
        df = df.merge(
            lookup,
            how="left",
            on="PULocationID",
            validate="many_to_one",
        )

        quality["unmatched_pickup_lookup"] = int(
            df["pickup_Zone"].isna().sum()
        )

        # -----------------------------------------------------
        # Section 6.1: export January 2024 common-month outputs
        # -----------------------------------------------------
        if source_year == 2024 and source_month == 1:
            jan_hourly = (
                df.groupby("pickup_hour")
                .size()
                .rename("trip_count")
                .reset_index()
                .sort_values("pickup_hour")
            )

            jan_top_zones = (
                df.dropna(subset=["pickup_Zone"])
                .groupby(["pickup_Borough", "pickup_Zone"])
                .size()
                .rename("trip_count")
                .reset_index()
                .sort_values(
                    ["trip_count", "pickup_Borough", "pickup_Zone"],
                    ascending=[False, True, True],
                )
            )

            jan_distance_rows = []
            for band in DISTANCE_BANDS:
                jan_group = df.loc[df["distance_band"].eq(band)]
                jan_valid_fare = jan_group.loc[jan_group["valid_fare"]]

                jan_distance_rows.append(
                    {
                        "distance_band": band,
                        "trip_count": int(len(jan_group)),
                        "avg_valid_fare_amount": jan_valid_fare[
                            "fare_amount"
                        ].mean(),
                    }
                )

            jan_distance = pd.DataFrame(jan_distance_rows)

            busiest = jan_hourly.sort_values(
                ["trip_count", "pickup_hour"],
                ascending=[False, True],
            ).iloc[0]

            top_zone = jan_top_zones.iloc[0]

            jan_common = pd.DataFrame(
                [
                    {
                        "source_period": "2024-01",
                        "raw_rows": int(quality["raw_rows"]),
                        "valid_rows": int(quality["structurally_valid_rows"]),
                        "busiest_pickup_hour": int(busiest["pickup_hour"]),
                        "busiest_pickup_hour_trips": int(busiest["trip_count"]),
                        "top_pickup_borough": str(top_zone["pickup_Borough"]),
                        "top_pickup_zone": str(top_zone["pickup_Zone"]),
                        "top_pickup_zone_trips": int(top_zone["trip_count"]),
                    }
                ]
            )

            jan_common.to_csv(
                validation_output / "common_metrics.csv",
                index=False,
            )
            jan_hourly.to_csv(
                validation_output / "hourly_demand.csv",
                index=False,
            )
            jan_top_zones.head(20).to_csv(
                validation_output / "top_pickup_zones.csv",
                index=False,
            )
            jan_distance.to_csv(
                validation_output / "distance_fare_summary.csv",
                index=False,
            )
            pd.DataFrame([quality]).to_csv(
                validation_output / "quality_summary.csv",
                index=False,
            )

            print(
                "    January 2024 Section 6.1 validation outputs saved to:",
                validation_output,
            )

        monthly_quality_rows.append(quality)
        total_raw_rows += quality["raw_rows"]
        total_valid_rows += quality["structurally_valid_rows"]

        # -----------------------------------------------------
        # Common/full-period counts
        # -----------------------------------------------------
        add_group_count(hour_counts, df, "pickup_hour")

        zone_part = (
            df.dropna(subset=["pickup_Zone"])
            .groupby(["pickup_Borough", "pickup_Zone"])
            .size()
            .reset_index(name="trip_count")
        )
        for _, row in zone_part.iterrows():
            zone_counts[
                (str(row["pickup_Borough"]), str(row["pickup_Zone"]))
            ] += int(row["trip_count"])

        # -----------------------------------------------------
        # 5.5 distance/fare
        # -----------------------------------------------------
        for band in DISTANCE_BANDS:
            group = df.loc[df["distance_band"].eq(band)]
            acc = distance_acc[band]

            acc["trip_count"] += int(len(group))

            duration = group["trip_duration_minutes"].dropna()
            acc["duration_sum"] += float(duration.sum())
            acc["duration_count"] += int(duration.count())

            valid_fare = group.loc[
                group["valid_fare"] & group["fare_amount"].notna()
            ]

            fares = valid_fare["fare_amount"].astype("float64")
            acc["fare_sum"] += float(fares.sum())
            acc["fare_count"] += int(fares.count())

            # Keep only fare values for exact full-period median.
            if len(fares):
                acc["fare_values"].append(
                    fares.to_numpy(copy=True)
                )

            totals = valid_fare["total_amount"].dropna()
            acc["total_sum_for_valid_fare"] += float(totals.sum())
            acc["total_count_for_valid_fare"] += int(totals.count())

            valid_both = group.loc[
                group["valid_fare"]
                & group["valid_distance"]
                & group["fare_amount"].notna()
                & group["trip_distance"].notna()
            ]
            fare_per_mile = (
                valid_both["fare_amount"]
                / valid_both["trip_distance"]
            )
            fare_per_mile = fare_per_mile.replace(
                [np.inf, -np.inf], np.nan
            ).dropna()

            acc["fare_per_mile_sum"] += float(fare_per_mile.sum())
            acc["fare_per_mile_count"] += int(fare_per_mile.count())

        # -----------------------------------------------------
        # 5.6 passenger patterns
        # -----------------------------------------------------
        for category in PASSENGER_CATEGORIES:
            group = df.loc[
                df["passenger_category"].eq(category)
            ]
            acc = passenger_acc[category]

            acc["trip_count"] += int(len(group))

            fare = group.loc[
                group["valid_fare"], "fare_amount"
            ].dropna()
            acc["fare_sum"] += float(fare.sum())
            acc["fare_count"] += int(fare.count())

            distance = group.loc[
                group["valid_distance"], "trip_distance"
            ].dropna()
            acc["distance_sum"] += float(distance.sum())
            acc["distance_count"] += int(distance.count())

            duration = group["trip_duration_minutes"].dropna()
            acc["duration_sum"] += float(duration.sum())
            acc["duration_count"] += int(duration.count())

        # -----------------------------------------------------
        # 5.7 weekday/weekend
        # -----------------------------------------------------
        hourly_part = (
            df.groupby(["is_weekend", "pickup_hour"])
            .size()
            .reset_index(name="trip_count")
        )
        for _, row in hourly_part.iterrows():
            weekday_hour_counts[
                (bool(row["is_weekend"]), int(row["pickup_hour"]))
            ] += int(row["trip_count"])

        daily_part = (
            df.groupby(["is_weekend", "pickup_date"])
            .size()
            .reset_index(name="trip_count")
        )
        for _, row in daily_part.iterrows():
            date_key = pd.Timestamp(row["pickup_date"]).date().isoformat()
            weekday_daily_counts[
                (bool(row["is_weekend"]), date_key)
            ] += int(row["trip_count"])

        wz = (
            df.dropna(subset=["pickup_Zone"])
            .groupby(
                ["is_weekend", "pickup_Borough", "pickup_Zone"]
            )
            .size()
            .reset_index(name="trip_count")
        )
        for _, row in wz.iterrows():
            weekday_zone_counts[
                (
                    bool(row["is_weekend"]),
                    str(row["pickup_Borough"]),
                    str(row["pickup_Zone"]),
                )
            ] += int(row["trip_count"])

        # -----------------------------------------------------
        # 5.8 monthly 2024 versus 2025
        # -----------------------------------------------------
        valid_fare = df.loc[df["valid_fare"]]
        valid_distance = df.loc[df["valid_distance"]]

        monthly_rows.append(
            {
                "pickup_year": source_year,
                "pickup_month": source_month,
                "trip_count": int(len(df)),
                "avg_valid_fare_amount": valid_fare[
                    "fare_amount"
                ].mean(),
                "avg_total_amount_for_valid_fare": valid_fare[
                    "total_amount"
                ].mean(),
                "avg_valid_trip_distance": valid_distance[
                    "trip_distance"
                ].mean(),
                "avg_trip_duration_min": df[
                    "trip_duration_minutes"
                ].mean(),
                "avg_congestion_surcharge": df[
                    "congestion_surcharge"
                ].mean(),
                "cbd_fee_available": bool(
                    df["cbd_congestion_fee"].notna().any()
                ),
                "cbd_fee_charged_trips": int(
                    (df["cbd_congestion_fee"] > 0).sum()
                ),
                "cbd_fee_charged_share_pct": (
                    (df["cbd_congestion_fee"] > 0).mean() * 100
                    if df["cbd_congestion_fee"].notna().any()
                    else np.nan
                ),
            }
        )

        elapsed = time.perf_counter() - month_start
        print(
            f"    raw={quality['raw_rows']:,} | "
            f"valid={quality['structurally_valid_rows']:,} | "
            f"{elapsed:.2f}s"
        )

        del df
        gc.collect()

    # =========================================================
    # Final output: quality + monthly
    # =========================================================
    monthly_quality = pd.DataFrame(monthly_quality_rows)
    monthly_quality.to_csv(
        output / "monthly_quality.csv",
        index=False,
    )

    monthly = pd.DataFrame(monthly_rows).sort_values(
        ["pickup_year", "pickup_month"]
    )
    monthly.to_csv(
        output / "monthly_year_comparison.csv",
        index=False,
    )
    monthly[
        ["pickup_year", "pickup_month", "trip_count"]
    ].to_csv(
        output / "monthly_trip_demand.csv",
        index=False,
    )

    # =========================================================
    # Common metrics
    # =========================================================
    busiest_hour, busiest_hour_trips = sorted(
        hour_counts.items(),
        key=lambda item: (-item[1], item[0]),
    )[0]

    top_zone_key, top_zone_trips = sorted(
        zone_counts.items(),
        key=lambda item: (
            -item[1],
            item[0][0],
            item[0][1],
        ),
    )[0]

    common_metrics = pd.DataFrame(
        [
            {
                "source_period": "2024-01_to_2025-12",
                "months_processed": 24,
                "raw_rows": int(total_raw_rows),
                "valid_rows": int(total_valid_rows),
                "busiest_pickup_hour": int(busiest_hour),
                "busiest_pickup_hour_trips": int(
                    busiest_hour_trips
                ),
                "top_pickup_borough": top_zone_key[0],
                "top_pickup_zone": top_zone_key[1],
                "top_pickup_zone_trips": int(top_zone_trips),
            }
        ]
    )
    common_metrics.to_csv(
        output / "common_metrics.csv",
        index=False,
    )

    hourly_demand = pd.DataFrame(
        [
            {
                "pickup_hour": int(hour),
                "trip_count": int(count),
            }
            for hour, count in hour_counts.items()
        ]
    ).sort_values("pickup_hour")
    hourly_demand.to_csv(
        output / "hourly_demand.csv",
        index=False,
    )

    top_pickup_zones = pd.DataFrame(
        [
            {
                "pickup_Borough": borough,
                "pickup_Zone": zone,
                "trip_count": int(count),
            }
            for (borough, zone), count in zone_counts.items()
        ]
    ).sort_values(
        ["trip_count", "pickup_Borough", "pickup_Zone"],
        ascending=[False, True, True],
    )
    top_pickup_zones.head(20).to_csv(
        output / "top_pickup_zones.csv",
        index=False,
    )

    # =========================================================
    # 5.5 final distance summary
    # =========================================================
    distance_rows = []

    for band in DISTANCE_BANDS:
        acc = distance_acc[band]

        fare_median = np.nan
        if acc["fare_values"]:
            all_fares = np.concatenate(acc["fare_values"])
            fare_median = float(np.median(all_fares))
            del all_fares

        distance_rows.append(
            {
                "distance_band": band,
                "trip_count": int(acc["trip_count"]),
                "avg_valid_fare_amount": (
                    acc["fare_sum"] / acc["fare_count"]
                    if acc["fare_count"] else np.nan
                ),
                "median_valid_fare_amount": fare_median,
                "avg_total_amount_for_valid_fare": (
                    acc["total_sum_for_valid_fare"]
                    / acc["total_count_for_valid_fare"]
                    if acc["total_count_for_valid_fare"]
                    else np.nan
                ),
                "avg_trip_duration_min": (
                    acc["duration_sum"] / acc["duration_count"]
                    if acc["duration_count"] else np.nan
                ),
                "avg_fare_per_mile": (
                    acc["fare_per_mile_sum"]
                    / acc["fare_per_mile_count"]
                    if acc["fare_per_mile_count"]
                    else np.nan
                ),
            }
        )

    distance_summary = pd.DataFrame(distance_rows)
    distance_summary.to_csv(
        output / "distance_fare_summary.csv",
        index=False,
    )

    # Release retained fare arrays after median calculation.
    for band in DISTANCE_BANDS:
        distance_acc[band]["fare_values"].clear()
    gc.collect()

    # =========================================================
    # 5.6 passenger summary
    # =========================================================
    passenger_rows = []
    for category in PASSENGER_CATEGORIES:
        acc = passenger_acc[category]
        passenger_rows.append(
            {
                "passenger_category": category,
                "trip_count": int(acc["trip_count"]),
                "share_pct": (
                    acc["trip_count"] / total_valid_rows * 100
                    if total_valid_rows else np.nan
                ),
                "avg_valid_fare_amount": (
                    acc["fare_sum"] / acc["fare_count"]
                    if acc["fare_count"] else np.nan
                ),
                "avg_valid_trip_distance": (
                    acc["distance_sum"] / acc["distance_count"]
                    if acc["distance_count"] else np.nan
                ),
                "avg_trip_duration_min": (
                    acc["duration_sum"] / acc["duration_count"]
                    if acc["duration_count"] else np.nan
                ),
            }
        )

    passenger_summary = pd.DataFrame(passenger_rows)
    passenger_summary.to_csv(
        output / "passenger_count_summary.csv",
        index=False,
    )

    single = passenger_acc["1"]
    multiple_trip_count = sum(
        passenger_acc[str(i)]["trip_count"]
        for i in range(2, 7)
    )
    valid_1_to_6 = (
        single["trip_count"] + multiple_trip_count
    )

    def combine_passenger(categories):
        total = {
            "trip_count": 0,
            "fare_sum": 0.0,
            "fare_count": 0,
            "distance_sum": 0.0,
            "distance_count": 0,
            "duration_sum": 0.0,
            "duration_count": 0,
        }
        for category in categories:
            acc = passenger_acc[category]
            for key in total:
                total[key] += acc[key]
        return total

    single_acc = combine_passenger(["1"])
    multiple_acc = combine_passenger(
        [str(i) for i in range(2, 7)]
    )

    sm_rows = []
    for label, acc in [
        ("Single passenger", single_acc),
        ("Multiple passengers (2-6)", multiple_acc),
    ]:
        sm_rows.append(
            {
                "passenger_group": label,
                "trip_count": int(acc["trip_count"]),
                "share_of_valid_1_to_6_pct": (
                    acc["trip_count"] / valid_1_to_6 * 100
                    if valid_1_to_6 else np.nan
                ),
                "avg_valid_fare_amount": (
                    acc["fare_sum"] / acc["fare_count"]
                    if acc["fare_count"] else np.nan
                ),
                "avg_valid_trip_distance": (
                    acc["distance_sum"] / acc["distance_count"]
                    if acc["distance_count"] else np.nan
                ),
                "avg_trip_duration_min": (
                    acc["duration_sum"] / acc["duration_count"]
                    if acc["duration_count"] else np.nan
                ),
            }
        )

    pd.DataFrame(sm_rows).to_csv(
        output / "passenger_single_multiple_summary.csv",
        index=False,
    )

    # =========================================================
    # 5.7 weekday/weekend summary
    # =========================================================
    weekday_hourly_rows = []
    weekday_summary_rows = []

    for is_weekend in [False, True]:
        day_type = "Weekend" if is_weekend else "Weekday"

        day_counts = [
            count
            for (flag, _date), count in weekday_daily_counts.items()
            if flag == is_weekend
        ]
        number_of_days = len(day_counts)

        hours = [
            (hour, count)
            for (flag, hour), count in weekday_hour_counts.items()
            if flag == is_weekend
        ]
        hours.sort(key=lambda x: x[0])

        for hour, count in hours:
            weekday_hourly_rows.append(
                {
                    "day_type": day_type,
                    "pickup_hour": int(hour),
                    "total_trips": int(count),
                    "number_of_days": int(number_of_days),
                    "avg_daily_trips": (
                        count / number_of_days
                        if number_of_days else np.nan
                    ),
                }
            )

        peak_hour, peak_hour_total = sorted(
            hours,
            key=lambda x: (-x[1], x[0]),
        )[0]

        top_location_candidates = [
            ((borough, zone), count)
            for (flag, borough, zone), count
            in weekday_zone_counts.items()
            if flag == is_weekend
        ]
        top_location, top_location_trips = sorted(
            top_location_candidates,
            key=lambda item: (
                -item[1],
                item[0][0],
                item[0][1],
            ),
        )[0]

        # Weighted 24-month averages from monthly raw data are computed
        # directly from all valid rows using a second compact pass over
        # monthly summary? No: retain exact fare/distance/duration below
        # by deriving from a lightweight second pass.
        weekday_summary_rows.append(
            {
                "day_type": day_type,
                "trip_count": int(sum(day_counts)),
                "number_of_days": int(number_of_days),
                "avg_daily_trips": (
                    float(np.mean(day_counts))
                    if day_counts else np.nan
                ),
                "peak_hour": int(peak_hour),
                "peak_hour_total_trips": int(peak_hour_total),
                "peak_hour_avg_daily_trips": (
                    peak_hour_total / number_of_days
                    if number_of_days else np.nan
                ),
                "top_pickup_borough": top_location[0],
                "top_pickup_zone": top_location[1],
                "top_pickup_zone_trips": int(
                    top_location_trips
                ),
            }
        )

    # Second monthly pass for exact weekday/weekend fare/distance/duration.
    # This avoids storing 89M rows while still producing the requested metrics.
    ww_numeric = {
        False: defaultdict(float),
        True: defaultdict(float),
    }

    print("\nCalculating weekday/weekend fare-distance-duration metrics...")
    for index, file in enumerate(files, start=1):
        source_year, source_month = parse_source_period(file)
        raw = load_raw_month(file)
        df, _quality = prepare_month(
            raw,
            source_year,
            source_month,
            args.max_duration_minutes,
        )
        del raw

        for flag in [False, True]:
            group = df.loc[df["is_weekend"].eq(flag)]
            valid_fare = group.loc[group["valid_fare"], "fare_amount"].dropna()
            valid_distance = group.loc[
                group["valid_distance"], "trip_distance"
            ].dropna()
            duration = group["trip_duration_minutes"].dropna()

            ww_numeric[flag]["fare_sum"] += float(valid_fare.sum())
            ww_numeric[flag]["fare_count"] += int(valid_fare.count())
            ww_numeric[flag]["distance_sum"] += float(valid_distance.sum())
            ww_numeric[flag]["distance_count"] += int(valid_distance.count())
            ww_numeric[flag]["duration_sum"] += float(duration.sum())
            ww_numeric[flag]["duration_count"] += int(duration.count())

        del df
        gc.collect()
        print(f"  metrics pass {index:02d}/24 complete")

    for row in weekday_summary_rows:
        flag = row["day_type"] == "Weekend"
        acc = ww_numeric[flag]
        row["avg_valid_fare_amount"] = (
            acc["fare_sum"] / acc["fare_count"]
            if acc["fare_count"] else np.nan
        )
        row["avg_valid_trip_distance"] = (
            acc["distance_sum"] / acc["distance_count"]
            if acc["distance_count"] else np.nan
        )
        row["avg_trip_duration_min"] = (
            acc["duration_sum"] / acc["duration_count"]
            if acc["duration_count"] else np.nan
        )

    pd.DataFrame(weekday_summary_rows).to_csv(
        output / "weekday_weekend_summary.csv",
        index=False,
    )
    pd.DataFrame(weekday_hourly_rows).to_csv(
        output / "weekday_weekend_hourly.csv",
        index=False,
    )

    # =========================================================
    # 5.8 year summary
    # =========================================================
    year_rows = []
    for year, part in monthly.groupby("pickup_year"):
        days_in_year = 366 if int(year) == 2024 else 365
        peak = part.sort_values(
            ["trip_count", "pickup_month"],
            ascending=[False, True],
        ).iloc[0]

        year_rows.append(
            {
                "pickup_year": int(year),
                "total_valid_trips": int(part["trip_count"].sum()),
                "avg_daily_trips": (
                    part["trip_count"].sum() / days_in_year
                ),
                "peak_month": int(peak["pickup_month"]),
                "peak_monthly_trips": int(peak["trip_count"]),
                "avg_monthly_valid_fare_amount": np.average(
                    part["avg_valid_fare_amount"],
                    weights=part["trip_count"],
                ),
                "avg_monthly_total_amount": np.average(
                    part["avg_total_amount_for_valid_fare"],
                    weights=part["trip_count"],
                ),
                "avg_congestion_surcharge": np.average(
                    part["avg_congestion_surcharge"].fillna(0),
                    weights=part["trip_count"],
                ),
                "cbd_fee_charged_trips": int(
                    part["cbd_fee_charged_trips"].sum()
                ),
            }
        )

    pd.DataFrame(year_rows).to_csv(
        output / "year_comparison_summary.csv",
        index=False,
    )

    elapsed_all = time.perf_counter() - start_all

    print("\nFULL PANDAS ANALYSIS COMPLETED")
    print("=" * 72)
    print(f"Raw rows:   {total_raw_rows:,}")
    print(f"Valid rows: {total_valid_rows:,}")
    print(
        f"Busiest hour: {int(busiest_hour):02d}:00 "
        f"({busiest_hour_trips:,} trips)"
    )
    print(
        f"Top pickup zone: {top_zone_key[1]} "
        f"({top_zone_trips:,} trips)"
    )
    print(f"Elapsed: {elapsed_all:.2f} seconds")
    print(f"Saved to: {output}")


if __name__ == "__main__":
    main()
