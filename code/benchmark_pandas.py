#!/usr/bin/env python3
"""
benchmark_pandas.py - pandas scalability benchmark across the 2024-2025 file sequence.

Scales
------
1 month   : Jan 2024
3 months  : Jan-Mar 2024
6 months  : Jan-Jun 2024
12 months : Jan-Dec 2024
24 months : Jan 2024-Dec 2025

The benchmark uses the same structural cleaning rules and a representative
top-pickup-zone aggregation. It processes each month sequentially so that the
24-month test is safer on a single machine.
"""

from __future__ import annotations

import gc
import os
import re
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd
import psutil


MAX_DURATION_MINUTES = 240
SCALES = (1, 3, 6, 12, 24)


def resolve_input_folder(base_folder: Path):
    candidates = [
        base_folder / "raw_data",
        base_folder,
    ]
    for folder in candidates:
        if (folder / "yellow_tripdata_2024-01.parquet").is_file():
            return folder

    raise FileNotFoundError(
        "Could not find yellow_tripdata_2024-01.parquet."
    )


def resolve_lookup(base_folder: Path):
    candidates = [
        base_folder / "raw_data" / "taxi_zone_lookup.csv",
        base_folder / "taxi_zone_lookup.csv",
    ]
    for path in candidates:
        if path.is_file():
            return path

    raise FileNotFoundError(
        "Could not find taxi_zone_lookup.csv."
    )


def parse_args():
    class Args:
        pass

    args = Args()

    BASE_FOLDER = Path(
        r"C:\Users\Jin Yon\Downloads\NYC_Taxi"
    )

    args.input_folder = str(
        resolve_input_folder(BASE_FOLDER)
    )
    args.lookup = str(
        resolve_lookup(BASE_FOLDER)
    )
    args.output = str(
        BASE_FOLDER
        / "results"
        / "benchmarks"
        / "pandas_runtime.csv"
    )

    return args


def chronological_month_files(folder: Path):
    files = []

    for year in [2024, 2025]:
        for month in range(1, 13):
            files.append(
                folder / f"yellow_tripdata_{year}-{month:02d}.parquet"
            )

    return files


def parse_period(path: Path):
    match = re.search(
        r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$",
        path.name,
    )

    if not match:
        raise ValueError(
            f"Unexpected filename: {path.name}"
        )

    return int(match.group(1)), int(match.group(2))


def load_lookup(path: Path):
    lookup = pd.read_csv(
        path,
        usecols=["LocationID", "Borough", "Zone"],
    )

    lookup["LocationID"] = pd.to_numeric(
        lookup["LocationID"],
        errors="coerce",
    )

    lookup = (
        lookup.dropna(subset=["LocationID"])
        .drop_duplicates("LocationID")
        .rename(
            columns={
                "LocationID": "PULocationID",
                "Borough": "pickup_borough",
                "Zone": "pickup_zone",
            }
        )
    )

    lookup["PULocationID"] = lookup[
        "PULocationID"
    ].astype("int64")

    return lookup


def read_and_clean(file: Path):
    source_year, source_month = parse_period(file)

    df = pd.read_parquet(
        file,
        columns=[
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
        ],
        engine="pyarrow",
    )

    pickup = pd.to_datetime(
        df["tpep_pickup_datetime"],
        errors="coerce",
    )
    dropoff = pd.to_datetime(
        df["tpep_dropoff_datetime"],
        errors="coerce",
    )

    duration = (
        dropoff - pickup
    ).dt.total_seconds() / 60.0

    valid = (
        pickup.notna()
        & dropoff.notna()
        & (dropoff > pickup)
        & (pickup.dt.year == source_year)
        & (pickup.dt.month == source_month)
        & df["PULocationID"].notna()
        & df["DOLocationID"].notna()
        & (duration > 0)
        & (duration <= MAX_DURATION_MINUTES)
    )

    result = df.loc[
        valid,
        ["PULocationID"],
    ].copy()

    result["PULocationID"] = result[
        "PULocationID"
    ].astype("int64")

    return result


def run_scale(files, lookup):
    process = psutil.Process(os.getpid())

    start = time.perf_counter()
    rss_before = (
        process.memory_info().rss / 1024 / 1024
    )
    rss_peak = rss_before

    valid_rows = 0
    location_counts = defaultdict(int)

    for file in files:
        frame = read_and_clean(file)
        valid_rows += int(len(frame))

        month_counts = (
            frame.groupby("PULocationID")
            .size()
        )

        for location_id, count in month_counts.items():
            location_counts[int(location_id)] += int(count)

        rss_peak = max(
            rss_peak,
            process.memory_info().rss / 1024 / 1024,
        )

        del frame, month_counts
        gc.collect()

    count_df = pd.DataFrame(
        {
            "PULocationID": list(location_counts.keys()),
            "trips": list(location_counts.values()),
        }
    )

    result = (
        count_df.merge(
            lookup,
            on="PULocationID",
            how="left",
            validate="many_to_one",
        )
        .dropna(subset=["pickup_zone"])
        .sort_values(
            ["trips", "pickup_borough", "pickup_zone"],
            ascending=[False, True, True],
        )
    )

    top_zone = (
        ""
        if result.empty
        else str(result.iloc[0]["pickup_zone"])
    )

    top_zone_trips = (
        ""
        if result.empty
        else int(result.iloc[0]["trips"])
    )

    elapsed = time.perf_counter() - start
    rss_after = (
        process.memory_info().rss / 1024 / 1024
    )

    return {
        "valid_rows": valid_rows,
        "runtime_seconds": elapsed,
        "rss_mb_before_workload": rss_before,
        "rss_mb_peak_observed": rss_peak,
        "rss_mb_after_workload": rss_after,
        "top_pickup_zone": top_zone,
        "top_pickup_zone_trips": top_zone_trips,
    }


def main():
    args = parse_args()

    input_folder = Path(args.input_folder)
    lookup_file = Path(args.lookup)
    output = Path(args.output)

    output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    all_files = chronological_month_files(
        input_folder
    )

    lookup = load_lookup(lookup_file)
    rows = []

    for scale in SCALES:
        files = all_files[:scale]

        missing = [
            str(path)
            for path in files
            if not path.is_file()
        ]

        print(
            f"\nRunning pandas benchmark: {scale} month(s)"
        )

        if missing:
            rows.append(
                {
                    "months": scale,
                    "period": (
                        "2024-01 to 2025-12"
                        if scale == 24
                        else f"first {scale} month(s)"
                    ),
                    "status": "SKIPPED",
                    "valid_rows": "",
                    "runtime_seconds": "",
                    "rss_mb_before_workload": "",
                    "rss_mb_peak_observed": "",
                    "rss_mb_after_workload": "",
                    "top_pickup_zone": "",
                    "top_pickup_zone_trips": "",
                    "result_or_error": (
                        f"Missing file: {missing[0]}"
                    ),
                }
            )
            continue

        try:
            result = run_scale(
                files,
                lookup,
            )

            rows.append(
                {
                    "months": scale,
                    "period": (
                        "2024-01 to 2025-12"
                        if scale == 24
                        else f"first {scale} month(s) from 2024-01"
                    ),
                    "status": "SUCCESS",
                    "valid_rows": result["valid_rows"],
                    "runtime_seconds": round(
                        result["runtime_seconds"], 3
                    ),
                    "rss_mb_before_workload": round(
                        result["rss_mb_before_workload"], 2
                    ),
                    "rss_mb_peak_observed": round(
                        result["rss_mb_peak_observed"], 2
                    ),
                    "rss_mb_after_workload": round(
                        result["rss_mb_after_workload"], 2
                    ),
                    "top_pickup_zone": result[
                        "top_pickup_zone"
                    ],
                    "top_pickup_zone_trips": result[
                        "top_pickup_zone_trips"
                    ],
                    "result_or_error": "",
                }
            )

        except MemoryError as exc:
            rows.append(
                {
                    "months": scale,
                    "period": "",
                    "status": "MEMORY_ERROR",
                    "valid_rows": "",
                    "runtime_seconds": "",
                    "rss_mb_before_workload": "",
                    "rss_mb_peak_observed": "",
                    "rss_mb_after_workload": "",
                    "top_pickup_zone": "",
                    "top_pickup_zone_trips": "",
                    "result_or_error": str(exc),
                }
            )

        except Exception as exc:
            rows.append(
                {
                    "months": scale,
                    "period": "",
                    "status": "FAILED",
                    "valid_rows": "",
                    "runtime_seconds": "",
                    "rss_mb_before_workload": "",
                    "rss_mb_peak_observed": "",
                    "rss_mb_after_workload": "",
                    "top_pickup_zone": "",
                    "top_pickup_zone_trips": "",
                    "result_or_error": (
                        f"{type(exc).__name__}: {exc}"
                    ),
                }
            )

        gc.collect()

    runtime = pd.DataFrame(rows)
    runtime.to_csv(
        output,
        index=False,
    )

    print("\nB4 BENCHMARK COMPLETED")
    print("=" * 72)
    print(runtime.to_string(index=False))
    print("\nSaved:", output)


if __name__ == "__main__":
    main()
