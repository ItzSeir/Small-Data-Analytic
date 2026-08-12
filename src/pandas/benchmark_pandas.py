"""
B4 - pandas scalability benchmark.

Benchmark scales
----------------
1 month  : required
3 months : required
6 months : attempt where feasible
12 months: attempt where feasible

The workload is a representative top-pickup-zone aggregation after applying
the same structural rules used in B2/Spark.

"""

from __future__ import annotations

import argparse
import csv
import gc
import os
import re
import time
from pathlib import Path

import pandas as pd
import psutil


MAX_DURATION_MINUTES = 240
SCALES = (1, 3, 6, 12)


def parse_args():
    """
    Spyder direct-run configuration.
    No command-line arguments are required.
    """
    class Args:
        pass

    args = Args()

    BASE_FOLDER = Path(
        r"C:\Users\Jin Yon\Downloads\NYC_Taxi"
    )

    args.input_folder = str(
        BASE_FOLDER
    )

    args.lookup = str(
        BASE_FOLDER / "taxi_zone_lookup.csv"
    )

    args.output = str(
        BASE_FOLDER
        / "results"
        / "benchmarks"
        / "pandas_runtime.csv"
    )

    return args


def month_files(folder: Path, count: int):
    # Per the assignment plan, benchmark the first N months of 2024.
    return [
        folder / f"yellow_tripdata_2024-{month:02d}.parquet"
        for month in range(1, count + 1)
    ]


def parse_period(path: Path):
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

    columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
    ]

    df = pd.read_parquet(
        file,
        columns=columns,
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

    df = df.loc[
        valid,
        ["PULocationID"],
    ].copy()

    df["PULocationID"] = df[
        "PULocationID"
    ].astype("int64")

    return df


def run_scale(files, lookup):
    start = time.perf_counter()

    process = psutil.Process(os.getpid())

    memory_before_mb = (
        process.memory_info().rss
        / 1024
        / 1024
    )

    frames = []

    for file in files:
        frames.append(
            read_and_clean(file)
        )

    combined = pd.concat(
        frames,
        ignore_index=True,
    )

    valid_rows = int(len(combined))

    merged = combined.merge(
        lookup,
        on="PULocationID",
        how="left",
        validate="many_to_one",
    )

    result = (
        merged.dropna(subset=["pickup_zone"])
        .groupby(
            ["pickup_borough", "pickup_zone"]
        )
        .size()
        .rename("trips")
        .reset_index()
        .sort_values(
            ["trips", "pickup_borough", "pickup_zone"],
            ascending=[False, True, True],
        )
        .head(10)
    )

    elapsed = time.perf_counter() - start

    memory_after_mb = (
        process.memory_info().rss
        / 1024
        / 1024
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

    del combined, merged, result, frames
    gc.collect()

    return {
        "valid_rows": valid_rows,
        "runtime_seconds": elapsed,
        "rss_mb_before_workload": memory_before_mb,
        "rss_mb_after_workload": memory_after_mb,
        "top_zone": top_zone,
        "top_zone_trips": top_zone_trips,
    }


def main():
    args = parse_args()

    input_folder = Path(args.input_folder)
    lookup_file = Path(args.lookup)
    output = Path(args.output)

    if not input_folder.is_dir():
        raise FileNotFoundError(input_folder)

    if not lookup_file.is_file():
        raise FileNotFoundError(lookup_file)

    output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    lookup = load_lookup(lookup_file)

    rows = []

    for scale in SCALES:
        files = month_files(
            input_folder,
            scale,
        )

        missing = [
            str(path)
            for path in files
            if not path.is_file()
        ]

        if missing:
            rows.append(
                {
                    "months": scale,
                    "status": "SKIPPED",
                    "valid_rows": "",
                    "runtime_seconds": "",
                    "rss_mb_before_workload": "",
                    "rss_mb_after_workload": "",
                    "top_pickup_zone": "",
                    "top_pickup_zone_trips": "",
                    "result_or_error": (
                        f"Missing file: {missing[0]}"
                    ),
                }
            )
            continue

        print(
            f"\nRunning pandas benchmark: {scale} month(s)"
        )

        try:
            result = run_scale(
                files,
                lookup,
            )

            rows.append(
                {
                    "months": scale,
                    "status": "SUCCESS",
                    "valid_rows": result["valid_rows"],
                    "runtime_seconds": round(
                        result["runtime_seconds"],
                        3,
                    ),
                    "rss_mb_before_workload": round(
                        result["rss_mb_before_workload"],
                        2,
                    ),
                    "rss_mb_after_workload": round(
                        result["rss_mb_after_workload"],
                        2,
                    ),
                    "top_pickup_zone": result["top_zone"],
                    "top_pickup_zone_trips": (
                        result["top_zone_trips"]
                    ),
                    "result_or_error": "",
                }
            )

        except MemoryError as exc:
            rows.append(
                {
                    "months": scale,
                    "status": "MEMORY_ERROR",
                    "valid_rows": "",
                    "runtime_seconds": "",
                    "rss_mb_before_workload": "",
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
                    "status": "FAILED",
                    "valid_rows": "",
                    "runtime_seconds": "",
                    "rss_mb_before_workload": "",
                    "rss_mb_after_workload": "",
                    "top_pickup_zone": "",
                    "top_pickup_zone_trips": "",
                    "result_or_error": (
                        f"{type(exc).__name__}: {exc}"
                    ),
                }
            )

        gc.collect()

    fieldnames = [
        "months",
        "status",
        "valid_rows",
        "runtime_seconds",
        "rss_mb_before_workload",
        "rss_mb_after_workload",
        "top_pickup_zone",
        "top_pickup_zone_trips",
        "result_or_error",
    ]

    with output.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)

    print("\nB4 BENCHMARK COMPLETED")
    print("=" * 60)

    for row in rows:
        print(
            f"{row['months']:>2} month(s): "
            f"{row['status']}"
            + (
                f" | {row['runtime_seconds']} s"
                if row["runtime_seconds"] != ""
                else ""
            )
        )

    print(f"\nSaved: {output}")


if __name__ == "__main__":
    main()
