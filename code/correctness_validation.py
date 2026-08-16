#!/usr/bin/env python3
"""
Section 6.1 - Spark-versus-pandas correctness validation for January 2024.

This script is ONLY for the common-month correctness comparison in Section 6.1.
It does not compare the full 2024-2025 dataset and it does not perform runtime
or scalability benchmarking.

Planned correctness checks
--------------------------
1. Structurally valid row count -> exact match
2. Busiest pickup hour + trip count -> exact match
3. Top pickup zone + trip count -> exact match, except documented ties
4. Average valid fare by distance band -> absolute difference <= 0.01

Planned output
--------------
results/validation/correctness_validation.csv

Expected pandas inputs
----------------------
results/validation/pandas-jan-2024/
    common_metrics.csv
    distance_fare_summary.csv

Expected Spark January inputs
-----------------------------
The script searches the Spark handover/bundle for January-specific files such as:
    jan2024_common_metrics.csv
    jan2024_hourly_demand.csv
    jan2024_top_pickup_zones.csv
    jan2024_distance_band_summary.csv

For the valid-row count only, monthly_quality.csv can also be used because the
January 2024 row can be selected safely.

If an equivalent January-only Spark output is not present, the corresponding
comparison is marked PENDING rather than comparing January pandas results with
full-period Spark aggregates.
"""

from __future__ import annotations

import tarfile
import tempfile
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd


TOLERANCE = 0.01


def parse_args():
    """
    Spyder direct-run configuration.
    Edit BASE_FOLDER only if the NYC_Taxi folder moves.
    """
    class Args:
        pass

    args = Args()

    BASE_FOLDER = Path(
        r"C:\Users\Jin Yon\Downloads\NYC_Taxi"
    )

    args.base_folder = str(BASE_FOLDER)

    # This matches the common-month folder stated in the project plan.
    args.pandas_results = str(
        BASE_FOLDER
        / "results"
        / "validation"
        / "pandas-jan-2024"
    )

    # Leave as None to auto-detect an extracted Spark handover or archive.
    args.spark_results = None

    # Exact output filename stated in the project plan.
    args.output = str(
        BASE_FOLDER
        / "results"
        / "validation"
        / "correctness_validation.csv"
    )

    args.tolerance = TOLERANCE

    return args


def locate_spark_results(base_folder: Path):
    """
    Locate a Spark handover/bundle.

    Prefer a source that contains January-specific validation files.
    If none is found, fall back to a source containing monthly_quality.csv,
    which is sufficient for the January valid-row comparison.
    """
    search_roots = [
        base_folder,
        base_folder.parent,
    ]

    folder_patterns = [
        "nyc_handover_to_jinyon*",
        "jl_part15_github_bundle*",
        "JIN_YON_FINAL_SPARK_HANDOVER*",
    ]

    # Prefer extracted folders with Jan-specific files.
    for root in search_roots:
        for pattern in folder_patterns:
            for candidate in sorted(root.glob(pattern)):
                if not candidate.is_dir():
                    continue

                jan_files = (
                    list(candidate.rglob("jan2024_common_metrics.csv"))
                    + list(candidate.rglob("jan2024_hourly_demand.csv"))
                    + list(candidate.rglob("jan2024_top_pickup_zones.csv"))
                    + list(candidate.rglob("jan2024_distance_band_summary.csv"))
                )

                if jan_files:
                    return candidate

    # Then extracted folders with monthly_quality.csv.
    for root in search_roots:
        for pattern in folder_patterns:
            for candidate in sorted(root.glob(pattern)):
                if candidate.is_dir() and list(
                    candidate.rglob("monthly_quality.csv")
                ):
                    return candidate

    archive_patterns = [
        "nyc_handover_to_jinyon*.tar.gz",
        "nyc_handover_to_jinyon*.tgz",
        "nyc_handover_to_jinyon*.gz",
        "jl_part15_github_bundle*.tar.gz",
        "jl_part15_github_bundle*.tgz",
        "jl_part15_github_bundle*.gz",
        "jl_part15_github_bundle*.zip",
        "JIN_YON_FINAL_SPARK_HANDOVER*.tar.gz",
        "JIN_YON_FINAL_SPARK_HANDOVER*.tgz",
        "JIN_YON_FINAL_SPARK_HANDOVER*.gz",
        "JIN_YON_FINAL_SPARK_HANDOVER*.zip",
    ]

    matches = []
    for root in search_roots:
        for pattern in archive_patterns:
            matches.extend(
                p for p in root.glob(pattern)
                if p.is_file()
            )

    if matches:
        return sorted(matches)[0]

    checked = "\n".join(
        f"  - {root}"
        for root in search_roots
    )

    raise FileNotFoundError(
        "Could not locate a Spark handover/bundle automatically. "
        f"Checked:\n{checked}"
    )


def resolve_spark_results(path: Path):
    if path.is_dir():
        return path, None

    if not path.is_file():
        raise FileNotFoundError(path)

    temp = tempfile.TemporaryDirectory()
    lower_name = path.name.lower()

    if lower_name.endswith(".zip"):
        with zipfile.ZipFile(path, "r") as archive:
            archive.extractall(temp.name)

    elif lower_name.endswith((".tar.gz", ".tgz", ".gz")):
        with tarfile.open(path, "r:gz") as archive:
            archive.extractall(temp.name)

    else:
        temp.cleanup()
        raise ValueError(
            "Spark results must be an extracted folder, "
            ".zip, .tar.gz, .tgz or .gz."
        )

    return Path(temp.name), temp


def find_first(root: Path, names):
    for name in names:
        matches = list(root.rglob(name))
        if matches:
            return matches[0]
    return None


def add_result(
    rows,
    metric,
    spark_result,
    pandas_result,
    difference,
    rule,
    status,
    explanation="",
):
    rows.append(
        {
            "metric": metric,
            "spark_result": spark_result,
            "pandas_result": pandas_result,
            "difference": difference,
            "rule": rule,
            "status": status,
            "explanation": explanation,
        }
    )


def January_month_mask(df):
    """
    Return a boolean mask for January 2024 across the common
    monthly_quality.csv layouts.
    """
    if "month" in df.columns:
        month_text = df["month"].astype(str).str.strip()
        return month_text.eq("2024-01")

    if (
        "pickup_year" in df.columns
        and "pickup_month" in df.columns
    ):
        return (
            pd.to_numeric(
                df["pickup_year"],
                errors="coerce",
            ).eq(2024)
            & pd.to_numeric(
                df["pickup_month"],
                errors="coerce",
            ).eq(1)
        )

    if (
        "year" in df.columns
        and "month" in df.columns
    ):
        return (
            pd.to_numeric(
                df["year"],
                errors="coerce",
            ).eq(2024)
            & pd.to_numeric(
                df["month"],
                errors="coerce",
            ).eq(1)
        )

    return pd.Series(
        False,
        index=df.index,
        dtype=bool,
    )


def main():
    args = parse_args()

    pandas_folder = Path(args.pandas_results)
    output_file = Path(args.output)

    if not pandas_folder.is_dir():
        raise FileNotFoundError(
            "January pandas validation folder was not found.\n"
            "Run the January 2024 pandas baseline first.\n"
            f"Expected:\n{pandas_folder}"
        )

    pandas_common_file = (
        pandas_folder / "common_metrics.csv"
    )
    pandas_distance_file = (
        pandas_folder / "distance_fare_summary.csv"
    )

    if not pandas_common_file.is_file():
        raise FileNotFoundError(
            pandas_common_file
        )

    if not pandas_distance_file.is_file():
        raise FileNotFoundError(
            pandas_distance_file
        )

    spark_source = (
        Path(args.spark_results)
        if args.spark_results
        else locate_spark_results(
            Path(args.base_folder)
        )
    )

    print(
        "SECTION 6.1 CORRECTNESS VALIDATION - JANUARY 2024"
    )
    print("=" * 80)
    print("Pandas folder:", pandas_folder)
    print("Spark source: ", spark_source)

    spark_root, temporary = resolve_spark_results(
        spark_source
    )

    pandas_common = pd.read_csv(
        pandas_common_file
    ).iloc[0]

    pandas_distance = pd.read_csv(
        pandas_distance_file
    )

    validation_rows = []

    # =========================================================
    # 1. Structurally valid row count
    # =========================================================
    spark_valid_rows = None

    spark_common_file = find_first(
        spark_root,
        [
            "jan2024_common_metrics.csv",
            "common_metrics_jan2024.csv",
            "spark_jan2024_common_metrics.csv",
        ],
    )

    spark_common = None

    if spark_common_file is not None:
        spark_common = pd.read_csv(
            spark_common_file
        ).iloc[0]

        for candidate in [
            "valid_rows",
            "structurally_valid_rows",
            "trip_count",
        ]:
            if candidate in spark_common.index:
                spark_valid_rows = int(
                    spark_common[candidate]
                )
                break

    # monthly_quality.csv is safe for this metric because
    # January 2024 can be selected directly.
    if spark_valid_rows is None:
        monthly_quality_file = find_first(
            spark_root,
            ["monthly_quality.csv"],
        )

        if monthly_quality_file is not None:
            monthly_quality = pd.read_csv(
                monthly_quality_file
            )

            jan_mask = January_month_mask(
                monthly_quality
            )

            jan = monthly_quality.loc[
                jan_mask
            ]

            if not jan.empty:
                for candidate in [
                    "structurally_valid_rows",
                    "valid_rows",
                ]:
                    if candidate in jan.columns:
                        spark_valid_rows = int(
                            jan.iloc[0][candidate]
                        )
                        break

    pandas_valid_rows = int(
        pandas_common["valid_rows"]
    )

    if spark_valid_rows is None:
        add_result(
            validation_rows,
            "Structurally valid rows",
            "N/A",
            pandas_valid_rows,
            "",
            "Exact",
            "PENDING",
            "Equivalent January 2024 Spark valid-row output was not found.",
        )
    else:
        passed = (
            spark_valid_rows
            == pandas_valid_rows
        )

        add_result(
            validation_rows,
            "Structurally valid rows",
            spark_valid_rows,
            pandas_valid_rows,
            pandas_valid_rows - spark_valid_rows,
            "Exact",
            "PASS" if passed else "FAIL",
            "" if passed else (
                "Check structural cleaning and source-month filtering."
            ),
        )

    # =========================================================
    # 2. Busiest pickup hour
    # =========================================================
    spark_hour = None
    spark_hour_trips = None

    if spark_common is not None:
        if (
            "busiest_pickup_hour"
            in spark_common.index
        ):
            spark_hour = int(
                spark_common[
                    "busiest_pickup_hour"
                ]
            )

        for candidate in [
            "busiest_pickup_hour_trips",
            "busiest_hour_trips",
        ]:
            if candidate in spark_common.index:
                spark_hour_trips = int(
                    spark_common[candidate]
                )
                break

    if (
        spark_hour is None
        or spark_hour_trips is None
    ):
        spark_hour_file = find_first(
            spark_root,
            [
                "jan2024_hourly_demand.csv",
                "hourly_demand_jan2024.csv",
                "spark_jan2024_hourly_demand.csv",
            ],
        )

        if spark_hour_file is not None:
            spark_hour_df = pd.read_csv(
                spark_hour_file
            )

            hour_col = next(
                (
                    c
                    for c in [
                        "pickup_hour",
                        "hour",
                    ]
                    if c
                    in spark_hour_df.columns
                ),
                None,
            )

            count_col = next(
                (
                    c
                    for c in [
                        "trip_count",
                        "trips",
                        "total_trips",
                    ]
                    if c
                    in spark_hour_df.columns
                ),
                None,
            )

            if hour_col and count_col:
                top = spark_hour_df.sort_values(
                    [count_col, hour_col],
                    ascending=[False, True],
                ).iloc[0]

                spark_hour = int(
                    top[hour_col]
                )
                spark_hour_trips = int(
                    top[count_col]
                )

    pandas_hour = int(
        pandas_common[
            "busiest_pickup_hour"
        ]
    )
    pandas_hour_trips = int(
        pandas_common[
            "busiest_pickup_hour_trips"
        ]
    )

    if (
        spark_hour is None
        or spark_hour_trips is None
    ):
        add_result(
            validation_rows,
            "Busiest pickup hour",
            "N/A",
            (
                f"{pandas_hour:02d}:00 "
                f"({pandas_hour_trips:,} trips)"
            ),
            "",
            "Same hour and count",
            "PENDING",
            "January-only Spark hourly output was not found.",
        )
    else:
        passed = (
            spark_hour == pandas_hour
            and spark_hour_trips
            == pandas_hour_trips
        )

        add_result(
            validation_rows,
            "Busiest pickup hour",
            (
                f"{spark_hour:02d}:00 "
                f"({spark_hour_trips:,} trips)"
            ),
            (
                f"{pandas_hour:02d}:00 "
                f"({pandas_hour_trips:,} trips)"
            ),
            "",
            "Same hour and count",
            "PASS" if passed else "FAIL",
        )

    # =========================================================
    # 3. Top pickup zone
    # =========================================================
    spark_zone = None
    spark_zone_trips = None

    if spark_common is not None:
        for candidate in [
            "top_pickup_zone",
            "pickup_Zone",
            "pickup_zone",
        ]:
            if candidate in spark_common.index:
                spark_zone = str(
                    spark_common[candidate]
                )
                break

        for candidate in [
            "top_pickup_zone_trips",
            "trip_count",
            "trips",
        ]:
            if candidate in spark_common.index:
                spark_zone_trips = int(
                    spark_common[candidate]
                )
                break

    if (
        spark_zone is None
        or spark_zone_trips is None
    ):
        spark_zone_file = find_first(
            spark_root,
            [
                "jan2024_top_pickup_zones.csv",
                "top_pickup_zones_jan2024.csv",
                "spark_jan2024_top_pickup_zones.csv",
            ],
        )

        if spark_zone_file is not None:
            spark_zone_df = pd.read_csv(
                spark_zone_file
            )

            zone_col = next(
                (
                    c
                    for c in [
                        "pickup_Zone",
                        "pickup_zone",
                        "Zone",
                    ]
                    if c
                    in spark_zone_df.columns
                ),
                None,
            )

            count_col = next(
                (
                    c
                    for c in [
                        "trip_count",
                        "trips",
                    ]
                    if c
                    in spark_zone_df.columns
                ),
                None,
            )

            if zone_col and count_col:
                top = spark_zone_df.sort_values(
                    [count_col, zone_col],
                    ascending=[False, True],
                ).iloc[0]

                spark_zone = str(
                    top[zone_col]
                )
                spark_zone_trips = int(
                    top[count_col]
                )

    pandas_zone = str(
        pandas_common["top_pickup_zone"]
    )
    pandas_zone_trips = int(
        pandas_common[
            "top_pickup_zone_trips"
        ]
    )

    if (
        spark_zone is None
        or spark_zone_trips is None
    ):
        add_result(
            validation_rows,
            "Top pickup zone",
            "N/A",
            (
                f"{pandas_zone} "
                f"({pandas_zone_trips:,} trips)"
            ),
            "",
            "Same zone and count, except documented ties",
            "PENDING",
            "January-only Spark top-zone output was not found.",
        )
    else:
        passed = (
            spark_zone == pandas_zone
            and spark_zone_trips
            == pandas_zone_trips
        )

        add_result(
            validation_rows,
            "Top pickup zone",
            (
                f"{spark_zone} "
                f"({spark_zone_trips:,} trips)"
            ),
            (
                f"{pandas_zone} "
                f"({pandas_zone_trips:,} trips)"
            ),
            "",
            "Same zone and count, except documented ties",
            "PASS" if passed else "REVIEW",
            "" if passed else (
                "Check lookup naming and possible tied zones."
            ),
        )

    # =========================================================
    # 4. Average valid fare by distance band
    # =========================================================
    spark_distance_file = find_first(
        spark_root,
        [
            "jan2024_distance_band_summary.csv",
            "distance_band_summary_jan2024.csv",
            "spark_jan2024_distance_band_summary.csv",
        ],
    )

    if spark_distance_file is None:
        add_result(
            validation_rows,
            "Average fare by distance band",
            "N/A",
            "Available in pandas January summary",
            "",
            (
                "Absolute difference "
                f"<= {args.tolerance:.2f}"
            ),
            "PENDING",
            "January-only Spark distance-band output was not found.",
        )
    else:
        spark_distance = pd.read_csv(
            spark_distance_file
        )

        band_col = next(
            (
                c
                for c in [
                    "distance_band",
                    "band",
                ]
                if c
                in spark_distance.columns
            ),
            None,
        )

        spark_fare_col = next(
            (
                c
                for c in [
                    "avg_valid_fare_amount",
                    "avg_fare",
                    "average_fare",
                ]
                if c
                in spark_distance.columns
            ),
            None,
        )

        pandas_fare_col = next(
            (
                c
                for c in [
                    "avg_valid_fare_amount",
                    "avg_fare",
                    "average_fare",
                ]
                if c
                in pandas_distance.columns
            ),
            None,
        )

        if not band_col or not spark_fare_col:
            raise ValueError(
                "January Spark distance summary does not contain "
                "the required band/average-fare columns."
            )

        if pandas_fare_col is None:
            raise ValueError(
                "pandas distance_fare_summary.csv does not contain "
                "an average-fare column."
            )

        merged = pandas_distance[
            [
                "distance_band",
                pandas_fare_col,
            ]
        ].rename(
            columns={
                pandas_fare_col:
                    "pandas_avg_fare"
            }
        ).merge(
            spark_distance[
                [
                    band_col,
                    spark_fare_col,
                ]
            ].rename(
                columns={
                    band_col:
                        "distance_band",
                    spark_fare_col:
                        "spark_avg_fare",
                }
            ),
            on="distance_band",
            how="outer",
        )

        for _, row in merged.iterrows():
            p = row["pandas_avg_fare"]
            s = row["spark_avg_fare"]

            if pd.isna(p) or pd.isna(s):
                difference = np.nan
                status = "REVIEW"
            else:
                difference = abs(
                    float(p) - float(s)
                )

                status = (
                    "PASS"
                    if difference
                    <= args.tolerance
                    else "FAIL"
                )

            add_result(
                validation_rows,
                (
                    "Average fare - "
                    f"{row['distance_band']}"
                ),
                s,
                p,
                difference,
                (
                    "Absolute difference "
                    f"<= {args.tolerance:.2f}"
                ),
                status,
            )

    validation = pd.DataFrame(
        validation_rows
    )

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    validation.to_csv(
        output_file,
        index=False,
    )

    print("\nSECTION 6.1 RESULT")
    print("=" * 80)
    print(
        validation.to_string(
            index=False
        )
    )

    print(
        f"\nSaved planned output:\n{output_file}"
    )

    if temporary is not None:
        temporary.cleanup()


if __name__ == "__main__":
    main()
