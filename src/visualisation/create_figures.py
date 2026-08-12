"""
Generate the eight figures.
"""

from __future__ import annotations

import argparse
import tarfile
import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter


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

    # If your TAR filename is slightly different, edit this line only.
    args.spark_results = str(
        BASE_FOLDER / "nyc_handover_to_jinyon.tar.gz"
    )

    args.pandas_runtime = str(
        BASE_FOLDER
        / "results"
        / "benchmarks"
        / "pandas_runtime.csv"
    )

    args.output = str(
        BASE_FOLDER
        / "results"
        / "figures"
    )

    return args


def compact_count(value, _position=None):
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.0f}K"
    return f"{value:.0f}"


def save_figure(fig, output, filename):
    fig.tight_layout()
    fig.savefig(
        output / filename,
        dpi=300,
        bbox_inches="tight",
    )
    plt.close(fig)
    print(f"Created: {filename}")


def resolve_spark_results(path: Path):
    if path.is_dir():
        return path, None

    if not path.is_file():
        raise FileNotFoundError(path)

    temp = tempfile.TemporaryDirectory()

    with tarfile.open(path, "r:gz") as archive:
        archive.extractall(temp.name)

    candidates = list(
        Path(temp.name).rglob("monthly_trip_demand.csv")
    )

    if not candidates:
        temp.cleanup()
        raise FileNotFoundError(
            "Could not locate monthly_trip_demand.csv inside the handover."
        )

    return candidates[0].parent, temp


def find_first(folder: Path, names):
    for name in names:
        path = folder / name
        if path.is_file():
            return path

        nested = list(folder.rglob(name))
        if nested:
            return nested[0]

    return None


def read_optional(folder, names):
    path = find_first(folder, names)
    if path is None:
        return None, None
    return pd.read_csv(path), path


def normalise_weekend(series):
    if pd.api.types.is_bool_dtype(series):
        return series

    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map(
            {
                "true": True,
                "false": False,
                "1": True,
                "0": False,
            }
        )
    )


def main():
    args = parse_args()

    output = Path(args.output)
    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    pandas_runtime_file = Path(
        args.pandas_runtime
    )

    if not pandas_runtime_file.is_file():
        raise FileNotFoundError(
            pandas_runtime_file
        )

    spark_folder, temporary = resolve_spark_results(
        Path(args.spark_results)
    )

    missing = []

    # =========================================================
    # FIGURE 1 - Hourly demand
    # =========================================================
    hourly, hourly_path = read_optional(
        spark_folder,
        [
            "hourly_weekend_by_year.csv",
            "hourly_weekend_demand_by_year.csv",
            "hourly_weekend_demand.csv",
            "hourly_demand.csv",
        ],
    )

    if hourly is None:
        missing.append(
            "Figure 1: hourly demand CSV"
        )
    else:
        weekend_column = next(
            (
                c
                for c in ["is_weekend", "weekend"]
                if c in hourly.columns
            ),
            None,
        )

        if weekend_column is None:
            raise ValueError(
                f"{hourly_path.name} has no weekend/is_weekend column."
            )

        hourly[weekend_column] = normalise_weekend(
            hourly[weekend_column]
        )

        count_column = next(
            (
                c
                for c in [
                    "avg_daily_trips",
                    "trip_count",
                    "total_trips",
                    "trips",
                ]
                if c in hourly.columns
            ),
            None,
        )

        if count_column is None:
            raise ValueError(
                f"{hourly_path.name} has no usable trip-count column."
            )

        fig, ax = plt.subplots(
            figsize=(11, 6)
        )

        if "pickup_year" in hourly.columns:
            groups = hourly.groupby(
                ["pickup_year", weekend_column]
            )

            for (year, weekend), part in groups:
                part = part.sort_values(
                    "pickup_hour"
                )
                label = (
                    f"{int(year)} - "
                    f"{'Weekend' if weekend else 'Weekday'}"
                )
                ax.plot(
                    part["pickup_hour"],
                    part[count_column],
                    marker="o",
                    linewidth=2,
                    label=label,
                )

            y_label = (
                "Average trips per day"
                if count_column == "avg_daily_trips"
                else "Trips"
            )

        else:
            groups = hourly.groupby(
                weekend_column
            )

            for weekend, part in groups:
                part = part.sort_values(
                    "pickup_hour"
                )
                ax.plot(
                    part["pickup_hour"],
                    part[count_column],
                    marker="o",
                    linewidth=2,
                    label=(
                        "Weekend"
                        if weekend
                        else "Weekday"
                    ),
                )

            y_label = (
                "Average trips per day"
                if count_column == "avg_daily_trips"
                else "Trips across 2024-2025"
            )

        ax.set_title(
            "Yellow Taxi Hourly Demand: Weekday versus Weekend"
        )
        ax.set_xlabel("Pickup hour")
        ax.set_ylabel(y_label)
        ax.set_xticks(range(24))
        ax.yaxis.set_major_formatter(
            FuncFormatter(compact_count)
        )
        ax.grid(alpha=0.3)
        ax.legend()

        save_figure(
            fig,
            output,
            "figure_1_hourly_demand.png",
        )

    # =========================================================
    # FIGURE 2 - Top pickup zones
    # =========================================================
    zones, zones_path = read_optional(
        spark_folder,
        [
            "top20_pickup_zones.csv",
            "top_pickup_zones.csv",
        ],
    )

    if zones is None:
        missing.append(
            "Figure 2: top pickup zone CSV"
        )
    else:
        zone_column = next(
            (
                c
                for c in ["pickup_Zone", "pickup_zone"]
                if c in zones.columns
            ),
            None,
        )

        count_column = next(
            (
                c
                for c in ["trip_count", "trips"]
                if c in zones.columns
            ),
            None,
        )

        if zone_column is None or count_column is None:
            raise ValueError(
                f"{zones_path.name} has incompatible columns."
            )

        top10 = (
            zones.sort_values(
                count_column,
                ascending=False,
            )
            .head(10)
            .sort_values(
                count_column,
                ascending=True,
            )
        )

        fig, ax = plt.subplots(
            figsize=(11, 7)
        )

        bars = ax.barh(
            top10[zone_column],
            top10[count_column],
        )

        ax.set_title(
            "Top 10 Yellow Taxi Pickup Zones"
        )
        ax.set_xlabel("Trips")
        ax.set_ylabel("Pickup zone")
        ax.xaxis.set_major_formatter(
            FuncFormatter(compact_count)
        )
        ax.grid(
            axis="x",
            alpha=0.3,
        )

        ax.bar_label(
            bars,
            labels=[
                compact_count(value)
                for value in top10[count_column]
            ],
            padding=3,
            fontsize=8,
        )

        save_figure(
            fig,
            output,
            "figure_2_top_pickup_zones.png",
        )

    # =========================================================
    # FIGURE 3 - Average fare by distance band
    # =========================================================
    distance, distance_path = read_optional(
        spark_folder,
        [
            "distance_band_detailed_summary.csv",
            "distance_band_summary.csv",
            "distance_fare_summary.csv",
        ],
    )

    if distance is None:
        missing.append(
            "Figure 3: distance-band summary CSV"
        )
    else:
        fare_column = next(
            (
                c
                for c in [
                    "avg_valid_fare_amount",
                    "avg_fare",
                ]
                if c in distance.columns
            ),
            None,
        )

        if fare_column is None:
            raise ValueError(
                f"{distance_path.name} has no average-fare column."
            )

        band_order = [
            "0-<2",
            "2-<5",
            "5-<10",
            "10-<20",
            "20+",
            "Invalid/Non-positive",
        ]

        distance = distance.copy()
        distance["_order"] = (
            distance["distance_band"]
            .map(
                {
                    band: index
                    for index, band in enumerate(band_order)
                }
            )
        )

        distance = distance.sort_values(
            "_order"
        )

        fig, ax = plt.subplots(
            figsize=(11, 6)
        )

        bars = ax.bar(
            distance["distance_band"],
            distance[fare_column],
        )

        ax.set_title(
            "Average Valid Fare by Trip-Distance Band"
        )
        ax.set_xlabel("Distance band")
        ax.set_ylabel("Average fare (USD)")
        ax.tick_params(
            axis="x",
            rotation=25,
        )
        ax.grid(
            axis="y",
            alpha=0.3,
        )

        ax.bar_label(
            bars,
            labels=[
                (
                    f"${value:.2f}"
                    if pd.notna(value)
                    else ""
                )
                for value in distance[fare_column]
            ],
            padding=3,
            fontsize=8,
        )

        save_figure(
            fig,
            output,
            "figure_3_average_fare_by_distance.png",
        )

    # =========================================================
    # FIGURE 4 - Fare per mile
    # =========================================================
    if (
        distance is not None
        and "avg_fare_per_mile" in distance.columns
    ):
        fare_per_mile = distance.loc[
            distance["distance_band"]
            != "Invalid/Non-positive"
        ]

        fig, ax = plt.subplots(
            figsize=(11, 6)
        )

        bars = ax.bar(
            fare_per_mile["distance_band"],
            fare_per_mile["avg_fare_per_mile"],
        )

        ax.set_title(
            "Average Fare per Mile by Distance Band"
        )
        ax.set_xlabel("Distance band")
        ax.set_ylabel(
            "Average fare per mile (USD)"
        )
        ax.tick_params(
            axis="x",
            rotation=25,
        )
        ax.grid(
            axis="y",
            alpha=0.3,
        )

        ax.bar_label(
            bars,
            labels=[
                f"${value:.2f}"
                for value in fare_per_mile[
                    "avg_fare_per_mile"
                ]
            ],
            padding=3,
        )

        save_figure(
            fig,
            output,
            "figure_4_fare_per_mile.png",
        )
    else:
        missing.append(
            "Figure 4: avg_fare_per_mile is missing from the full Spark "
            "distance-band output."
        )

    # =========================================================
    # FIGURE 5 - Monthly trip volume
    # =========================================================
    monthly, monthly_path = read_optional(
        spark_folder,
        [
            "monthly_2024_2025_summary.csv",
            "year_month_comparison.csv",
            "monthly_trip_demand.csv",
        ],
    )

    if monthly is None:
        missing.append(
            "Figure 5: monthly 2024-2025 summary CSV"
        )
    else:
        trip_column = next(
            (
                c
                for c in ["trip_count", "trips"]
                if c in monthly.columns
            ),
            None,
        )

        if trip_column is None:
            raise ValueError(
                f"{monthly_path.name} has no monthly trip-count column."
            )

        fig, ax = plt.subplots(
            figsize=(11, 6)
        )

        for year, part in monthly.groupby(
            "pickup_year"
        ):
            part = part.sort_values(
                "pickup_month"
            )

            ax.plot(
                part["pickup_month"],
                part[trip_column],
                marker="o",
                linewidth=2,
                label=str(int(year)),
            )

        ax.set_title(
            "Monthly Yellow Taxi Trip Volume: 2024 versus 2025"
        )
        ax.set_xlabel("Month")
        ax.set_ylabel("Trips")
        ax.set_xticks(range(1, 13))
        ax.yaxis.set_major_formatter(
            FuncFormatter(compact_count)
        )
        ax.grid(alpha=0.3)
        ax.legend()

        save_figure(
            fig,
            output,
            "figure_5_monthly_trip_volume.png",
        )

    # =========================================================
    # FIGURE 6 - Monthly average total amount
    # =========================================================
    total_column = None

    if monthly is not None:
        total_column = next(
            (
                c
                for c in [
                    "avg_total_amount",
                    "average_total_amount",
                ]
                if c in monthly.columns
            ),
            None,
        )

    if monthly is not None and total_column is not None:
        fig, ax = plt.subplots(
            figsize=(11, 6)
        )

        for year, part in monthly.groupby(
            "pickup_year"
        ):
            part = part.sort_values(
                "pickup_month"
            )

            ax.plot(
                part["pickup_month"],
                part[total_column],
                marker="o",
                linewidth=2,
                label=str(int(year)),
            )

        ax.set_title(
            "Monthly Average Total Amount: 2024 versus 2025"
        )
        ax.set_xlabel("Month")
        ax.set_ylabel(
            "Average total amount (USD)"
        )
        ax.set_xticks(range(1, 13))
        ax.grid(alpha=0.3)
        ax.legend()

        save_figure(
            fig,
            output,
            "figure_6_average_total_amount.png",
        )
    else:
        missing.append(
            "Figure 6: monthly avg_total_amount is missing from the "
            "current Spark handover."
        )

    # =========================================================
    # FIGURE 7 - CBD fee in 2025
    # =========================================================
    cbd_column = None

    if monthly is not None:
        cbd_column = next(
            (
                c
                for c in [
                    "avg_cbd_congestion_fee",
                    "cbd_fee_charged_share_pct",
                    "trips_with_cbd_fee_pct",
                ]
                if c in monthly.columns
            ),
            None,
        )

    if monthly is not None and cbd_column is not None:
        cbd = monthly.loc[
            monthly["pickup_year"].eq(2025)
        ].sort_values(
            "pickup_month"
        )

        fig, ax = plt.subplots(
            figsize=(11, 6)
        )

        bars = ax.bar(
            cbd["pickup_month"],
            cbd[cbd_column],
        )

        if "pct" in cbd_column.lower() or "share" in cbd_column.lower():
            title = (
                "Share of 2025 Trips with a CBD Congestion Fee"
            )
            ylabel = "Trips with CBD fee (%)"
            labels = [
                f"{value:.1f}%"
                for value in cbd[cbd_column]
            ]
        else:
            title = (
                "Average CBD Congestion Fee in 2025"
            )
            ylabel = "Average CBD fee (USD)"
            labels = [
                f"${value:.2f}"
                for value in cbd[cbd_column]
            ]

        ax.set_title(title)
        ax.set_xlabel("Month")
        ax.set_ylabel(ylabel)
        ax.set_xticks(range(1, 13))
        ax.grid(
            axis="y",
            alpha=0.3,
        )

        ax.bar_label(
            bars,
            labels=labels,
            padding=3,
            fontsize=8,
        )

        save_figure(
            fig,
            output,
            "figure_7_cbd_fee_2025.png",
        )
    else:
        missing.append(
            "Figure 7: monthly CBD-fee statistic is missing from the "
            "current Spark handover."
        )

    # =========================================================
    # FIGURE 8 - Runtime comparison
    # =========================================================
    pandas_runtime = pd.read_csv(
        pandas_runtime_file
    )

    spark_runtime, spark_runtime_path = read_optional(
        spark_folder,
        [
            "spark_runtime.csv",
            "benchmark_results.csv",
        ],
    )

    if spark_runtime is None:
        missing.append(
            "Figure 8: Spark benchmark result"
        )
    else:
        fig, ax = plt.subplots(
            figsize=(11, 6)
        )

        # Final Figure 8 is valid only when Spark provides a matching
        # month-scale benchmark such as 1/3/6/12 months.
        if {
            "months",
            "median_seconds",
        }.issubset(spark_runtime.columns):
            spark_plot = spark_runtime.sort_values(
                "months"
            )

            ax.plot(
                spark_plot["months"],
                spark_plot["median_seconds"],
                marker="o",
                linewidth=2,
                label="Spark SQL on EMR",
            )

            pandas_ok = pandas_runtime.loc[
                pandas_runtime["status"].eq("SUCCESS")
            ].copy()

            if not pandas_ok.empty:
                ax.plot(
                    pandas_ok["months"],
                    pandas_ok["runtime_seconds"],
                    marker="o",
                    linewidth=2,
                    label="pandas local",
                )

            ax.set_title(
                "Runtime Growth by Number of Monthly Files"
            )
            ax.set_xlabel("Months")
            ax.set_ylabel("Runtime (seconds)")
            ax.grid(alpha=0.3)
            ax.legend()

            save_figure(
                fig,
                output,
                "figure_8_runtime_comparison.png",
            )

        elif {
            "benchmark",
            "elapsed_seconds",
        }.issubset(spark_runtime.columns):
            # The current Jie Long handover contains different Spark tasks,
            # not a matched 1/3/6/12-month scaling experiment. Keep it as
            # supplementary evidence, but do not label it as the final
            # Spark-vs-pandas runtime comparison.
            plt.close(fig)

            fig, ax = plt.subplots(
                figsize=(11, 6)
            )

            spark_plot = spark_runtime.copy()
            labels = (
                spark_plot["benchmark"].astype(str)
                + "\n"
                + spark_plot["method"].astype(str)
            )

            bars = ax.bar(
                labels,
                spark_plot["elapsed_seconds"],
            )

            ax.set_title(
                "Observed Spark Benchmark Tasks"
            )
            ax.set_xlabel("Spark benchmark task")
            ax.set_ylabel("Runtime (seconds)")
            ax.tick_params(
                axis="x",
                rotation=20,
            )
            ax.grid(
                axis="y",
                alpha=0.3,
            )

            ax.bar_label(
                bars,
                labels=[
                    f"{value:.2f}s"
                    for value in spark_plot["elapsed_seconds"]
                ],
                padding=3,
            )

            save_figure(
                fig,
                output,
                "supplementary_spark_benchmark_tasks.png",
            )

            missing.append(
                "Figure 8: Jie Long's current benchmark_results.csv does "
                "not contain matching 1/3/6/12-month Spark runtimes. A "
                "month-scale Spark benchmark CSV with columns months and "
                "median_seconds is needed for a fair runtime-growth chart."
            )

        else:
            plt.close(fig)
            missing.append(
                "Figure 8: Spark benchmark file has an unsupported format."
            )

    # =========================================================
    # Record missing final inputs
    # =========================================================
    missing_file = output / "MISSING_FIGURE_INPUTS.txt"

    if missing:
        missing_file.write_text(
            "The following final figure inputs are still missing:\n\n"
            + "\n".join(
                f"- {item}"
                for item in missing
            )
            + (
                "\n\nDo not invent these values. Add Jie Long's enhanced "
                "Spark summary CSVs and rerun B5_create_figures.py."
            ),
            encoding="utf-8",
        )

        print(
            f"\nSome figures need additional Spark outputs. "
            f"See: {missing_file}"
        )
    else:
        if missing_file.exists():
            missing_file.unlink()

        print(
            "\nAll eight required figures were created successfully."
        )

    if temporary is not None:
        temporary.cleanup()


if __name__ == "__main__":
    main()
