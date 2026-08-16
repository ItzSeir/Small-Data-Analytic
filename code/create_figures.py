#!/usr/bin/env python3
"""
create_figures.py - report figures using the final pandas analytical outputs.

Sections
--------
5.5 Fare Patterns by Distance Band
5.6 Passenger-Count Patterns
5.7 Weekday versus Weekend
5.8 2024 versus 2025 Comparison
6.2 pandas runtime/scalability
6.3 pandas memory/resource growth

Run B2 full-period analysis first, then B4 benchmark, then run this script.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter


BASE = Path(
    r"C:\Users\Jin Yon\Downloads\NYC_Taxi"
)

PANDAS_RESULTS = (
    BASE
    / "results"
    / "pandas-full-2024-2025"
)

PANDAS_RUNTIME = (
    BASE
    / "results"
    / "benchmarks"
    / "pandas_runtime.csv"
)

OUTPUT = (
    BASE
    / "results"
    / "figures_pandas_full_2024_2025"
)


def compact(value, _pos=None):
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.0f}K"
    return f"{value:.0f}"


def require(name):
    path = PANDAS_RESULTS / name
    if not path.is_file():
        raise FileNotFoundError(
            f"{name} not found. Run B2 full-period analysis first.\n"
            f"Expected: {path}"
        )
    return path


def save(fig, filename):
    fig.tight_layout()
    fig.savefig(
        OUTPUT / filename,
        dpi=300,
        bbox_inches="tight",
    )
    plt.close(fig)
    print("Created:", filename)


def main():
    OUTPUT.mkdir(
        parents=True,
        exist_ok=True,
    )

    # =========================================================
    # 5.5 Fare Patterns by Distance Band
    # =========================================================
    distance = pd.read_csv(
        require("distance_fare_summary.csv")
    )

    band_order = [
        "0-<2",
        "2-<5",
        "5-<10",
        "10-<20",
        "20+",
    ]

    distance = distance.loc[
        distance["distance_band"].isin(band_order)
    ].copy()

    distance["_order"] = distance[
        "distance_band"
    ].map(
        {
            band: i
            for i, band in enumerate(band_order)
        }
    )
    distance = distance.sort_values("_order")

    x = np.arange(len(distance))
    width = 0.38

    fig, ax = plt.subplots(figsize=(9.6, 5.5))

    bars1 = ax.bar(
        x - width / 2,
        distance["avg_valid_fare_amount"],
        width,
        label="Average fare",
    )
    bars2 = ax.bar(
        x + width / 2,
        distance["median_valid_fare_amount"],
        width,
        label="Median fare",
    )

    ax.set_title(
        "Average and Median Fare by Distance Band, 2024-2025"
    )
    ax.set_xlabel("Distance Band (Miles)")
    ax.set_ylabel("Fare (USD)")
    ax.set_xticks(x)
    ax.set_xticklabels(distance["distance_band"])
    ax.grid(axis="y", alpha=0.25)
    ax.legend()

    ax.bar_label(
        bars1,
        labels=[
            f"${v:.2f}"
            for v in distance["avg_valid_fare_amount"]
        ],
        padding=2,
        fontsize=8,
    )
    ax.bar_label(
        bars2,
        labels=[
            f"${v:.2f}"
            for v in distance["median_valid_fare_amount"]
        ],
        padding=2,
        fontsize=8,
    )

    save(
        fig,
        "figure_5_5_1_average_median_fare_2024_2025.png",
    )

    fig, ax = plt.subplots(figsize=(9.2, 5.4))
    bars = ax.bar(
        distance["distance_band"],
        distance["avg_total_amount_for_valid_fare"],
    )
    ax.set_title(
        "Average Total Amount by Distance Band, 2024-2025"
    )
    ax.set_xlabel("Distance Band (Miles)")
    ax.set_ylabel("Average Total Amount (USD)")
    ax.grid(axis="y", alpha=0.25)
    ax.bar_label(
        bars,
        labels=[
            f"${v:.2f}"
            for v in distance[
                "avg_total_amount_for_valid_fare"
            ]
        ],
        padding=3,
        fontsize=8,
    )
    save(
        fig,
        "figure_5_5_2_average_total_amount_2024_2025.png",
    )

    fig, ax = plt.subplots(figsize=(9.2, 5.4))
    bars = ax.bar(
        distance["distance_band"],
        distance["avg_fare_per_mile"],
    )
    ax.set_title(
        "Average Fare per Mile by Distance Band, 2024-2025"
    )
    ax.set_xlabel("Distance Band (Miles)")
    ax.set_ylabel("Average Fare per Mile (USD)")
    ax.grid(axis="y", alpha=0.25)
    ax.bar_label(
        bars,
        labels=[
            f"${v:.2f}"
            for v in distance["avg_fare_per_mile"]
        ],
        padding=3,
        fontsize=8,
    )
    save(
        fig,
        "figure_5_5_3_fare_per_mile_2024_2025.png",
    )

    fig, ax = plt.subplots(figsize=(9.2, 5.4))
    bars = ax.bar(
        distance["distance_band"],
        distance["avg_trip_duration_min"],
    )
    ax.set_title(
        "Average Trip Duration by Distance Band, 2024-2025"
    )
    ax.set_xlabel("Distance Band (Miles)")
    ax.set_ylabel("Average Duration (Minutes)")
    ax.grid(axis="y", alpha=0.25)
    ax.bar_label(
        bars,
        labels=[
            f"{v:.1f}"
            for v in distance["avg_trip_duration_min"]
        ],
        padding=3,
        fontsize=8,
    )
    save(
        fig,
        "figure_5_5_4_average_duration_2024_2025.png",
    )

    distance.drop(
        columns=["_order"]
    ).to_csv(
        OUTPUT / "table_5_5_distance_fare_2024_2025.csv",
        index=False,
    )

    # =========================================================
    # 5.6 Passenger-Count Patterns
    # =========================================================
    passenger = pd.read_csv(
        require("passenger_count_summary.csv")
    )

    passenger_order = [
        "Missing", "0", "1", "2", "3", "4", "5", "6", "7+"
    ]

    passenger["_order"] = passenger[
        "passenger_category"
    ].map(
        {
            category: i
            for i, category in enumerate(passenger_order)
        }
    )
    passenger = passenger.sort_values("_order")

    fig, ax = plt.subplots(figsize=(9.4, 5.5))
    bars = ax.bar(
        passenger["passenger_category"],
        passenger["share_pct"],
    )
    ax.set_title(
        "Passenger-Count Distribution, 2024-2025"
    )
    ax.set_xlabel("Passenger Count")
    ax.set_ylabel(
        "Share of Structurally Valid Trips (%)"
    )
    ax.grid(axis="y", alpha=0.25)

    ax.bar_label(
        bars,
        labels=[
            f"{v:.2f}%"
            for v in passenger["share_pct"]
        ],
        padding=3,
        fontsize=8,
    )

    save(
        fig,
        "figure_5_6_1_passenger_count_distribution_2024_2025.png",
    )

    single_multiple = pd.read_csv(
        require("passenger_single_multiple_summary.csv")
    )

    fig, ax = plt.subplots(figsize=(8.6, 5.2))
    bars = ax.bar(
        single_multiple["passenger_group"],
        single_multiple["share_of_valid_1_to_6_pct"],
    )
    ax.set_title(
        "Single versus Multiple Passenger Trips, 2024-2025"
    )
    ax.set_xlabel("Passenger Group")
    ax.set_ylabel(
        "Share of Valid 1-6 Passenger Trips (%)"
    )
    ax.grid(axis="y", alpha=0.25)

    ax.bar_label(
        bars,
        labels=[
            f"{v:.1f}%"
            for v in single_multiple[
                "share_of_valid_1_to_6_pct"
            ]
        ],
        padding=3,
    )

    save(
        fig,
        "figure_5_6_2_single_multiple_2024_2025.png",
    )

    passenger.drop(
        columns=["_order"]
    ).to_csv(
        OUTPUT / "table_5_6_passenger_count_2024_2025.csv",
        index=False,
    )
    single_multiple.to_csv(
        OUTPUT / "table_5_6_single_multiple_2024_2025.csv",
        index=False,
    )

    # =========================================================
    # 5.7 Weekday versus Weekend
    # =========================================================
    ww = pd.read_csv(
        require("weekday_weekend_summary.csv")
    )
    ww_hourly = pd.read_csv(
        require("weekday_weekend_hourly.csv")
    )

    fig, ax = plt.subplots(figsize=(9.8, 5.5))

    for day_type, part in ww_hourly.groupby(
        "day_type"
    ):
        part = part.sort_values("pickup_hour")
        ax.plot(
            part["pickup_hour"],
            part["avg_daily_trips"],
            marker="o",
            linewidth=2,
            label=day_type,
        )

    ax.set_title(
        "Average Daily Yellow Taxi Demand by Hour, 2024-2025"
    )
    ax.set_xlabel("Pickup Hour")
    ax.set_ylabel("Average Trips per Day")
    ax.set_xticks(range(24))
    ax.grid(alpha=0.25)
    ax.legend()

    save(
        fig,
        "figure_5_7_1_weekday_weekend_hourly_2024_2025.png",
    )

    ww.to_csv(
        OUTPUT / "table_5_7_weekday_weekend_2024_2025.csv",
        index=False,
    )

    # =========================================================
    # 5.8 2024 versus 2025
    # =========================================================
    monthly = pd.read_csv(
        require("monthly_year_comparison.csv")
    )
    year_summary = pd.read_csv(
        require("year_comparison_summary.csv")
    )

    fig, ax = plt.subplots(figsize=(9.8, 5.5))

    for year, part in monthly.groupby(
        "pickup_year"
    ):
        part = part.sort_values("pickup_month")
        ax.plot(
            part["pickup_month"],
            part["trip_count"],
            marker="o",
            linewidth=2,
            label=str(year),
        )

    ax.set_title(
        "Monthly Yellow Taxi Trip Volume: 2024 versus 2025"
    )
    ax.set_xlabel("Month")
    ax.set_ylabel("Structurally Valid Trips")
    ax.set_xticks(range(1, 13))
    ax.yaxis.set_major_formatter(
        FuncFormatter(compact)
    )
    ax.grid(alpha=0.25)
    ax.legend()

    save(
        fig,
        "figure_5_8_1_monthly_trip_volume.png",
    )

    # Average daily trips by month.
    monthly["days_in_month"] = pd.to_datetime(
        monthly["pickup_year"].astype(str)
        + "-"
        + monthly["pickup_month"].astype(str)
        + "-01"
    ).dt.days_in_month

    monthly["avg_daily_trips"] = (
        monthly["trip_count"]
        / monthly["days_in_month"]
    )

    fig, ax = plt.subplots(figsize=(9.8, 5.5))

    for year, part in monthly.groupby(
        "pickup_year"
    ):
        part = part.sort_values("pickup_month")
        ax.plot(
            part["pickup_month"],
            part["avg_daily_trips"],
            marker="o",
            linewidth=2,
            label=str(year),
        )

    ax.set_title(
        "Average Daily Yellow Taxi Trips by Month: 2024 versus 2025"
    )
    ax.set_xlabel("Month")
    ax.set_ylabel("Average Trips per Day")
    ax.set_xticks(range(1, 13))
    ax.yaxis.set_major_formatter(
        FuncFormatter(compact)
    )
    ax.grid(alpha=0.25)
    ax.legend()

    save(
        fig,
        "figure_5_8_2_average_daily_trips.png",
    )

    # Monthly average fare comparison.
    fig, ax = plt.subplots(figsize=(9.8, 5.5))

    for year, part in monthly.groupby(
        "pickup_year"
    ):
        part = part.sort_values("pickup_month")
        ax.plot(
            part["pickup_month"],
            part["avg_valid_fare_amount"],
            marker="o",
            linewidth=2,
            label=str(year),
        )

    ax.set_title(
        "Monthly Average Valid Fare: 2024 versus 2025"
    )
    ax.set_xlabel("Month")
    ax.set_ylabel("Average Valid Fare (USD)")
    ax.set_xticks(range(1, 13))
    ax.grid(alpha=0.25)
    ax.legend()

    save(
        fig,
        "figure_5_8_3_monthly_average_fare.png",
    )

    # CBD-fee charged share where available.
    cbd = monthly.loc[
        monthly["cbd_fee_charged_share_pct"].notna()
    ].copy()

    if not cbd.empty:
        fig, ax = plt.subplots(figsize=(9.8, 5.5))

        for year, part in cbd.groupby(
            "pickup_year"
        ):
            part = part.sort_values("pickup_month")
            ax.plot(
                part["pickup_month"],
                part["cbd_fee_charged_share_pct"],
                marker="o",
                linewidth=2,
                label=str(year),
            )

        ax.set_title(
            "CBD Congestion Fee Charged Share by Month"
        )
        ax.set_xlabel("Month")
        ax.set_ylabel("Trips with Positive CBD Fee (%)")
        ax.set_xticks(range(1, 13))
        ax.grid(alpha=0.25)
        ax.legend()

        save(
            fig,
            "figure_5_8_4_cbd_fee_charged_share.png",
        )

    monthly.to_csv(
        OUTPUT / "table_5_8_monthly_2024_2025.csv",
        index=False,
    )
    year_summary.to_csv(
        OUTPUT / "table_5_8_year_summary.csv",
        index=False,
    )

    # =========================================================
    # 6.2 and 6.3 pandas benchmark
    # =========================================================
    if PANDAS_RUNTIME.is_file():
        runtime = pd.read_csv(
            PANDAS_RUNTIME
        )
        ok = runtime.loc[
            runtime["status"].eq("SUCCESS")
        ].copy()

        fig, ax = plt.subplots(figsize=(8.8, 5.2))
        ax.plot(
            ok["months"],
            ok["runtime_seconds"],
            marker="o",
            linewidth=2,
        )
        ax.set_title(
            "pandas Runtime Growth by Number of Monthly Files"
        )
        ax.set_xlabel("Months")
        ax.set_ylabel("Runtime (Seconds)")
        ax.set_xticks(ok["months"].tolist())
        ax.grid(alpha=0.25)

        save(
            fig,
            "figure_6_2_pandas_runtime.png",
        )

        memory_col = (
            "rss_mb_peak_observed"
            if "rss_mb_peak_observed" in ok.columns
            else "rss_mb_after_workload"
        )

        fig, ax = plt.subplots(figsize=(8.8, 5.2))
        ax.plot(
            ok["months"],
            ok[memory_col],
            marker="o",
            linewidth=2,
        )
        ax.set_title(
            "pandas Observed Memory by Number of Monthly Files"
        )
        ax.set_xlabel("Months")
        ax.set_ylabel("RSS Memory (MB)")
        ax.set_xticks(ok["months"].tolist())
        ax.grid(alpha=0.25)

        save(
            fig,
            "figure_6_3_pandas_memory.png",
        )

        runtime.to_csv(
            OUTPUT / "table_6_2_pandas_runtime.csv",
            index=False,
        )

    print(
        f"\nAll full-period pandas figures saved to:\n{OUTPUT}"
    )


if __name__ == "__main__":
    main()
