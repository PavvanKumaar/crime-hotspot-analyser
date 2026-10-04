"""
Crime Hotspot Detection and Spatial-Temporal Analysis -- entry point.

Run with:
    python main.py

See README.md for dataset download and environment setup instructions.
"""

import os
import time

from pyspark.sql import SparkSession

import config
from src.data_loader import load_crime_data
from src.data_cleaning import clean_data
from src.spatial_grid import assign_grid_cells
from src.hotspot_analysis import aggregate_by_grid, rank_hotspots
from src.temporal_analysis import (
    category_summary,
    hotspot_category_breakdown,
    monthly_counts,
    hourly_counts,
    day_night_comparison,
)
from src.visualization import (
    build_hotspot_map,
    plot_top_hotspots_bar,
    plot_category_bar,
    plot_monthly_line,
    plot_hourly_bar,
)


def main() -> None:
    start_time = time.time()
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.CHARTS_DIR, exist_ok=True)

    spark = SparkSession.builder.appName("CrimeHotspotAnalysis").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # --- FR-1: Load -----------------------------------------------------
        raw_df = load_crime_data(spark, config.INPUT_CSV_PATH)

        # --- FR-2: Clean ------------------------------------------------------
        cleaned_df, clean_stats = clean_data(
            raw_df, config.COLUMNS, config.DATE_FORMAT,
            config.LAT_MIN, config.LAT_MAX, config.LON_MIN, config.LON_MAX,
        )

        # Reused repeatedly below (FR-5/FR-6/FR-7) -> cache once rather than
        # recomputing the clean + parse chain on every downstream aggregation.
        # Justification: this dataframe is read at least 5 times further
        # down (grid agg, category summary, monthly, hourly, day/night), and
        # it's small enough post-cleaning to comfortably fit in memory.
        cleaned_df = cleaned_df.cache()
        cleaned_count = cleaned_df.count()  # materializes the cache

        # --- FR-3: Spatial grid -------------------------------------------
        gridded_df = assign_grid_cells(cleaned_df, config.GRID_CELL_SIZE_DEGREES)

        # --- FR-4: Aggregation + FR-5: Hotspot ranking ------------------------
        grid_counts = aggregate_by_grid(gridded_df)
        hotspots = rank_hotspots(grid_counts, config.TOP_N_HOTSPOTS)
        hotspots_cached = hotspots.cache()

        # --- Acceptance check: row-count conservation --------------------
        # Every retained incident must land in exactly one grid cell, and the
        # grid-cell counts must sum back to the cleaned record count -- this
        # catches silent row loss/duplication introduced by the join/groupBy
        # stages above.
        sum_of_grid_counts = grid_counts.agg({"incident_count": "sum"}).collect()[0][0]
        assert sum_of_grid_counts == cleaned_count, (
            f"Row-count conservation check FAILED: cleaned={cleaned_count} "
            f"vs sum(grid counts)={sum_of_grid_counts}"
        )

        # --- FR-6: Category analysis ---------------------------------------
        cat_summary = category_summary(gridded_df)
        hotspot_cats = hotspot_category_breakdown(gridded_df, hotspots_cached)

        # --- FR-7: Temporal analysis ----------------------------------------
        monthly = monthly_counts(gridded_df)
        hourly = hourly_counts(gridded_df)
        day_night = day_night_comparison(gridded_df)

        # --- FR-8: Map + FR-9: Charts -----------------------------------------
        build_hotspot_map(
            hotspots_cached, config.GRID_CELL_SIZE_DEGREES,
            os.path.join(config.OUTPUT_DIR, "hotspot_map.html"),
        )
        plot_top_hotspots_bar(hotspots_cached, os.path.join(config.CHARTS_DIR, "top_hotspots_bar.png"))
        plot_category_bar(cat_summary, os.path.join(config.CHARTS_DIR, "category_bar.png"))
        plot_monthly_line(monthly, os.path.join(config.CHARTS_DIR, "monthly_line.png"))
        plot_hourly_bar(hourly, os.path.join(config.CHARTS_DIR, "hourly_bar.png"))

        # --- FR-10: Exports ---------------------------------------------------
        _export_csv(grid_counts, os.path.join(config.OUTPUT_DIR, "grid_crime_counts.csv"))
        _export_csv(hotspots_cached, os.path.join(config.OUTPUT_DIR, "top_hotspots.csv"))
        _export_csv(cat_summary, os.path.join(config.OUTPUT_DIR, "crime_category_summary.csv"))
        _export_csv(hotspot_cats, os.path.join(config.OUTPUT_DIR, "hotspot_category_breakdown.csv"))
        _export_csv(monthly, os.path.join(config.OUTPUT_DIR, "monthly_crime_summary.csv"))
        _export_csv(hourly, os.path.join(config.OUTPUT_DIR, "hourly_crime_summary.csv"))
        _export_csv(day_night, os.path.join(config.OUTPUT_DIR, "day_night_summary.csv"))

        num_occupied_cells = grid_counts.count()
        elapsed = time.time() - start_time

        # --- Section 13: execution summary -----------------------------------
        print("=" * 60)
        print("EXECUTION SUMMARY")
        print("=" * 60)
        print(f"Input record count:          {clean_stats['input_count']}")
        print(f"Valid (cleaned) record count: {clean_stats['cleaned_count']}")
        print(f"Removed record count:        {clean_stats['total_removed']}")
        print(f"Grid cells with incidents:   {num_occupied_cells}")
        print(f"Hotspot cells exported:      {config.TOP_N_HOTSPOTS}")
        print(f"Row-count conservation check: PASSED "
              f"({cleaned_count} cleaned == {sum_of_grid_counts} summed across cells)")
        print(f"Elapsed time:                {elapsed:.1f}s")
        print(f"Output directory:            {os.path.abspath(config.OUTPUT_DIR)}")
        print("Outputs:")
        for fname in sorted(os.listdir(config.OUTPUT_DIR)):
            if os.path.isfile(os.path.join(config.OUTPUT_DIR, fname)):
                print(f"  - {os.path.join(config.OUTPUT_DIR, fname)}")
        for fname in sorted(os.listdir(config.CHARTS_DIR)):
            print(f"  - {os.path.join(config.CHARTS_DIR, fname)}")

    finally:
        spark.stop()


def _export_csv(spark_df, path: str) -> None:
    """Convert a small, already-aggregated Spark result to a single CSV via
    Pandas (per tech stack: Pandas handles small-result export), avoiding
    Spark's own multi-part CSV output for these summary-sized tables."""
    spark_df.toPandas().to_csv(path, index=False)
    print(f"FR-10: Export saved -> {path}")


if __name__ == "__main__":
    main()
