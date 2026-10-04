"""
FR-4: Crime Aggregation
FR-5: Hotspot Identification

Groups cleaned, grid-assigned crime records by cell and ranks them by
incident count. These are deliberately called "high-count cells", never
"statistically significant hotspots" -- that label would require a proper
spatial-statistics test (e.g. Getis-Ord Gi*) that is out of scope here
(see README "Limitations").
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_by_grid(df: DataFrame) -> DataFrame:
    """FR-4: group by grid cell and compute per-cell summary stats."""
    grid_counts = (
        df.groupBy("cell_x", "cell_y", "cell_center_lat", "cell_center_lon")
          .agg(
              F.count("*").alias("incident_count"),
              F.countDistinct("category").alias("distinct_categories"),
              F.min("event_ts").alias("earliest_incident"),
              F.max("event_ts").alias("latest_incident"),
          )
          .orderBy(F.col("incident_count").desc())
    )

    # Technical note (kept lightweight, no extra pipeline stage): print the
    # physical plan for this aggregation so the shuffle/exchange stage Spark
    # introduces for the groupBy is visible and explainable in the report.
    print("=" * 60)
    print("FR-4: CRIME AGGREGATION")
    print("=" * 60)
    print(f"Partitions used for aggregation: {df.rdd.getNumPartitions()}")
    print("Physical plan for the grid-cell aggregation (note the shuffle/exchange"
          " stage introduced by groupBy -- this is the distributed computation"
          " this project is built to demonstrate):")
    grid_counts.explain()
    print()

    return grid_counts


def rank_hotspots(grid_counts: DataFrame, top_n: int) -> DataFrame:
    """FR-5: take the top N cells by incident_count.

    These are high-count cells, ranked by raw total -- NOT statistically
    validated hotspots. No clustering significance test has been applied.
    """
    hotspots = grid_counts.limit(top_n)

    print("=" * 60)
    print(f"FR-5: HOTSPOT IDENTIFICATION (top {top_n} high-count cells)")
    print("=" * 60)
    print("Note: ranked by raw incident count only. These are 'high-count")
    print("cells', not statistically significant clusters -- no significance")
    print("test (e.g. Getis-Ord Gi*) has been applied. See README Limitations.")
    print()

    return hotspots
