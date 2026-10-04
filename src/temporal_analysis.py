"""
FR-6: Crime Category Analysis
FR-7: Temporal Analysis

All temporal summaries operate only on rows with has_valid_date == True,
per FR-2/FR-7: records with valid coordinates but missing/unparseable
dates are retained upstream for spatial analysis but excluded here.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import config


def category_summary(df: DataFrame) -> DataFrame:
    """FR-6: total incidents per category, most frequent first."""
    result = (
        df.groupBy("category")
          .agg(F.count("*").alias("incident_count"))
          .orderBy(F.col("incident_count").desc())
    )
    print("=" * 60)
    print("FR-6: CRIME CATEGORY ANALYSIS")
    print("=" * 60)
    result.show(10, truncate=False)
    return result


def hotspot_category_breakdown(df: DataFrame, hotspots: DataFrame) -> DataFrame:
    """FR-6: category counts restricted to the identified high-count cells."""
    hotspot_cells = hotspots.select("cell_x", "cell_y")
    joined = df.join(hotspot_cells, on=["cell_x", "cell_y"], how="inner")
    return (
        joined.groupBy("cell_x", "cell_y", "category")
              .agg(F.count("*").alias("incident_count"))
              .orderBy(F.col("cell_x"), F.col("cell_y"), F.col("incident_count").desc())
    )


def _valid_dates(df: DataFrame) -> DataFrame:
    return df.filter(F.col("has_valid_date"))


def monthly_counts(df: DataFrame) -> DataFrame:
    """FR-7: incidents per calendar month (year-month)."""
    valid = _valid_dates(df)
    result = (
        valid.withColumn("year_month", F.date_format("event_ts", "yyyy-MM"))
             .groupBy("year_month")
             .agg(F.count("*").alias("incident_count"))
             .orderBy("year_month")
    )
    excluded = df.count() - valid.count()
    print("=" * 60)
    print("FR-7: TEMPORAL ANALYSIS -- monthly counts")
    print(f"(excluded {excluded} records with missing/invalid dates)")
    print("=" * 60)
    return result


def hourly_counts(df: DataFrame) -> DataFrame:
    """FR-7: incidents per hour of day, when time-of-day data is available."""
    valid = _valid_dates(df)
    return (
        valid.withColumn("hour", F.hour("event_ts"))
             .groupBy("hour")
             .agg(F.count("*").alias("incident_count"))
             .orderBy("hour")
    )


def day_night_comparison(df: DataFrame) -> DataFrame:
    """FR-7: day vs night incident counts using config.DAY_START_HOUR /
    config.NIGHT_START_HOUR. Hours in [DAY_START, NIGHT_START) = 'day'."""
    valid = _valid_dates(df)
    labeled = valid.withColumn(
        "period",
        F.when(
            (F.hour("event_ts") >= config.DAY_START_HOUR) & (F.hour("event_ts") < config.NIGHT_START_HOUR),
            F.lit("day"),
        ).otherwise(F.lit("night")),
    )
    result = labeled.groupBy("period").agg(F.count("*").alias("incident_count"))
    print(f"Day/night split: day = [{config.DAY_START_HOUR}:00, {config.NIGHT_START_HOUR}:00), else night")
    return result
