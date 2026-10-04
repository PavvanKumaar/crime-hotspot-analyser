"""
FR-2: Data Cleaning

Standardizes column names, validates coordinates, parses dates, removes
duplicates, and reports how many records were removed and why.

Output schema after cleaning (fixed internal names, regardless of the
source dataset's own column names):
    crime_id, category, raw_date, event_ts, latitude, longitude, has_valid_date
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def _require_columns(df: DataFrame, columns: dict) -> None:
    missing = [
        col for key, col in columns.items()
        if key in ("category", "date", "latitude", "longitude") and col not in df.columns
    ]
    if missing:
        raise ValueError(
            f"Configured column(s) not found in dataset: {missing}. "
            f"Available columns: {df.columns}. Update COLUMNS in config.py."
        )


def clean_data(df: DataFrame, columns: dict, date_format: str,
               lat_min: float, lat_max: float,
               lon_min: float, lon_max: float) -> tuple[DataFrame, dict]:
    """Clean the raw dataframe and return (cleaned_df, stats)."""
    _require_columns(df, columns)
    input_count = df.count()

    has_id = "id" in columns and columns["id"] in df.columns

    # --- Standardize column names -----------------------------------------
    std = df.select(
        (F.col(columns["id"]).cast("string") if has_id else F.monotonically_increasing_id().cast("string")).alias("crime_id"),
        F.col(columns["category"]).alias("category"),
        F.col(columns["date"]).alias("raw_date"),
        F.col(columns["latitude"]).cast(DoubleType()).alias("latitude"),
        F.col(columns["longitude"]).cast(DoubleType()).alias("longitude"),
    )

    # --- Coordinate validation ----------------------------------------------
    coord_valid = (
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
        & (F.col("latitude") != 0.0) & (F.col("longitude") != 0.0)  # drops common (0,0) null-island errors
        & F.col("latitude").between(lat_min, lat_max)
        & F.col("longitude").between(lon_min, lon_max)
    )
    before_coord_filter = std.count()
    with_valid_coords = std.filter(coord_valid)
    invalid_coord_count = before_coord_filter - with_valid_coords.count()

    # --- Parse dates (kept nullable; invalid/missing dates do not drop the row) ---
    parsed = with_valid_coords.withColumn(
        "event_ts", F.to_timestamp(F.col("raw_date"), date_format)
    ).withColumn(
        "has_valid_date", F.col("event_ts").isNotNull()
    )

    # --- Missing category -> explicit placeholder, not a dropped row --------
    parsed = parsed.withColumn(
        "category",
        F.when(F.col("category").isNull() | (F.trim(F.col("category")) == ""), F.lit("UNKNOWN"))
         .otherwise(F.upper(F.trim(F.col("category")))),
    )

    # --- Deduplication --------------------------------------------------------
    # Strategy: if a true source ID exists, dedupe on it. Otherwise dedupe on
    # the full (category, date, lat, lon) tuple, which is the closest
    # available proxy for "the same reported incident".
    dedup_keys = ["crime_id"] if has_id else ["category", "raw_date", "latitude", "longitude"]
    before_dedup = parsed.count()
    cleaned = parsed.dropDuplicates(dedup_keys)
    duplicate_count = before_dedup - cleaned.count()

    cleaned_count = cleaned.count()
    missing_date_count = cleaned.filter(~F.col("has_valid_date")).count()

    stats = {
        "input_count": input_count,
        "invalid_coord_removed": invalid_coord_count,
        "duplicate_removed": duplicate_count,
        "cleaned_count": cleaned_count,
        "total_removed": input_count - cleaned_count,
        "missing_date_count": missing_date_count,
        "dedup_strategy": f"dropDuplicates on {dedup_keys}" + (" (source ID)" if has_id else " (no source ID; composite key used)"),
    }

    # --- Coverage summary (for FR-1/FR-2 reporting) -----------------------
    coverage = cleaned.select(
        F.min("latitude").alias("lat_min"), F.max("latitude").alias("lat_max"),
        F.min("longitude").alias("lon_min"), F.max("longitude").alias("lon_max"),
        F.min("event_ts").alias("date_min"), F.max("event_ts").alias("date_max"),
    ).collect()[0]
    stats["geo_coverage"] = (coverage["lat_min"], coverage["lat_max"], coverage["lon_min"], coverage["lon_max"])
    stats["date_coverage"] = (coverage["date_min"], coverage["date_max"])

    print("=" * 60)
    print("FR-2: DATA CLEANING")
    print("=" * 60)
    print(f"Input records:              {stats['input_count']}")
    print(f"Removed (invalid coords):   {stats['invalid_coord_removed']}")
    print(f"Removed (duplicates):       {stats['duplicate_removed']}  [{stats['dedup_strategy']}]")
    print(f"Cleaned records:            {stats['cleaned_count']}")
    print(f"  of which missing date:    {stats['missing_date_count']} (kept for spatial analysis, excluded from temporal)")
    print(f"Geo coverage (lat,lon):     {stats['geo_coverage']}")
    print(f"Date coverage:              {stats['date_coverage']}")
    print()

    return cleaned, stats
