"""
FR-3: Spatial Grid Generation

Divides the study area into rectangular grid cells using plain degree-based
binning, anchored at the (0, 0) origin so cell indices are reproducible
across runs and datasets (not dependent on this dataset's min/max bounds).

Limitation (documented per PRD Section 8/FR-3): degrees of latitude and
longitude are not uniform distances. A cell is roughly square only near the
equator; at higher latitudes the same degree-width cell covers less
east-west ground distance than north-south. This is an accepted
simplification for a city-level MVP, not a metric/projected grid.

`compute_cell` is pure Python (no Spark) so it can be unit-tested directly
against hand-calculated coordinates (see tests/test_spatial_grid.py), and
the Spark path below uses the identical arithmetic via native column
expressions (no UDF, so it stays a distributed operation).
"""

import math

# PySpark is imported lazily inside assign_grid_cells() so that compute_cell()
# -- and the pure-Python unit tests that exercise it -- can run without a
# PySpark installation or a running Spark session.


def compute_cell(lat: float, lon: float, cell_size: float) -> tuple[int, int, float, float]:
    """Return (cell_x, cell_y, cell_center_lat, cell_center_lon) for one point."""
    cell_x = math.floor(lon / cell_size)
    cell_y = math.floor(lat / cell_size)
    center_lat = (cell_y + 0.5) * cell_size
    center_lon = (cell_x + 0.5) * cell_size
    return cell_x, cell_y, center_lat, center_lon


def assign_grid_cells(df, cell_size: float):
    """Add cell_x, cell_y, cell_center_lat, cell_center_lon to every row.

    Uses Spark's native floor() over the latitude/longitude columns, so this
    remains a distributed transformation rather than a row-by-row UDF.
    """
    from pyspark.sql import functions as F

    with_cells = (
        df.withColumn("cell_x", F.floor(F.col("longitude") / F.lit(cell_size)).cast("int"))
          .withColumn("cell_y", F.floor(F.col("latitude") / F.lit(cell_size)).cast("int"))
          .withColumn("cell_center_lat", (F.col("cell_y") + F.lit(0.5)) * F.lit(cell_size))
          .withColumn("cell_center_lon", (F.col("cell_x") + F.lit(0.5)) * F.lit(cell_size))
    )

    num_occupied_cells = with_cells.select("cell_x", "cell_y").distinct().count()
    print("=" * 60)
    print("FR-3: SPATIAL GRID GENERATION")
    print("=" * 60)
    print(f"Grid cell size:              {cell_size} degrees (~{cell_size * 111:.2f} km at the equator)")
    print(f"Occupied grid cells:         {num_occupied_cells}")
    print("Method: degree-based binning, origin (0,0). Not a metric-equal-area grid.")
    print()

    return with_cells
