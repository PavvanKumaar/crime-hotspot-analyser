"""
Central configuration for the Crime Hotspot Analyzer.

Edit this file to point at a different dataset or change analysis
parameters. Nothing else in the project should need column-name
or path changes once you've updated this file.
"""

# ---------------------------------------------------------------------------
# Input / output paths
# ---------------------------------------------------------------------------
INPUT_CSV_PATH = "data/crime_data.csv"
OUTPUT_DIR = "outputs"
CHARTS_DIR = "outputs/charts"

# ---------------------------------------------------------------------------
# Column mapping
# The dataset's real column names go on the right. Defaults below match the
# City of Chicago "Crimes - 2001 to present" dataset
# (https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2).
# Change these if you use a different source (e.g. LA or SF open data).
# ---------------------------------------------------------------------------
COLUMNS = {
    "id": "ID",
    "category": "Primary Type",
    "date": "Date",
    "latitude": "Latitude",
    "longitude": "Longitude",
}

# strptime-style format matching the dataset's date column.
# Chicago's format looks like: 01/23/2023 11:40:00 PM
DATE_FORMAT = "MM/dd/yyyy hh:mm:ss a"

# ---------------------------------------------------------------------------
# Spatial grid
# ---------------------------------------------------------------------------
# Degree-based grid cell size. ~0.01 degrees of latitude is roughly 1.1 km;
# longitude distance varies with latitude, so this is a simplified,
# explicitly-labeled approximation suitable for a city-level view, not a
# true equal-area grid. See README for the limitation.
GRID_CELL_SIZE_DEGREES = 0.01

# ---------------------------------------------------------------------------
# Hotspot ranking
# ---------------------------------------------------------------------------
TOP_N_HOTSPOTS = 10

# ---------------------------------------------------------------------------
# Day / night split (24h clock). Hours in [DAY_START, NIGHT_START) = "day".
# ---------------------------------------------------------------------------
DAY_START_HOUR = 6
NIGHT_START_HOUR = 18

# ---------------------------------------------------------------------------
# Coordinate validity bounds (standard lat/lon ranges; kept here so they're
# easy to tighten to a specific city's bounding box if you want to drop
# obviously mis-geocoded points, e.g. (0, 0)).
# ---------------------------------------------------------------------------
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0
