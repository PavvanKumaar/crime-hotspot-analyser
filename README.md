# Crime Hotspot Detection and Spatial-Temporal Analysis

A Big Data mini-project that uses Apache Spark (PySpark) to process a
historical crime dataset, identify high-count geographical "hotspot" grid
cells, analyze crime patterns by category and time, and visualize the
results on an interactive map and in charts.

**This is descriptive, not predictive.** Results show where crimes have
historically been reported, not where future crime will occur, and ranked
cells are **not** statistically validated clusters (see Limitations).

---

## 1. Requirements

- Python 3.9+
- **Java 8, 11, or 17** (required by PySpark; verify with `java -version`).
  If Java isn't installed:
  - Ubuntu/Debian: `sudo apt install openjdk-17-jdk`
  - macOS: `brew install openjdk@17`
  - Windows: install Eclipse Temurin 17 and set `JAVA_HOME`.

## 2. Setup

```bash
git clone <this repo>  # or just unzip it
cd crime-hotspot-analyzer
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Get a dataset

You need a crime dataset with latitude/longitude, a category field, and a
date field. **Do not invent coordinates** if a dataset lacks them — pick a
different one.

### Recommended: City of Chicago crime data (default config matches this)

1. Go to the official portal:
   https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present/ijzp-q8t2
2. The full dataset is multi-million rows / several GB — too large to
   iterate on quickly. **Filter first:**
   - Use the page's **Filter** tool to restrict to a date range (e.g. a
     single year), which comfortably lands in the tens-of-thousands-of-rows
     range this project targets.
   - Then **Export → CSV**.
   - Or pull a bounded slice directly via the Socrata API, e.g.:
     ```
     https://data.cityofchicago.org/resource/ijzp-q8t2.csv?$limit=50000&$where=date>'2023-01-01'
     ```
3. Save the file as `data/crime_data.csv`.

### Alternatives

- A pre-filtered 2018–2019 Chicago extract (~138 MB):
  https://zenodo.org/records/3902623
- Los Angeles crime data: `data.lacity.org` → "Crime Data from 2020 to Present"
- San Francisco incident reports: `datasf.org` → "Police Department Incident Reports"

If you use a dataset other than Chicago's, **update `config.py`**:
`COLUMNS` (map your dataset's real column names) and `DATE_FORMAT` (a
`to_timestamp`-style pattern matching your date column, e.g.
`"yyyy-MM-dd'T'HH:mm:ss"`).

## 4. Run

```bash
python main.py
```

This prints a stage-by-stage log (load → clean → grid → aggregate →
analyze → visualize → export) ending in an execution summary:

```
EXECUTION SUMMARY
Input record count:          <N>
Valid (cleaned) record count: <N>
Removed record count:        <N>
Grid cells with incidents:   <N>
Hotspot cells exported:      10
Row-count conservation check: PASSED
Elapsed time:                <N>s
Output directory:            .../outputs
```

## 5. Outputs (`outputs/`)

| File | Contents |
|---|---|
| `hotspot_map.html` | Interactive Folium map — open directly in a browser |
| `top_hotspots.csv` | Ranked top-N high-count grid cells |
| `grid_crime_counts.csv` | Incident counts for every occupied grid cell |
| `crime_category_summary.csv` | Total incidents per crime category |
| `hotspot_category_breakdown.csv` | Category mix within the top hotspot cells |
| `monthly_crime_summary.csv` | Incidents per calendar month |
| `hourly_crime_summary.csv` | Incidents per hour of day |
| `day_night_summary.csv` | Day vs. night incident counts |
| `charts/*.png` | Bar/line charts matching the CSVs above |

## 6. Testing

```bash
pytest tests/test_spatial_grid.py -v
```

Verifies grid-cell assignment against hand-calculated coordinates
(including a boundary case for negative longitudes, where naive rounding
would misassign western-hemisphere points by one cell). This runs in pure
Python with no Spark session required.

The pipeline itself also self-checks **row-count conservation** on every
run: the sum of per-cell incident counts must equal the total cleaned
record count. If a join or group-by silently dropped or duplicated rows,
this assertion fails loudly instead of letting bad numbers reach the
report.

## 7. How Spark is actually used here

This isn't "Spark imported but not doing anything." The distributed
operations are:
- CSV ingestion and schema inference
- Coordinate validation and deduplication (`dropDuplicates`)
- Grid-cell assignment via native column expressions (`floor()`, no UDF —
  keeps the transformation distributed rather than row-by-row Python)
- The grid aggregation (`groupBy` + `count`/`min`/`max`/`countDistinct`)
  and the category/temporal aggregations

Running `main.py` prints the physical query plan (`.explain()`) for the
main grid aggregation — look for the `Exchange` (shuffle) stages, which is
the actual distributed computation Spark is introducing.

The cleaned dataframe is `.cache()`d once, since it's read repeatedly by
the grid, category, and temporal aggregations downstream; this trades
memory for avoiding re-running the load/clean/parse chain five separate
times.

## 8. Limitations (documented deliberately, not omissions)

- **Grid cells, not a projected/metric grid.** Cells are fixed-degree
  bins. A degree of longitude covers less real-world distance than a
  degree of latitude outside the equator, so cells aren't perfectly
  square on the ground. Acceptable for a city-scale MVP; a true
  equal-area grid would require projecting coordinates into a local CRS
  (e.g. via Apache Sedona), which is out of scope here.
- **"Hotspots" are high-count cells, not statistically significant
  clusters.** No spatial autocorrelation test (e.g. Getis-Ord Gi*,
  Moran's I) has been applied. A cell ranking highly here could, in
  principle, reflect normal random variation rather than real
  clustering — proper hotspot statistics would need that test, which
  this timeline didn't include.
- **No predictive claim.** This describes historical report
  concentrations only.
- Records with valid coordinates but unparseable/missing dates are kept
  for spatial analysis (FR-2/FR-3/FR-4/FR-5) but excluded from every
  temporal and category-by-time breakdown (FR-6/FR-7).

## 9. Project structure

```
crime-hotspot-analyzer/
├── data/                       # place crime_data.csv here (not included)
├── outputs/                    # generated on each run
│   └── charts/
├── src/
│   ├── data_loader.py          # FR-1
│   ├── data_cleaning.py        # FR-2
│   ├── spatial_grid.py         # FR-3
│   ├── hotspot_analysis.py     # FR-4, FR-5
│   ├── temporal_analysis.py    # FR-6, FR-7
│   └── visualization.py        # FR-8, FR-9
├── tests/
│   └── test_spatial_grid.py
├── config.py                   # all tunables: columns, grid size, paths
├── main.py                     # orchestrates the full pipeline, FR-10
└── requirements.txt
```
