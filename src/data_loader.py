"""
FR-1: Data Loading

Loads the raw crime CSV through Spark and reports basic structural
information about it before any cleaning happens.
"""

import os
from pyspark.sql import DataFrame, SparkSession


def load_crime_data(spark: SparkSession, csv_path: str) -> DataFrame:
    """Load the crime CSV through Spark with a permissive, header-aware read.

    Raises a clear error if the file is missing rather than letting Spark's
    own (less friendly) exception surface.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Dataset not found at '{csv_path}'.\n"
            "Download a crime dataset with latitude/longitude columns "
            "(see README.md) and place it at this path, or update "
            "INPUT_CSV_PATH in config.py."
        )

    df = spark.read.csv(
        csv_path,
        header=True,
        inferSchema=True,
        multiLine=True,
        escape='"',
    )

    total_records = df.count()
    print("=" * 60)
    print("FR-1: DATA LOADING")
    print("=" * 60)
    print(f"Input file:        {csv_path}")
    print(f"Total input rows:  {total_records}")
    print(f"Columns ({len(df.columns)}):")
    for name, dtype in df.dtypes:
        print(f"  - {name}: {dtype}")
    print()

    return df
