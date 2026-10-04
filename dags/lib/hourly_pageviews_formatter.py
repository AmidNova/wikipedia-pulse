"""
hourly_pageviews_formatter.py

Convertit les pageviews horaires brutes (TSV gz) en parquet normalisé — via Spark.

Lecture  : datalake/raw/wikimedia_dumps/PageviewsHourly/{YYYYMMDD}/pageviews_hourly.tsv.gz
Écriture : datalake/formatted/wikimedia_dumps/PageviewsHourly/{YYYYMMDD}/pageviews_hourly.snappy.parquet
"""

import os
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lib.common import target_date
from lib.spark_session import get_spark

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))
RAW_SCHEMA = "hour_utc STRING, project STRING, title STRING, views LONG"


def normalize_hourly(df: DataFrame) -> DataFrame:
    return df.select(
        F.to_timestamp("hour_utc", "yyyy-MM-dd HH:mm:ss").alias("hour_utc"),
        "project",
        F.regexp_replace("title", "_", " ").alias("title"),
        "views",
    )


def raw_to_formatted_hourly_pageviews(**kwargs):
    date = target_date(kwargs)
    date_str = date.strftime("%Y%m%d")
    print(f"=== raw_to_formatted_hourly_pageviews (Spark) | {date:%Y-%m-%d} ===")

    input_file = DATALAKE_ROOT / "raw" / "wikimedia_dumps" / "PageviewsHourly" / date_str / "pageviews_hourly.tsv.gz"
    if not input_file.exists():
        raise FileNotFoundError(f"Pageviews horaires brutes introuvables : {input_file}")
    output_file = str(
        DATALAKE_ROOT / "formatted" / "wikimedia_dumps" / "PageviewsHourly" / date_str
        / "pageviews_hourly.snappy.parquet"
    )

    spark = get_spark("WikipediaPulse-HourlyPageviewsFormatter")
    try:
        raw = spark.read.csv(str(input_file), sep="\t", schema=RAW_SCHEMA)
        df = normalize_hourly(raw)
        df.write.mode("overwrite").parquet(output_file)
        print(f"  → {df.count()} lignes horaires saved to {output_file}")
    finally:
        spark.stop()
    print("=== raw_to_formatted_hourly_pageviews done ===")
