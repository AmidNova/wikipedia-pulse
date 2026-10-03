"""
pageviews_formatter.py

Convertit les pageviews brutes JSON en parquet normalisé — via Spark.

Lecture  : datalake/raw/wikimedia_analytics/Pageviews/{YYYYMMDD}/pageviews_{project}.json
Écriture : datalake/formatted/wikimedia_analytics/Pageviews/{YYYYMMDD}/pageviews_{project}.snappy.parquet
"""

import json
import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from lib.common import PROJECTS, target_date
from lib.spark_session import get_spark

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))
# Mêmes langues que le fetcher, au format des noms de fichiers (en_wikipedia)
FORMATTER_PROJECTS = [p.replace(".", "_") for p in PROJECTS]


def convert_pageviews(project: str, date: datetime, spark) -> Path:
    date_str = date.strftime("%Y%m%d")

    input_file = (
        DATALAKE_ROOT / "raw" / "wikimedia_analytics" / "Pageviews"
        / date_str / f"pageviews_{project}.json"
    )
    output_dir = (
        DATALAKE_ROOT / "formatted" / "wikimedia_analytics" / "Pageviews" / date_str
    )
    output_file = str(output_dir / f"pageviews_{project}.snappy.parquet")

    print(f"Reading {input_file}...")

    with open(input_file, "r", encoding="utf-8") as f:
        raw = json.load(f)

    articles = raw["items"][0]["articles"]

    schema = StructType([
        StructField("article", StringType(), True),
        StructField("views", LongType(), True),
        StructField("rank", LongType(), True),
    ])

    df = spark.createDataFrame(articles, schema=schema)

    df = (
        df
        .withColumn("project", F.lit(project.replace("_", ".")))
        .withColumn("date_utc", F.to_timestamp(F.lit(date.strftime("%Y-%m-%d"))))
        .withColumn("article", F.regexp_replace(F.col("article"), "_", " "))
        .select("date_utc", "project", "rank", "article", "views")
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(output_file)
    print(f"  → {df.count()} articles saved to {output_file}")
    return Path(output_file)


def raw_to_formatted_pageviews(**kwargs):
    date = target_date(kwargs)
    print(f"=== raw_to_formatted_pageviews (Spark) | {date.strftime('%Y-%m-%d')} ===")

    spark = get_spark("WikipediaPulse-PageviewsFormatter")
    failures = []
    try:
        for project in FORMATTER_PROJECTS:
            try:
                convert_pageviews(project, date, spark)
            except Exception as e:
                print(f"  ✗ Erreur pour {project}: {e}")
                failures.append(project)
    finally:
        spark.stop()

    if failures:
        raise RuntimeError(f"Formatage pageviews échoué pour : {', '.join(failures)}")
    print("=== raw_to_formatted_pageviews done ===")
