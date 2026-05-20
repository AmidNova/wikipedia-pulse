"""
pageviews_formatter.py

Convertit les pageviews brutes JSON en parquet normalisé — via Spark.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import pyspark
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))
PROJECTS = ["en_wikipedia", "fr_wikipedia"]


def get_spark():
    return (
        SparkSession.builder
        .appName("WikipediaPulse-PageviewsFormatter")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def convert_pageviews(project: str, date: datetime, spark) -> Path:
    date_str = date.strftime("%Y%m%d")

    input_file = (
        DATALAKE_ROOT / "raw" / "wikimedia_analytics" / "Pageviews"
        / date_str / f"pageviews_{project}.json"
    )
    output_dir = (
        DATALAKE_ROOT / "formatted" / "wikimedia_analytics" / "Pageviews" / date_str
    )
    output_dir.mkdir(parents=True, exist_ok=True)
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

    df.write.mode("overwrite").parquet(output_file)
    print(f"  → {df.count()} articles saved to {output_file}")
    return Path(output_file)


def raw_to_formatted_pageviews(**kwargs):
    execution_date = kwargs["dag_run"].execution_date
    target_date = (execution_date - timedelta(days=1)).replace(tzinfo=timezone.utc)

    print(f"=== raw_to_formatted_pageviews (Spark) | {target_date.strftime('%Y-%m-%d')} ===")

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    for project in PROJECTS:
        try:
            convert_pageviews(project, target_date, spark)
        except Exception as e:
            print(f"  ✗ Erreur pour {project}: {e}")

    spark.stop()
    print("=== raw_to_formatted_pageviews done ===")
