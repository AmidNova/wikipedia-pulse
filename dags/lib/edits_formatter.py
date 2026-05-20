"""
edits_formatter.py

Convertit les éditions brutes NDJSON en parquet normalisé — via Spark.

Lecture  : datalake/raw/wikimedia_stream/Edits/{YYYYMMDD}/edits.ndjson
Écriture : datalake/formatted/wikimedia_stream/Edits/{YYYYMMDD}/edits.snappy.parquet
"""

import os
from datetime import datetime, timezone
from pathlib import Path

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import pyspark
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))


def get_spark():
    return (
        SparkSession.builder
        .appName("WikipediaPulse-EditsFormatter")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def convert_edits(date: datetime, spark) -> Path:
    date_str = date.strftime("%Y%m%d")

    input_file = str(
        DATALAKE_ROOT / "raw" / "wikimedia_stream" / "Edits" / date_str / "edits.ndjson"
    )
    output_dir = DATALAKE_ROOT / "formatted" / "wikimedia_stream" / "Edits" / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = str(output_dir / "edits.snappy.parquet")

    print(f"Reading {input_file}...")

    # Spark lit le NDJSON nativement (un JSON par ligne)
    df = spark.read.json(input_file)

    # Normalisation avec Spark
    df = (
        df
        .withColumn("timestamp_utc", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))
        .withColumn("edit_size", F.col("length_new") - F.col("length_old"))
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("comment", F.coalesce(F.col("comment"), F.lit("")))
        .select(
            "timestamp_utc", "project", "title", "user",
            "type", "minor", "edit_size", "length_old", "length_new", "comment"
        )
    )

    df.write.mode("overwrite").parquet(output_file)
    print(f"  → {df.count()} edits saved to {output_file}")
    return Path(output_file)


def raw_to_formatted_edits(**kwargs):
    execution_date = kwargs["dag_run"].execution_date
    target_date = execution_date.replace(tzinfo=timezone.utc)

    print(f"=== raw_to_formatted_edits (Spark) | {target_date.strftime('%Y-%m-%d')} ===")

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        convert_edits(target_date, spark)
    except Exception as e:
        print(f"  ✗ Erreur : {e}")

    spark.stop()
    print("=== raw_to_formatted_edits done ===")
