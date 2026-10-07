"""
edits_formatter.py

Convertit les éditions brutes NDJSON en parquet normalisé — via Spark.

Lecture  : datalake/raw/wikimedia_stream/Edits/{YYYYMMDD}/edits.ndjson
Écriture : datalake/formatted/wikimedia_stream/Edits/{YYYYMMDD}/edits.snappy.parquet
"""

import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lib.common import target_date
from lib.quality import check_edits, edits_metrics
from lib.spark_session import get_spark

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))

# Le consumer Kafka est at-least-once et les fichiers raw sont en append → on
# dédoublonne ici. Clé : rev_id (unique) ; à défaut (données d'avant rev_id),
# clé naturelle de l'édition.
NATURAL_KEY = ["timestamp", "project", "title", "user", "length_new"]


def normalize_edits(df: DataFrame) -> DataFrame:
    if "rev_id" not in df.columns:
        df = df.withColumn("rev_id", F.lit(None).cast("long"))
    dedup_key = (
        F.when(F.col("rev_id").isNotNull(), F.concat_ws("|", "project", "rev_id"))
        .otherwise(F.concat_ws("|", *NATURAL_KEY))
    )
    return (
        df
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("dedup_key", dedup_key)
        .dropDuplicates(["dedup_key"])
        .withColumn("timestamp_utc", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))
        .withColumn("edit_size", F.col("length_new") - F.col("length_old"))
        .withColumn("comment", F.coalesce(F.col("comment"), F.lit("")))
        .select(
            "timestamp_utc", "project", "rev_id", "title", "user",
            "type", "minor", "edit_size", "length_old", "length_new", "comment"
        )
    )


def convert_edits(date: datetime, spark) -> Path:
    date_str = date.strftime("%Y%m%d")

    input_file = DATALAKE_ROOT / "raw" / "wikimedia_stream" / "Edits" / date_str / "edits.ndjson"
    if not input_file.exists():
        raise FileNotFoundError(f"Éditions brutes introuvables : {input_file}")

    output_dir = DATALAKE_ROOT / "formatted" / "wikimedia_stream" / "Edits" / date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = str(output_dir / "edits.snappy.parquet")

    print(f"Reading {input_file}...")
    df = normalize_edits(spark.read.json(str(input_file))).cache()

    metrics = edits_metrics(df, date)
    print(f"  → {metrics['total']} edits, {metrics['in_day_share']:.2%} datées du jour, {metrics['per_project']}")
    check_edits(metrics)

    df.write.mode("overwrite").parquet(output_file)
    print(f"  → saved to {output_file}")
    return Path(output_file)


def raw_to_formatted_edits(**kwargs):
    date = target_date(kwargs)
    print(f"=== raw_to_formatted_edits (Spark) | {date.strftime('%Y-%m-%d')} ===")

    spark = get_spark("WikipediaPulse-EditsFormatter")
    try:
        convert_edits(date, spark)
    finally:
        spark.stop()
    print("=== raw_to_formatted_edits done ===")
