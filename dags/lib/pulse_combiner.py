"""
pulse_combiner.py

Combine éditions et pageviews via Spark pour détecter les articles trending.

Lecture :
    formatted/wikimedia_stream/Edits/{YYYYMMDD}/edits.snappy.parquet
    formatted/wikimedia_analytics/Pageviews/{YYYYMMDD}/pageviews_*.snappy.parquet

Écriture :
    usage/wikipediaPulse/TrendingArticles/{YYYYMMDD}/trending.snappy.parquet
    usage/wikipediaPulse/EditLeadLag/{YYYYMMDD}/leadlag.snappy.parquet
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))
SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/spark")

# Ajoute PySpark au path
sys.path.insert(0, f"{SPARK_HOME}/python")
sys.path.insert(0, f"{SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark():
    return (
        SparkSession.builder
        .appName("WikipediaPulse-Combination")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def combine(date: datetime) -> None:
    date_str = date.strftime("%Y%m%d")

    edits_path     = str(DATALAKE_ROOT / "formatted" / "wikimedia_stream" / "Edits" / date_str)
    pageviews_path = str(DATALAKE_ROOT / "formatted" / "wikimedia_analytics" / "Pageviews" / date_str)
    trending_dir   = DATALAKE_ROOT / "usage" / "wikipediaPulse" / "TrendingArticles" / date_str
    leadlag_dir    = DATALAKE_ROOT / "usage" / "wikipediaPulse" / "EditLeadLag" / date_str

    trending_dir.mkdir(parents=True, exist_ok=True)
    leadlag_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Reading edits from {edits_path}...")
    df_edits = spark.read.parquet(edits_path)
    print(f"  → {df_edits.count()} edits")

    print(f"Reading pageviews from {pageviews_path}...")
    df_pageviews = spark.read.parquet(pageviews_path)
    print(f"  → {df_pageviews.count()} pageview entries")

    # ── Agrégation des éditions par article ───────────────────────────────────
    df_edit_agg = (
        df_edits
        .groupBy("title", "project")
        .agg(
            F.count("*").alias("edit_count"),
            F.sum(F.abs(F.col("edit_size"))).alias("total_edit_size"),
            F.countDistinct("user").alias("unique_editors"),
            F.min("timestamp_utc").alias("first_edit"),
            F.max("timestamp_utc").alias("last_edit"),
        )
    )

    # ── Normalisation du titre pageviews pour matcher avec edits ─────────────
    # Pageviews : "2026_United_States" → "2026 United States"
    # Edits : "2026 United States" (déjà normalisé dans formatter)
    df_pageviews_norm = df_pageviews.withColumn(
        "title_norm", F.regexp_replace(F.col("article"), "_", " ")
    )

    # ── Join éditions × pageviews ─────────────────────────────────────────────
    df_joined = df_edit_agg.join(
        df_pageviews_norm,
        (df_edit_agg["title"] == df_pageviews_norm["title_norm"]),
        how="left"
    ).select(
        df_edit_agg["title"],
        df_edit_agg["project"].alias("edit_project"),
        df_pageviews_norm["project"].alias("pageview_project"),
        "edit_count",
        "total_edit_size",
        "unique_editors",
        "first_edit",
        "last_edit",
        F.coalesce(F.col("views"), F.lit(0)).alias("pageviews"),
        F.coalesce(F.col("rank"), F.lit(9999)).alias("pageview_rank"),
    )

    # ── Score trending = edit_count * unique_editors / log(pageviews + 1) ─────
    # Un article avec beaucoup d'édits mais peu de vues = signal fort
    df_trending = df_joined.withColumn(
        "trending_score",
        F.round(
            F.col("edit_count") * F.col("unique_editors") /
            F.log(F.col("pageviews") + 2),
            4
        )
    ).orderBy(F.col("trending_score").desc())

    # ── Lead-Lag : articles avec édits ET pageviews ───────────────────────────
    df_leadlag = df_joined.filter(
        (F.col("edit_count") > 1) & (F.col("pageviews") > 0)
    ).withColumn(
        "edit_to_view_ratio",
        F.round(F.col("edit_count") / F.log(F.col("pageviews") + 2), 4)
    ).orderBy(F.col("edit_to_view_ratio").desc())

    # ── Sauvegarde ────────────────────────────────────────────────────────────
    trending_out = str(trending_dir / "trending.snappy.parquet")
    leadlag_out  = str(leadlag_dir / "leadlag.snappy.parquet")

    df_trending.write.mode("overwrite").parquet(trending_out)
    df_leadlag.write.mode("overwrite").parquet(leadlag_out)

    print(f"  → Trending articles saved to {trending_out}")
    print(f"  → Lead-lag saved to {leadlag_out}")

    # ── Preview top 10 trending ───────────────────────────────────────────────
    print("\nTop 10 trending articles :")
    df_trending.select("title", "edit_count", "unique_editors", "pageviews", "trending_score").show(10, truncate=50)

    spark.stop()


def produce_pulse(**kwargs):
    execution_date = kwargs["dag_run"].execution_date
    target_date = execution_date.replace(tzinfo=timezone.utc)

    print(f"=== produce_pulse | {target_date.strftime('%Y-%m-%d')} ===")
    combine(target_date)
    print("=== produce_pulse done ===")
