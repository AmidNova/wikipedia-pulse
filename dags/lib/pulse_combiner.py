"""
pulse_combiner.py

Combine éditions et pageviews via Spark, puis détecte les événements émergents
via Machine Learning (Isolation Forest).
"""

import os
from datetime import datetime, timedelta, timezone
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
        .appName("WikipediaPulse-Combination")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def detect_emerging(pdf):
    """Détection d'anomalies via Isolation Forest."""
    from sklearn.ensemble import IsolationForest

    features = ["edit_count", "unique_editors", "total_edit_size", "edit_velocity"]

    if len(pdf) < 10:
        print(f"  ⚠ Trop peu d'articles ({len(pdf)}) pour le ML")
        pdf["is_emerging"] = False
        pdf["anomaly_score"] = 0.0
        return pdf

    X = pdf[features].fillna(0)
    model = IsolationForest(contamination=0.1, random_state=42)
    predictions = model.fit_predict(X)
    scores = model.score_samples(X)

    pdf["is_emerging"] = (predictions == -1)
    pdf["anomaly_score"] = scores.round(4)

    n_emerging = int(pdf["is_emerging"].sum())
    print(f"  → {n_emerging} événements émergents détectés par Isolation Forest")
    return pdf


def combine(date: datetime) -> None:
    date_str          = date.strftime("%Y%m%d")
    date_str_previous = (date - timedelta(days=1)).strftime("%Y%m%d")

    edits_path     = str(DATALAKE_ROOT / "formatted" / "wikimedia_stream" / "Edits" / date_str / "edits.snappy.parquet")
    pageviews_path = str(DATALAKE_ROOT / "formatted" / "wikimedia_analytics" / "Pageviews" / date_str_previous / "pageviews_*.snappy.parquet")
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

    # Agrégation des éditions par article
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

    # edit_velocity = éditions par minute
    df_edit_agg = df_edit_agg.withColumn(
        "duration_seconds",
        F.unix_timestamp("last_edit") - F.unix_timestamp("first_edit")
    ).withColumn(
        "edit_velocity",
        F.when(
            F.col("duration_seconds") > 0,
            F.round(F.col("edit_count") / F.col("duration_seconds") * 60, 4)
        ).otherwise(F.col("edit_count").cast("double"))
    )

    # Normalisation du titre pageviews
    df_pageviews_norm = df_pageviews.withColumn(
        "title_norm", F.regexp_replace(F.col("article"), "_", " ")
    )

    # Join éditions × pageviews
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
        "edit_velocity",
        "first_edit",
        "last_edit",
        F.coalesce(F.col("views"), F.lit(0)).alias("pageviews"),
        F.coalesce(F.col("rank"), F.lit(9999)).alias("pageview_rank"),
    )

    # Score trending
    df_trending = df_joined.withColumn(
        "trending_score",
        F.round(
            F.col("edit_count") * F.col("unique_editors") /
            F.log(F.col("pageviews") + 2),
            4
        )
    ).orderBy(F.col("trending_score").desc())

    # ─── ML : Isolation Forest ────────────────────────────────────────────────
    print("Détection d'événements émergents (Isolation Forest)...")
    pdf_trending = df_trending.toPandas()

    # Fix timestamps nanosecondes → microsecondes
    for col in ["first_edit", "last_edit"]:
        if col in pdf_trending.columns:
            pdf_trending[col] = pdf_trending[col].astype("datetime64[us]")

    pdf_trending = detect_emerging(pdf_trending)

    # ─── Lead-Lag ─────────────────────────────────────────────────────────────
    df_leadlag = df_joined.filter(
        (F.col("edit_count") > 1) & (F.col("pageviews") > 0)
    ).withColumn(
        "edit_to_view_ratio",
        F.round(F.col("edit_count") / F.log(F.col("pageviews") + 2), 4)
    ).orderBy(F.col("edit_to_view_ratio").desc())

    # ─── Sauvegarde ───────────────────────────────────────────────────────────
    # Trending : écriture pandas (évite le conflit de types pandas↔Spark)
    trending_out = trending_dir / "trending.snappy.parquet"
    trending_out.mkdir(parents=True, exist_ok=True)
    pdf_trending.to_parquet(str(trending_out / "part-00000.snappy.parquet"), index=False)
    print(f"  → Trending saved ({len(pdf_trending)} articles)")

    # Lead-lag : écriture Spark
    leadlag_out = str(leadlag_dir / "leadlag.snappy.parquet")
    df_leadlag.write.mode("overwrite").parquet(leadlag_out)
    print(f"  → Lead-lag saved")

    # Affichage top 10
    print("\nTop 10 trending (avec détection émergence) :")
    top10 = pdf_trending.nlargest(10, "trending_score")[
        ["title", "edit_count", "unique_editors", "edit_velocity", "trending_score", "is_emerging"]
    ]
    print(top10.to_string(index=False))

    spark.stop()


def produce_pulse(**kwargs):
    execution_date = kwargs["dag_run"].execution_date
    target_date = execution_date.replace(tzinfo=timezone.utc)
    print(f"=== produce_pulse | {target_date.strftime('%Y-%m-%d')} ===")
    combine(target_date)
    print("=== produce_pulse done ===")
