"""
pulse_combiner.py

Combine éditions et pageviews via Spark, puis détecte les événements émergents
via Machine Learning (Isolation Forest).
"""

import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lib.common import target_date
from lib.spark_session import get_spark

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))


def join_edits_pageviews(df_edit_agg: DataFrame, df_pageviews: DataFrame) -> DataFrame:
    """Left join éditions × pageviews sur (titre, langue).

    Les éditions portent "fr.wikipedia.org", les pageviews "fr.wikipedia" :
    on aligne avant de joindre pour ne jamais associer deux langues.
    """
    edits = df_edit_agg.withColumn("join_project", F.regexp_replace("project", r"\.org$", ""))
    views = df_pageviews.select(
        F.regexp_replace("article", "_", " ").alias("pv_title"),
        F.col("project").alias("pageview_project"),
        "views",
        "rank",
    )
    joined = edits.join(
        views,
        (edits["title"] == views["pv_title"]) & (edits["join_project"] == views["pageview_project"]),
        how="left",
    )
    passthrough = [c for c in df_edit_agg.columns if c not in ("title", "project")]
    return joined.select(
        "title",
        F.col("project").alias("edit_project"),
        "pageview_project",
        *passthrough,
        F.coalesce(F.col("views"), F.lit(0)).alias("pageviews"),
        F.coalesce(F.col("rank"), F.lit(9999)).alias("pageview_rank"),
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
    date_str = date.strftime("%Y%m%d")

    edits_path     = str(DATALAKE_ROOT / "formatted" / "wikimedia_stream" / "Edits" / date_str / "edits.snappy.parquet")
    pageviews_path = str(DATALAKE_ROOT / "formatted" / "wikimedia_analytics" / "Pageviews" / date_str / "pageviews_*.snappy.parquet")
    trending_dir   = DATALAKE_ROOT / "usage" / "wikipediaPulse" / "TrendingArticles" / date_str
    leadlag_dir    = DATALAKE_ROOT / "usage" / "wikipediaPulse" / "EditLeadLag" / date_str

    trending_dir.mkdir(parents=True, exist_ok=True)
    leadlag_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark("WikipediaPulse-Combination")

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

    # ─── Signature linguistique ───────────────────────────────────────────────
    # Quelle langue édite un article en PREMIER ? Combien de langues en parlent ?
    from pyspark.sql.window import Window

    w_first = Window.partitionBy("title").orderBy("first_edit")
    df_first_lang = (
        df_edit_agg
        .withColumn("rn", F.row_number().over(w_first))
        .filter(F.col("rn") == 1)
        .select("title", F.col("project").alias("first_language"))
    )

    df_lang_count = (
        df_edit_agg
        .groupBy("title")
        .agg(
            F.countDistinct("project").alias("language_count"),
            F.concat_ws(",", F.collect_list("project")).alias("languages_editing")
        )
        .join(df_first_lang, "title", how="left")
    )

    # Join éditions × pageviews (même langue uniquement)
    df_joined = join_edits_pageviews(
        df_edit_agg.select(
            "title", "project", "edit_count", "total_edit_size",
            "unique_editors", "edit_velocity", "first_edit", "last_edit",
        ),
        df_pageviews,
    ).join(df_lang_count, "title", how="left")

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
    date = target_date(kwargs)
    print(f"=== produce_pulse | {date.strftime('%Y-%m-%d')} ===")
    combine(date)
    print("=== produce_pulse done ===")
