"""
pulse_combiner.py

Combine éditions, pageviews et entités Wikidata du jour D via Spark, puis détecte
les articles émergents par rapport à leur propre historique (lib/emergence.py).

Lecture :
    formatted/wikimedia_stream/Edits/{D}/                 éditions
    formatted/wikimedia_dumps/PageviewsHourly/{D}/        vues de TOUS les articles édités
    formatted/wikimedia_analytics/Pageviews/{D}/          top 1000 (rang uniquement)
    formatted/wikimedia_api/Wikidata/{D}/                 QID des articles édités

Écriture :
    usage/wikipediaPulse/TrendingArticles/{D}/      un article × langue par ligne
    usage/wikipediaPulse/EditLeadLag/{D}/           ratio éditions / vues (journalier)
    usage/wikipediaPulse/CrossLanguageEvents/{D}/   une entité Wikidata éditée dans ≥ 2 langues
"""

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lib.common import target_date
from lib.emergence import flag_emerging, load_history, observed_days
from lib.hourly_pageviews import TOTAL_TITLE
from lib.quality import check_pulse
from lib.spark_session import get_spark

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))
NO_RANK = 9999  # article hors du top 1000


def daily_views(df_hourly: DataFrame) -> DataFrame:
    """Vues du jour par (project, title) depuis les dumps horaires, totaux de langue exclus."""
    return (
        df_hourly
        .filter(F.col("title") != TOTAL_TITLE)
        .groupBy("project", "title")
        .agg(F.sum("views").alias("views"))
    )


def join_edits_pageviews(df_edit_agg: DataFrame, df_views: DataFrame, df_top: DataFrame) -> DataFrame:
    """Left join éditions × vues du jour × rang du top, sur (titre, langue).

    Les éditions portent "fr.wikipedia.org", les vues "fr.wikipedia" : on aligne
    avant de joindre pour ne jamais associer deux langues.
    """
    edits = df_edit_agg.withColumn("join_project", F.regexp_replace("project", r"\.org$", ""))
    views = df_views.select(
        F.col("title").alias("pv_title"), F.col("project").alias("pv_project"), "views",
    )
    top = (
        df_top.select(
            F.regexp_replace("article", "_", " ").alias("top_title"),
            F.col("project").alias("top_project"),
            "rank",
        )
        .groupBy("top_title", "top_project")
        .agg(F.min("rank").alias("rank"))  # un doublon dans le top dupliquerait l'article
    )
    joined = (
        edits
        .join(views, (edits["title"] == views["pv_title"]) & (edits["join_project"] == views["pv_project"]), "left")
        .join(top, (edits["title"] == top["top_title"]) & (edits["join_project"] == top["top_project"]), "left")
    )
    passthrough = [c for c in df_edit_agg.columns if c not in ("title", "project")]
    return joined.select(
        "title",
        F.col("project").alias("edit_project"),
        F.col("join_project").alias("pageview_project"),
        *passthrough,
        F.coalesce(F.col("views"), F.lit(0)).cast("long").alias("pageviews"),
        F.coalesce(F.col("rank"), F.lit(NO_RANK)).alias("pageview_rank"),
    )


def aggregate_edits(df_edits: DataFrame) -> DataFrame:
    """Une ligne par (title, project) : volume, éditeurs, fenêtre et vélocité (édits/min)."""
    agg = df_edits.groupBy("title", "project").agg(
        F.count("*").alias("edit_count"),
        F.sum(F.abs(F.col("edit_size"))).alias("total_edit_size"),
        F.countDistinct("user").alias("unique_editors"),
        F.min("timestamp_utc").alias("first_edit"),
        F.max("timestamp_utc").alias("last_edit"),
    )
    duration = F.unix_timestamp("last_edit") - F.unix_timestamp("first_edit")
    return agg.withColumn(
        "edit_velocity",
        F.when(duration > 0, F.round(F.col("edit_count") / duration * 60, 4))
        .otherwise(F.col("edit_count").cast("double")),
    )


def add_language_signature(pdf: pd.DataFrame) -> pd.DataFrame:
    """Langues qui éditent la même entité Wikidata (repli : même article, même langue).

    language_count, languages_editing (ordre de 1re édition), first_language, et
    language_lag_minutes : retard de cette langue sur la première à avoir édité.
    """
    out = pdf.copy()
    out["entity_key"] = out["wikidata_id"].fillna(out["edit_project"] + "|" + out["title"])
    ordered = out.sort_values(["entity_key", "first_edit", "edit_project"])
    by_entity = ordered.groupby("entity_key")
    signature = pd.DataFrame({
        "language_count": by_entity["edit_project"].nunique(),
        "languages_editing": by_entity["edit_project"].agg(lambda s: ",".join(dict.fromkeys(s))),
        "first_language": by_entity["edit_project"].first(),
        "entity_first_edit": by_entity["first_edit"].min(),
    })
    out = out.join(signature, on="entity_key")
    out["language_lag_minutes"] = ((out["first_edit"] - out["entity_first_edit"]).dt.total_seconds() / 60).round(1)
    return out.drop(columns=["entity_first_edit"])


def cross_language_events(pdf: pd.DataFrame) -> pd.DataFrame:
    """Une ligne par entité Wikidata éditée dans au moins 2 langues le même jour."""
    multi = pdf[pdf["wikidata_id"].notna() & (pdf["language_count"] >= 2)]
    columns = [
        "wikidata_id", "language_count", "languages_editing", "first_language", "first_edit",
        "spread_minutes", "titles", "total_edits", "sum_editors", "total_pageviews",
        "emerging_languages", "is_emerging",
    ]
    if multi.empty:
        return pd.DataFrame(columns=columns)
    ordered = multi.sort_values(["wikidata_id", "first_edit", "edit_project"])
    ordered = ordered.assign(lang_title=ordered["edit_project"].str.split(".").str[0] + ":" + ordered["title"])
    by_entity = ordered.groupby("wikidata_id")
    events = pd.DataFrame({
        "language_count": by_entity["language_count"].first(),
        "languages_editing": by_entity["languages_editing"].first(),
        "first_language": by_entity["first_language"].first(),
        "first_edit": by_entity["first_edit"].min(),
        "spread_minutes": by_entity["language_lag_minutes"].max(),
        "titles": by_entity["lang_title"].agg(" | ".join),
        "total_edits": by_entity["edit_count"].sum(),
        # somme des éditeurs distincts de chaque langue : un même compte peut compter deux fois
        "sum_editors": by_entity["unique_editors"].sum(),
        "total_pageviews": by_entity["pageviews"].sum(),
        "emerging_languages": by_entity["is_emerging"].sum().astype(int),
    }).reset_index()
    events["is_emerging"] = events["emerging_languages"] > 0
    return events[columns].sort_values(["language_count", "sum_editors"], ascending=False)


def write_pandas_parquet(pdf: pd.DataFrame, folder: Path) -> None:
    """Dossier parquet à un seul part-file (même forme qu'une sortie Spark)."""
    folder.mkdir(parents=True, exist_ok=True)
    for old in folder.glob("part-*.parquet"):
        old.unlink()
    pdf.to_parquet(str(folder / "part-00000.snappy.parquet"), index=False)


WIKIDATA_SCHEMA = "project STRING, title STRING, wikidata_id STRING"


def read_inputs(spark, date_str: str) -> dict:
    """DataFrames d'entrée du jour ; une source requise manquante fait échouer la tâche.

    Wikidata est un enrichissement : sans lui, les langues sont regroupées article par
    article (language_count = 1) et le run reste en échec via la tâche wikidata.
    """
    formatted = DATALAKE_ROOT / "formatted"
    paths = {
        "edits": formatted / "wikimedia_stream" / "Edits" / date_str / "edits.snappy.parquet",
        "hourly": formatted / "wikimedia_dumps" / "PageviewsHourly" / date_str / "pageviews_hourly.snappy.parquet",
        "top": formatted / "wikimedia_analytics" / "Pageviews" / date_str,
        "wikidata": formatted / "wikimedia_api" / "Wikidata" / date_str / "wikibase_items.snappy.parquet",
    }
    wikidata_path = paths.pop("wikidata")
    missing = [str(p) for p in paths.values() if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Entrées manquantes : {missing}")
    inputs = {name: spark.read.parquet(str(path)) for name, path in paths.items() if name != "top"}
    inputs["top"] = spark.read.parquet(str(paths["top"] / "pageviews_*.snappy.parquet"))
    if wikidata_path.exists():
        inputs["wikidata"] = spark.read.parquet(str(wikidata_path))
    else:
        print(f"  ⚠ QID absents ({wikidata_path}) : pas de regroupement multilingue ce jour")
        inputs["wikidata"] = spark.createDataFrame([], WIKIDATA_SCHEMA)
    for name, df in inputs.items():
        print(f"  → {name} : {df.count()} lignes")
    return inputs


def combine(date: datetime) -> None:
    date_str = date.strftime("%Y%m%d")
    usage = DATALAKE_ROOT / "usage" / "wikipediaPulse"

    spark = get_spark("WikipediaPulse-Combination")
    try:
        inputs = read_inputs(spark, date_str)
        df_edit_agg = aggregate_edits(inputs["edits"]).join(
            inputs["wikidata"].select("project", "title", "wikidata_id"), ["project", "title"], "left",
        )
        df_joined = join_edits_pageviews(df_edit_agg, daily_views(inputs["hourly"]), inputs["top"])
        df_trending = df_joined.withColumn(
            "trending_score",
            F.round(F.col("edit_count") * F.col("unique_editors") / F.log(F.col("pageviews") + 2), 4),
        )

        pdf = df_trending.toPandas()
        for col in ("first_edit", "last_edit"):  # nanosecondes pandas → microsecondes parquet/Spark
            pdf[col] = pdf[col].astype("datetime64[us]")
        match_rate = (pdf["pageviews"] > 0).groupby(pdf["edit_project"]).mean()
        print(f"  → part des articles édités avec des vues le jour même : {match_rate.round(3).to_dict()}")

        days = observed_days(date)
        history = load_history(spark, days, inputs["edits"])
        pdf = flag_emerging(pdf.assign(project=pdf["edit_project"]), history, days).drop(columns="project")
        pdf = add_language_signature(pdf).sort_values("trending_score", ascending=False)
        check_pulse(pdf)  # avant toute écriture : des données fausses ne quittent pas la tâche
        events = cross_language_events(pdf)

        df_leadlag = (
            df_joined.filter((F.col("edit_count") > 1) & (F.col("pageviews") > 0))
            .withColumn("edit_to_view_ratio", F.round(F.col("edit_count") / F.log(F.col("pageviews") + 2), 4))
            .orderBy(F.col("edit_to_view_ratio").desc())
        )
        df_leadlag.write.mode("overwrite").parquet(str(usage / "EditLeadLag" / date_str / "leadlag.snappy.parquet"))
    finally:
        spark.stop()

    write_pandas_parquet(pdf, usage / "TrendingArticles" / date_str / "trending.snappy.parquet")
    write_pandas_parquet(events, usage / "CrossLanguageEvents" / date_str / "crosslang.snappy.parquet")
    print(f"  → {len(pdf)} articles trending, {len(events)} événements multilingues")

    print("\nTop 10 émergents :")
    top = pdf[pdf["is_emerging"]].nlargest(10, "z_editors")
    print(top[["title", "edit_project", "unique_editors", "baseline_editors", "z_editors", "pageviews", "language_count"]]
          .to_string(index=False))


def produce_pulse(**kwargs):
    date = target_date(kwargs)
    print(f"=== produce_pulse | {date.strftime('%Y-%m-%d')} ===")
    combine(date)
    print("=== produce_pulse done ===")
