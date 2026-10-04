"""
leadlag.py

Vrai lead-lag édition → lecture, à l'heure près.

Pour chaque article édité le jour d'événement E (au moins MIN_EVENT_EDITS fois),
on construit deux séries horaires sur 48 h (E 00:00 → E+2 00:00 UTC) :
    e(t) = nombre d'éditions,  v(t) = nombre de vues désaisonnalisées
puis on calcule la corrélation croisée corr(e(t), v(t+k)) pour k ∈ [-12h, +24h].

Garde-fous statistiques :
  - désaisonnalisation : v(t) est divisé par le profil horaire de toute la langue
    (lignes "-" des dumps), sinon le cycle jour/nuit crée de faux lead-lags
  - significativité : p-value unilatérale de la meilleure corrélation, corrigée
    par Bonferroni sur les 37 décalages testés ; au-delà de ALPHA → decorrelated

    best_lag_hours > 0 : les lectures suivent les éditions (edit_led)
    best_lag_hours < 0 : les éditions suivent les lectures (view_led)

Le run du jour D traite E = D-1 : il faut les vues de D pour suivre les
éditions de fin de journée.

Lecture  : formatted/wikimedia_stream/Edits/{E, D}/
           formatted/wikimedia_dumps/PageviewsHourly/{E, D}/
Écriture : usage/wikipediaPulse/EditLeadLagHourly/{E}/leadlag_hourly.snappy.parquet
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from scipy import stats

from lib.common import target_date
from lib.hourly_pageviews import TOTAL_TITLE
from lib.spark_session import get_spark

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))

WINDOW_HOURS = 48
MAX_LEAD_HOURS = 12      # vues jusqu'à 12 h AVANT les éditions
MAX_LAG_HOURS = 24       # vues jusqu'à 24 h APRÈS les éditions
EVENT_HOURS = 24         # le jour d'événement = 24 premières heures de la fenêtre
MIN_EVENT_EDITS = 5      # en dessous, la série d'éditions est trop pauvre
MIN_CORR = 0.3           # en dessous, pas de lien exploitable
MIN_OVERLAP = 6          # points communs minimum pour une corrélation
ALPHA = 0.05             # seuil de significativité (après Bonferroni)

OUTPUT_SCHEMA = (
    "project STRING, title STRING, event_edits LONG, total_views LONG, "
    "best_lag_hours INT, best_corr DOUBLE, p_value DOUBLE, peak_lag_hours INT, "
    "edit_peak_utc TIMESTAMP, view_peak_utc TIMESTAMP, view_surge_ratio DOUBLE, pattern STRING"
)


def _corr_at(edits: np.ndarray, views: np.ndarray, k: int) -> float:
    """corr(e(t), v(t+k)) sur la partie commune des deux séries."""
    n = len(edits)
    e, v = (edits[: n - k], views[k:]) if k >= 0 else (edits[-k:], views[: n + k])
    if len(e) < MIN_OVERLAP or e.std() == 0 or v.std() == 0:
        return float("nan")
    return float(np.corrcoef(e, v)[0, 1])


def _overlap(n: int, k: int) -> int:
    return n - abs(k)


def bonferroni_p_value(r: float, n: int, n_tests: int) -> float:
    """P(corrélation ≥ r par hasard) pour n points, corrigée pour n_tests décalages."""
    if np.isnan(r) or n <= 2:
        return float("nan")
    if r >= 1.0:
        return 0.0
    t = r * np.sqrt((n - 2) / (1 - r * r))
    return float(min(1.0, stats.t.sf(t, n - 2) * n_tests))


def classify(best_lag, best_corr, p_value) -> str:
    if best_lag is None or np.isnan(best_corr) or best_corr < MIN_CORR:
        return "decorrelated"
    if np.isnan(p_value) or p_value > ALPHA:
        return "decorrelated"
    if best_lag > 0:
        return "edit_led"
    if best_lag < 0:
        return "view_led"
    return "simultaneous"


def lead_lag_metrics(edits: np.ndarray, views: np.ndarray) -> dict:
    lags = list(range(-MAX_LEAD_HOURS, MAX_LAG_HOURS + 1))
    corrs = np.array([_corr_at(edits, views, k) for k in lags])

    if np.all(np.isnan(corrs)):
        best_lag, best_corr, p_value = None, float("nan"), float("nan")
    else:
        best = int(np.nanargmax(corrs))
        best_lag, best_corr = lags[best], round(float(corrs[best]), 4)
        p_value = bonferroni_p_value(float(corrs[best]), _overlap(len(edits), best_lag), len(lags))

    median_views = float(np.median(views))
    has_views = views.max() > 0
    return {
        "best_lag_hours": best_lag,
        "best_corr": best_corr,
        "p_value": p_value,
        "edit_peak_hour": int(np.argmax(edits[:EVENT_HOURS])),
        "view_peak_hour": int(np.argmax(views)) if has_views else None,
        "peak_lag_hours": int(np.argmax(views) - np.argmax(edits[:EVENT_HOURS])) if has_views else None,
        "view_surge_ratio": round(float(views.max()) / max(median_views, 1.0), 4),
        "pattern": classify(best_lag, best_corr, p_value),
    }


def _article_metrics(window_start: datetime, pdf: pd.DataFrame) -> pd.DataFrame:
    """applyInPandas : une ligne de métriques par article (séries complétées à 0)."""
    hours = pd.date_range(window_start, periods=WINDOW_HOURS, freq="h")
    series = (
        pdf.groupby("hour_utc")[["edits", "views", "views_adj"]].sum()
        .reindex(hours, fill_value=0)
    )
    edits = series["edits"].to_numpy(dtype=float)
    metrics = lead_lag_metrics(edits, series["views_adj"].to_numpy(dtype=float))
    edit_peak, view_peak = metrics.pop("edit_peak_hour"), metrics.pop("view_peak_hour")

    row = {
        "project": pdf["project"].iloc[0],
        "title": pdf["title"].iloc[0],
        "event_edits": int(pdf["event_edits"].iloc[0]),
        "total_views": int(series["views"].sum()),
        **metrics,
        "edit_peak_utc": hours[edit_peak],
        "view_peak_utc": hours[view_peak] if view_peak is not None else pd.NaT,
    }
    out = pd.DataFrame([row])
    for col in ("best_lag_hours", "peak_lag_hours"):
        out[col] = out[col].astype("Int32")
    # pandas 3 crée des chaînes Arrow "large_string", refusées par applyInPandas (Spark 4.1)
    for col in ("project", "title", "pattern"):
        out[col] = out[col].astype(object)
    return out[[c.split(" ")[0] for c in OUTPUT_SCHEMA.split(", ")]]


def build_leadlag(edits: DataFrame, views: DataFrame, event_day: datetime) -> DataFrame:
    """edits : (project 'xx.wikipedia.org', title, timestamp_utc) ;
    views : (hour_utc, project 'xx.wikipedia', title, views)."""
    window_start = event_day
    window_end = event_day + timedelta(hours=WINDOW_HOURS)
    event_end = event_day + timedelta(days=1)
    in_window = (F.col("hour_utc") >= F.lit(window_start)) & (F.col("hour_utc") < F.lit(window_end))

    edits_hourly = (
        edits
        .withColumn("project", F.regexp_replace("project", r"\.org$", ""))
        .withColumn("hour_utc", F.date_trunc("hour", "timestamp_utc"))
        .filter(in_window)
        .groupBy("project", "title", "hour_utc")
        .agg(F.count("*").alias("edits"))
    )
    active = (
        edits_hourly
        .filter(F.col("hour_utc") < F.lit(event_end))
        .groupBy("project", "title")
        .agg(F.sum("edits").alias("event_edits"))
        .filter(F.col("event_edits") >= MIN_EVENT_EDITS)
    )
    views_in_window = views.filter(in_window)

    # Profil jour/nuit de chaque langue : total horaire / moyenne sur la fenêtre
    diurnal = (
        views_in_window.filter(F.col("title") == TOTAL_TITLE)
        .groupBy("project", "hour_utc").agg(F.sum("views").alias("lang_views"))
        .withColumn(
            "diurnal_factor",
            F.col("lang_views") / F.avg("lang_views").over(Window.partitionBy("project")),
        )
        .select("project", "hour_utc", "diurnal_factor")
    )
    views_hourly = (
        views_in_window.filter(F.col("title") != TOTAL_TITLE)
        .join(diurnal, ["project", "hour_utc"], "left")
        .withColumn("views_adj", F.col("views") / F.coalesce(F.col("diurnal_factor"), F.lit(1.0)))
        .select("hour_utc", "project", "title", "views", "views_adj")
    )

    series = (
        edits_hourly.join(views_hourly, ["project", "title", "hour_utc"], "full_outer")
        .join(active, ["project", "title"], "inner")
        .fillna(0, subset=["edits", "views", "views_adj"])
    )
    # Spark passe à pandas des timestamps naïfs dans le fuseau de session (UTC)
    naive_start = window_start.replace(tzinfo=None)

    def metrics_for_article(pdf: pd.DataFrame) -> pd.DataFrame:
        return _article_metrics(naive_start, pdf)

    return series.groupBy("project", "title").applyInPandas(metrics_for_article, schema=OUTPUT_SCHEMA)


def _required(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Entrée lead-lag manquante : {path}")
    return str(path)


def produce_leadlag(**kwargs):
    run_day = target_date(kwargs)
    event_day = run_day - timedelta(days=1)
    print(f"=== produce_leadlag | jour d'événement {event_day:%Y-%m-%d} (vues jusqu'au {run_day:%Y-%m-%d}) ===")

    days = [event_day.strftime("%Y%m%d"), run_day.strftime("%Y%m%d")]
    edits_paths = [
        _required(DATALAKE_ROOT / "formatted" / "wikimedia_stream" / "Edits" / d / "edits.snappy.parquet")
        for d in days
    ]
    views_paths = [
        _required(DATALAKE_ROOT / "formatted" / "wikimedia_dumps" / "PageviewsHourly" / d
                  / "pageviews_hourly.snappy.parquet")
        for d in days
    ]
    output = str(
        DATALAKE_ROOT / "usage" / "wikipediaPulse" / "EditLeadLagHourly" / days[0]
        / "leadlag_hourly.snappy.parquet"
    )

    spark = get_spark("WikipediaPulse-LeadLag")
    try:
        edits = spark.read.parquet(*edits_paths).select("project", "title", "timestamp_utc")
        views = spark.read.parquet(*views_paths)
        result = build_leadlag(edits, views, event_day).cache()
        result.write.mode("overwrite").parquet(output)

        print(f"  → {result.count()} articles analysés → {output}")
        result.groupBy("pattern").count().orderBy(F.desc("count")).show(truncate=False)
        print("Top 10 edit_led (les lectures suivent les éditions) :")
        (result.filter(F.col("pattern") == "edit_led")
         .orderBy(F.desc("view_surge_ratio"))
         .select("project", "title", "event_edits", "best_lag_hours", "best_corr", "view_surge_ratio")
         .show(10, truncate=40))
    finally:
        spark.stop()
    print("=== produce_leadlag done ===")
