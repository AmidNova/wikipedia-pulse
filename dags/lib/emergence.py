"""
emergence.py

Détection des articles émergents : chaque article est comparé à SON propre
historique, pas aux autres articles du jour.

Pour chaque article édité le jour D, on prend son nombre d'éditeurs distincts sur
les jours observés de [D-28, D-1] (0 les jours observés sans édition), puis :

    z = (x - médiane) / max(1.4826 × MAD, 1)

1.4826 × MAD estime l'écart-type sans être tiré par les pics passés ; le plancher
à 1 évite qu'un article jamais édité ne devienne "infiniment anormal" pour 1 éditeur.
Un article est émergent si z ≥ 3.5 (seuil usuel du z-score modifié) et s'il a au
moins 3 éditeurs distincts. Le nombre d'émergents varie donc avec l'actualité,
au lieu des 10 % fixes qu'imposait l'Isolation Forest.

Un jour "observé" = un jour dont les éditions formatées existent : un jour où le
pipeline était arrêté n'est pas compté comme un jour à zéro édition.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))

HISTORY_DAYS = 28
MIN_HISTORY_DAYS = 3     # en dessous, pas de référence fiable : personne n'est émergent
Z_THRESHOLD = 3.5
MIN_EDITORS = 3
MAD_TO_SIGMA = 1.4826
SCALE_FLOOR = 1.0
KEY = ["project", "title"]


def edits_folder(day: datetime) -> Path:
    return DATALAKE_ROOT / "formatted" / "wikimedia_stream" / "Edits" / day.strftime("%Y%m%d") / "edits.snappy.parquet"


def observed_days(date: datetime) -> list:
    """Jours de [D-28, D-1] pour lesquels les éditions existent (YYYYMMDD)."""
    days = (date - timedelta(days=k) for k in range(HISTORY_DAYS, 0, -1))
    return [d.strftime("%Y%m%d") for d in days if edits_folder(d).exists()]


def load_history(spark: SparkSession, days: list, articles: DataFrame) -> pd.DataFrame:
    """Éditions et éditeurs par (project, title, day), pour les seuls articles du jour."""
    if not days:
        return pd.DataFrame(columns=[*KEY, "day", "edit_count", "unique_editors"])
    paths = [str(DATALAKE_ROOT / "formatted" / "wikimedia_stream" / "Edits" / d / "edits.snappy.parquet") for d in days]
    return (
        spark.read.parquet(*paths)
        .withColumn("day", F.date_format("timestamp_utc", "yyyyMMdd"))
        .filter(F.col("day").isin(days))
        .join(articles.select(*KEY).distinct(), KEY, "left_semi")
        .groupBy(*KEY, "day")
        .agg(F.count("*").alias("edit_count"), F.countDistinct("user").alias("unique_editors"))
        .toPandas()
    )


def robust_zscores(today: pd.DataFrame, history: pd.DataFrame, days: list, metric: str) -> pd.DataFrame:
    """Médiane, échelle robuste et z-score de `metric`, alignés sur les lignes de `today`."""
    keys = pd.MultiIndex.from_frame(today[KEY])
    matrix = (
        history.pivot_table(index=KEY, columns="day", values=metric, aggfunc="sum", fill_value=0)
        .reindex(index=keys, columns=days, fill_value=0)
        .to_numpy(dtype=float)
    )
    median = np.median(matrix, axis=1)
    mad = np.median(np.abs(matrix - median[:, None]), axis=1)
    scale = np.maximum(MAD_TO_SIGMA * mad, SCALE_FLOOR)
    z = (today[metric].to_numpy(dtype=float) - median) / scale
    return pd.DataFrame({"baseline": median, "z": z}, index=today.index)


def flag_emerging(today: pd.DataFrame, history: pd.DataFrame, days: list) -> pd.DataFrame:
    """Ajoute history_days, baseline_editors, z_edits, z_editors, anomaly_score, is_emerging."""
    out = today.copy()
    out["history_days"] = len(days)
    if len(days) < MIN_HISTORY_DAYS:
        print(f"  ⚠ {len(days)} jour(s) d'historique (< {MIN_HISTORY_DAYS}) : pas de détection ce jour")
        out[["baseline_editors", "z_edits", "z_editors", "anomaly_score"]] = np.nan
        out["is_emerging"] = False
        return out

    editors = robust_zscores(out, history, days, "unique_editors")
    out["baseline_editors"] = editors["baseline"]
    # Seuil testé sur le z exact ; arrondi seulement pour le stockage
    out["is_emerging"] = (editors["z"] >= Z_THRESHOLD) & (out["unique_editors"] >= MIN_EDITORS)
    out["z_editors"] = editors["z"].round(2)
    out["z_edits"] = robust_zscores(out, history, days, "edit_count")["z"].round(2)
    out["anomaly_score"] = out["z_editors"]
    print(f"  → {int(out['is_emerging'].sum())} articles émergents sur {len(out)} ({len(days)} jours d'historique)")
    return out
