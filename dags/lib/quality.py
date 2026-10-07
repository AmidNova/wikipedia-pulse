"""
quality.py

Contrôles qualité entre les couches : une tâche vérifie sa sortie et échoue AVANT
d'écrire si les données sont manifestement fausses. Chaque seuil correspond à une
panne déjà rencontrée sur ce pipeline.
"""

from datetime import datetime

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lib.common import PROJECTS

# Une journée normale compte ~60 000 éditions : en dessous, le producer était arrêté
MIN_DAILY_EDITS = 1_000
# Le consumer range chaque édit dans le dossier de SON jour : un écart = décalage de dates
MIN_IN_DAY_SHARE = 0.99
# ~96 % des articles édités ont des vues dans les dumps ; 2 % = l'ancien bug du top 1000
MIN_VIEW_MATCH = 0.8
# Détection par historique : ~0,5 % un jour chargé ; 10 % = l'ancien taux fixe
MAX_EMERGING_SHARE = 0.05

EDIT_PROJECTS = {f"{p}.org" for p in PROJECTS}


class DataQualityError(RuntimeError):
    """Données manifestement fausses : la tâche échoue au lieu de les propager."""


def _fail_if(problems: list, layer: str) -> None:
    if problems:
        raise DataQualityError(f"{layer} : " + " ; ".join(problems))


def edits_metrics(df: DataFrame, date: datetime) -> dict:
    """Volume par langue et part des éditions datées du jour D, en une passe Spark."""
    day = date.strftime("%Y-%m-%d")
    rows = (
        df.groupBy("project")
        .agg(F.count("*").alias("n"),
             F.sum(F.when(F.to_date("timestamp_utc") == F.lit(day), 1).otherwise(0)).alias("in_day"))
        .collect()
    )
    total = sum(r["n"] for r in rows)
    return {
        "total": total,
        "per_project": {r["project"]: r["n"] for r in rows},
        "in_day_share": sum(r["in_day"] for r in rows) / total if total else 0.0,
    }


def check_edits(metrics: dict) -> None:
    problems = []
    if metrics["total"] < MIN_DAILY_EDITS:
        problems.append(f"{metrics['total']} éditions (< {MIN_DAILY_EDITS}) : producer arrêté ?")
    missing = sorted(EDIT_PROJECTS - set(metrics["per_project"]))
    if missing:
        problems.append(f"langues absentes : {missing}")
    if metrics["in_day_share"] < MIN_IN_DAY_SHARE:
        problems.append(f"{metrics['in_day_share']:.1%} des éditions datées du jour (< {MIN_IN_DAY_SHARE:.0%})")
    _fail_if(problems, "Edits")


def check_pulse(pdf: pd.DataFrame) -> None:
    """Trending du jour : jointure des vues, unicité, taux d'émergence."""
    problems = []
    match = (pdf["pageviews"] > 0).groupby(pdf["edit_project"]).mean()
    low = match[match < MIN_VIEW_MATCH]
    if not low.empty:
        problems.append(f"part d'articles avec vues trop faible : {low.round(3).to_dict()}")
    duplicates = int(pdf.duplicated(["edit_project", "title"]).sum())
    if duplicates:
        problems.append(f"{duplicates} articles en double (edit_project, title)")
    emerging_share = pdf["is_emerging"].mean() if len(pdf) else 0.0
    if emerging_share > MAX_EMERGING_SHARE:
        problems.append(f"{emerging_share:.1%} d'articles émergents (> {MAX_EMERGING_SHARE:.0%})")
    _fail_if(problems, "TrendingArticles")
