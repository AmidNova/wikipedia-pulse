"""
elastic_indexer.py

Indexe la couche usage dans Elasticsearch.

Lecture :
    usage/wikipediaPulse/TrendingArticles/{YYYYMMDD}/trending.snappy.parquet
    usage/wikipediaPulse/EditLeadLag/{YYYYMMDD}/leadlag.snappy.parquet

Index Elasticsearch :
    wikipedia-trending
    wikipedia-leadlag
"""

import hashlib
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from elasticsearch import Elasticsearch, helpers

from lib.common import target_date

DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow/datalake"))
ES_HOST = os.environ.get("ES_HOST", "http://elasticsearch:9200")


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_HOST)


def read_parquet_folder(folder: Path) -> pd.DataFrame:
    """Lit un dossier parquet Spark (contient des part-files)."""
    parts = list(folder.glob("part-*.parquet"))
    if not parts:
        raise FileNotFoundError(f"Aucun fichier parquet dans {folder}")
    return pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)


def doc_id(date_str: str, project: str, title: str) -> str:
    """Identifiant stable d'un article pour un jour : un rerun écrase au lieu de dupliquer."""
    return hashlib.sha1(f"{date_str}|{project}|{title}".encode("utf-8")).hexdigest()


def df_to_actions(df: pd.DataFrame, index: str, date_str: str):
    """Génère les actions bulk Elasticsearch depuis un DataFrame."""
    for _, row in df.iterrows():
        doc = row.to_dict()
        # Convertir les types pandas non-sérialisables
        for k, v in doc.items():
            if not isinstance(v, (list, dict)) and pd.isna(v):
                doc[k] = None
            elif hasattr(v, 'item'):  # numpy types
                doc[k] = v.item()
        doc["date"] = date_str
        yield {
            "_index": index,
            "_id": doc_id(date_str, doc.get("edit_project") or doc["project"], doc["title"]),
            "_source": doc,
        }


def index_trending(es: Elasticsearch, date: datetime) -> int:
    """Indexe les articles trending."""
    date_str = date.strftime("%Y%m%d")
    folder = (
        DATALAKE_ROOT / "usage" / "wikipediaPulse" / "TrendingArticles"
        / date_str / "trending.snappy.parquet"
    )

    print(f"Reading trending from {folder}...")
    df = read_parquet_folder(folder)
    print(f"  → {len(df)} articles trending à indexer")

    actions = list(df_to_actions(df, "wikipedia-trending", date_str))
    success, errors = helpers.bulk(es, actions, raise_on_error=False)
    print(f"  → {success} docs indexés dans 'wikipedia-trending'")
    if errors:
        raise RuntimeError(f"{len(errors)} documents rejetés par Elasticsearch : {errors[:3]}")
    return success


def index_leadlag(es: Elasticsearch, date: datetime) -> int:
    """Indexe le lead-lag."""
    date_str = date.strftime("%Y%m%d")
    folder = (
        DATALAKE_ROOT / "usage" / "wikipediaPulse" / "EditLeadLag"
        / date_str / "leadlag.snappy.parquet"
    )

    print(f"Reading leadlag from {folder}...")
    df = read_parquet_folder(folder)
    print(f"  → {len(df)} entrées lead-lag à indexer")

    actions = list(df_to_actions(df, "wikipedia-leadlag", date_str))
    success, errors = helpers.bulk(es, actions, raise_on_error=False)
    print(f"  → {success} docs indexés dans 'wikipedia-leadlag'")
    if errors:
        raise RuntimeError(f"{len(errors)} documents rejetés par Elasticsearch : {errors[:3]}")
    return success


def index_leadlag_hourly(es: Elasticsearch, event_day: datetime) -> int:
    """Indexe le lead-lag horaire du jour d'événement (J-1 du run)."""
    date_str = event_day.strftime("%Y%m%d")
    folder = (
        DATALAKE_ROOT / "usage" / "wikipediaPulse" / "EditLeadLagHourly"
        / date_str / "leadlag_hourly.snappy.parquet"
    )

    print(f"Reading hourly lead-lag from {folder}...")
    df = read_parquet_folder(folder)
    print(f"  → {len(df)} articles lead-lag horaire à indexer")

    actions = list(df_to_actions(df, "wikipedia-leadlag-hourly", date_str))
    success, errors = helpers.bulk(es, actions, raise_on_error=False)
    print(f"  → {success} docs indexés dans 'wikipedia-leadlag-hourly'")
    if errors:
        raise RuntimeError(f"{len(errors)} documents rejetés par Elasticsearch : {errors[:3]}")
    return success


def index_to_elastic(**kwargs):
    """Point d'entrée Airflow."""
    date = target_date(kwargs)
    print(f"=== index_to_elastic | {date.strftime('%Y-%m-%d')} ===")

    es = get_es_client()
    if not es.ping():
        raise ConnectionError(f"Impossible de joindre Elasticsearch sur {ES_HOST}")
    print(f"Connecté à Elasticsearch ({ES_HOST})")

    index_trending(es, date)
    index_leadlag(es, date)
    print("=== index_to_elastic done ===")


def index_leadlag_hourly_to_elastic(**kwargs):
    """Point d'entrée Airflow — tâche séparée : la branche horaire attend les dumps
    (publiés vers J+1 02:00) et ne doit pas retarder l'indexation principale."""
    event_day = target_date(kwargs) - timedelta(days=1)
    print(f"=== index_leadlag_hourly_to_elastic | {event_day:%Y-%m-%d} ===")

    es = get_es_client()
    if not es.ping():
        raise ConnectionError(f"Impossible de joindre Elasticsearch sur {ES_HOST}")

    index_leadlag_hourly(es, event_day)
    print("=== index_leadlag_hourly_to_elastic done ===")
