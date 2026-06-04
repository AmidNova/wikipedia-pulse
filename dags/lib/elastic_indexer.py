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

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from elasticsearch import Elasticsearch, helpers

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


def df_to_actions(df: pd.DataFrame, index: str, date_str: str):
    """Génère les actions bulk Elasticsearch depuis un DataFrame."""
    for _, row in df.iterrows():
        doc = row.to_dict()
        # Convertir les types pandas non-sérialisables
        for k, v in doc.items():
            if hasattr(v, 'item'):  # numpy types
                doc[k] = v.item()
            elif pd.isna(v) if not isinstance(v, (list, dict)) else False:
                doc[k] = None
        doc["date"] = date_str
        yield {
            "_index": index,
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
        print(f"  ⚠ {len(errors)} erreurs")
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
        print(f"  ⚠ {len(errors)} erreurs")
    return success


def index_to_elastic(**kwargs):
    """Point d'entrée Airflow."""
    execution_date = kwargs["dag_run"].execution_date
    target_date = execution_date.replace(tzinfo=timezone.utc)

    print(f"=== index_to_elastic | {target_date.strftime('%Y-%m-%d')} ===")

    es = get_es_client()

    if not es.ping():
        raise ConnectionError(f"Impossible de joindre Elasticsearch sur {ES_HOST}")

    print(f"Connecté à Elasticsearch ({ES_HOST})")

    try:
        index_trending(es, target_date)
    except FileNotFoundError as e:
        print(f"  ⚠ TrendingArticles introuvable : {e}")

    try:
        index_leadlag(es, target_date)
    except FileNotFoundError as e:
        print(f"  ⚠ EditLeadLag introuvable : {e}")

    print("=== index_to_elastic done ===")
