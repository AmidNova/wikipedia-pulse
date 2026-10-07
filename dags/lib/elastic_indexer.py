"""
elastic_indexer.py

Indexe la couche usage dans Elasticsearch.

Lecture → index :
    usage/wikipediaPulse/TrendingArticles/{YYYYMMDD}/trending.snappy.parquet          → wikipedia-trending
    usage/wikipediaPulse/EditLeadLag/{YYYYMMDD}/leadlag.snappy.parquet                → wikipedia-leadlag
    usage/wikipediaPulse/CrossLanguageEvents/{YYYYMMDD}/crosslang.snappy.parquet      → wikipedia-crosslang
    usage/wikipediaPulse/EditLeadLagHourly/{YYYYMMDD}/leadlag_hourly.snappy.parquet   → wikipedia-leadlag-hourly

Les mappings viennent du template "wikipedia-pulse" (posé avant chaque indexation) :
`date` est un vrai champ date, les textes gardent un sous-champ `.keyword`.
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

# index → (table usage, dossier parquet)
TABLES = {
    "wikipedia-trending": ("TrendingArticles", "trending.snappy.parquet"),
    "wikipedia-leadlag": ("EditLeadLag", "leadlag.snappy.parquet"),
    "wikipedia-crosslang": ("CrossLanguageEvents", "crosslang.snappy.parquet"),
    "wikipedia-leadlag-hourly": ("EditLeadLagHourly", "leadlag_hourly.snappy.parquet"),
}

TEMPLATE_NAME = "wikipedia-pulse"
INDEX_TEMPLATE = {
    "index_patterns": ["wikipedia-*"],
    "priority": 100,
    "template": {
        # un seul nœud : pas de réplique, sinon les index restent "yellow"
        "settings": {"number_of_replicas": 0},
        "mappings": {
            "dynamic_templates": [{
                "strings": {
                    "match_mapping_type": "string",
                    "mapping": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                },
            }],
            "properties": {
                "date": {"type": "date", "format": "yyyyMMdd"},
                "first_edit": {"type": "date"},
                "last_edit": {"type": "date"},
                "edit_peak_utc": {"type": "date"},
                "view_peak_utc": {"type": "date"},
                "wikidata_id": {"type": "keyword"},
                "is_emerging": {"type": "boolean"},
            },
        },
    },
}


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_HOST)


def connect() -> Elasticsearch:
    """Client joignable, avec le template d'index à jour (idempotent)."""
    es = get_es_client()
    if not es.ping():
        raise ConnectionError(f"Impossible de joindre Elasticsearch sur {ES_HOST}")
    es.indices.put_index_template(name=TEMPLATE_NAME, **INDEX_TEMPLATE)
    print(f"Connecté à Elasticsearch ({ES_HOST}), template '{TEMPLATE_NAME}' à jour")
    return es


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
        if "wikidata_id" in doc and "title" not in doc:  # événement multilingue : une entité par jour
            key = ("wikidata", doc["wikidata_id"])
        else:
            key = (doc.get("edit_project") or doc["project"], doc["title"])
        yield {
            "_index": index,
            "_id": doc_id(date_str, *key),
            "_source": doc,
        }


def index_table(es: Elasticsearch, index: str, date: datetime) -> int:
    """Indexe la table usage d'un jour dans son index ; échoue si un document est rejeté."""
    date_str = date.strftime("%Y%m%d")
    table, filename = TABLES[index]
    folder = DATALAKE_ROOT / "usage" / "wikipediaPulse" / table / date_str / filename

    print(f"Reading {table} from {folder}...")
    df = read_parquet_folder(folder)
    success, errors = helpers.bulk(es, df_to_actions(df, index, date_str), raise_on_error=False)
    print(f"  → {success}/{len(df)} docs indexés dans '{index}'")
    if errors:
        raise RuntimeError(f"{len(errors)} documents rejetés par Elasticsearch : {errors[:3]}")
    return success


def index_trending(es: Elasticsearch, date: datetime) -> int:
    return index_table(es, "wikipedia-trending", date)


def index_to_elastic(**kwargs):
    """Point d'entrée Airflow."""
    date = target_date(kwargs)
    print(f"=== index_to_elastic | {date.strftime('%Y-%m-%d')} ===")

    es = connect()
    for index in ("wikipedia-trending", "wikipedia-leadlag", "wikipedia-crosslang"):
        index_table(es, index, date)
    print("=== index_to_elastic done ===")


def index_leadlag_hourly_to_elastic(**kwargs):
    """Point d'entrée Airflow — tâche séparée : la branche horaire attend les dumps
    (publiés vers J+1 02:00) et ne doit pas retarder l'indexation principale."""
    event_day = target_date(kwargs) - timedelta(days=1)
    print(f"=== index_leadlag_hourly_to_elastic | {event_day:%Y-%m-%d} ===")

    index_table(connect(), "wikipedia-leadlag-hourly", event_day)
    print("=== index_leadlag_hourly_to_elastic done ===")
