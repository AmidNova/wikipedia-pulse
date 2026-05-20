"""
Wikipedia Pulse DAG.

Pipeline qui croise les éditions Wikipedia (streaming) et les pageviews (batch quotidien)
pour détecter les événements mondiaux émergents.

Architecture suivie :
    edits_stream_to_raw    ──> raw_to_formatted_edits     ──┐
                                                             ├──> produce_pulse ──> index_to_elastic
    pageviews_to_raw       ──> raw_to_formatted_pageviews ──┘
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# ─── Source 1 : Streaming (Wikimedia EventStreams) ─────────────────────────────

def edits_stream_to_raw(**kwargs):
    """Consomme le flux SSE de Wikimedia EventStreams pendant une fenêtre donnée
    et stocke les éditions brutes dans :
        datalake/raw/wikimedia_stream/Edits/{YYYYMMDD}/edits.ndjson
    """
    print("TODO: Connect to https://stream.wikimedia.org/v2/stream/recentchange")


def raw_to_formatted_edits(**kwargs):
    """Lit les éditions brutes NDJSON, normalise (dates UTC, colonnes nettoyées),
    et écrit en parquet dans :
        datalake/formatted/wikimedia_stream/Edits/{YYYYMMDD}/edits.snappy.parquet
    """
    print("TODO: Read raw NDJSON, normalize, save as parquet")


# ─── Source 2 : Batch (Wikimedia Analytics REST API) ───────────────────────────

def pageviews_to_raw(**kwargs):
    """Appelle l'API Wikimedia Analytics pour récupérer les pageviews du jour
    précédent et stocke le JSON brut dans :
        datalake/raw/wikimedia_analytics/Pageviews/{YYYYMMDD}/pageviews.json
    """
    print("TODO: Call https://wikimedia.org/api/rest_v1/metrics/pageviews")


def raw_to_formatted_pageviews(**kwargs):
    """Lit les pageviews brutes JSON et écrit en parquet dans :
        datalake/formatted/wikimedia_analytics/Pageviews/{YYYYMMDD}/pageviews.snappy.parquet
    """
    print("TODO: Read raw JSON, normalize, save as parquet")


# ─── Combination + Indexing ────────────────────────────────────────────────────

def produce_pulse(**kwargs):
    """Croise éditions et pageviews :
      - détecte les pics d'éditions par article (anomalies)
      - calcule le décalage édition → lecture (lead-lag)
    Sortie dans :
        datalake/usage/wikipediaPulse/TrendingArticles/{YYYYMMDD}/
        datalake/usage/wikipediaPulse/EditLeadLag/{YYYYMMDD}/
    """
    print("TODO: Join edits + pageviews, detect anomalies, compute lead-lag")


def index_to_elastic(**kwargs):
    """Indexe la couche usage dans Elasticsearch pour exposition Kibana."""
    print("TODO: Push usage data into Elasticsearch indices")


# ─── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    "wikipedia_pulse",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Pipeline Wikipedia Pulse : edits streaming + pageviews batch",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bigdata", "wikipedia"],
) as dag:

    dag.doc_md = """
    # Wikipedia Pulse Pipeline

    Croise les éditions Wikipedia en temps réel avec les pageviews quotidiennes
    pour détecter les événements émergents avant qu'ils deviennent viraux.
    """

    # Source 1 (streaming)
    t1a = PythonOperator(task_id="edits_stream_to_raw", python_callable=edits_stream_to_raw)
    t2a = PythonOperator(task_id="raw_to_formatted_edits", python_callable=raw_to_formatted_edits)

    # Source 2 (batch)
    t1b = PythonOperator(task_id="pageviews_to_raw", python_callable=pageviews_to_raw)
    t2b = PythonOperator(task_id="raw_to_formatted_pageviews", python_callable=raw_to_formatted_pageviews)

    # Combine + Index
    t3 = PythonOperator(task_id="produce_pulse", python_callable=produce_pulse)
    t4 = PythonOperator(task_id="index_to_elastic", python_callable=index_to_elastic)

    # Dépendances
    t1a >> t2a >> t3
    t1b >> t2b >> t3
    t3 >> t4