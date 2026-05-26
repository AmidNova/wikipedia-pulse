"""
Wikipedia Pulse DAG.

Pipeline qui croise les éditions Wikipedia (streaming via Kafka) et les pageviews
(batch quotidien) pour détecter les événements mondiaux émergents.

Architecture :
    edits_stream_to_raw    --> raw_to_formatted_edits     --+
                                                            +--> produce_pulse --> index_to_elastic
    pageviews_to_raw       --> raw_to_formatted_pageviews --+

Les fonctions métier sont importées depuis dags/lib/.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import des vraies fonctions métier depuis lib/
from lib.edits_consumer import edits_stream_to_raw
from lib.edits_formatter import raw_to_formatted_edits
from lib.pageviews_fetcher import pageviews_to_raw
from lib.pageviews_formatter import raw_to_formatted_pageviews
from lib.pulse_combiner import produce_pulse


# index_to_elastic : pas encore implémenté (cours Elasticsearch à venir)
def index_to_elastic(**kwargs):
    """Placeholder — sera implémenté après le cours Elasticsearch."""
    print("TODO: index_to_elastic — en attente du cours Elasticsearch")

# ok

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

    # Source 1 (streaming via Kafka)
    t1a = PythonOperator(task_id="edits_stream_to_raw", python_callable=edits_stream_to_raw)
    t2a = PythonOperator(task_id="raw_to_formatted_edits", python_callable=raw_to_formatted_edits)

    # Source 2 (batch API)
    t1b = PythonOperator(task_id="pageviews_to_raw", python_callable=pageviews_to_raw)
    t2b = PythonOperator(task_id="raw_to_formatted_pageviews", python_callable=raw_to_formatted_pageviews)

    # Combine + Index
    t3 = PythonOperator(task_id="produce_pulse", python_callable=produce_pulse)
    t4 = PythonOperator(task_id="index_to_elastic", python_callable=index_to_elastic)

    # Dépendances
    t1a >> t2a >> t3
    t1b >> t2b >> t3
    t3 >> t4
