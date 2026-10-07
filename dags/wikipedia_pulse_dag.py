"""
Wikipedia Pulse DAG.

Pipeline qui croise les éditions Wikipedia (streaming via Kafka) et les pageviews
(batch quotidien) pour détecter les événements mondiaux émergents.

Architecture :
    edits_stream_to_raw    --> raw_to_formatted_edits     --+
    pageviews_to_raw       --> raw_to_formatted_pageviews --+
    raw_to_formatted_edits --> wikidata_to_raw --> raw_to_formatted_wikidata --+--> produce_pulse --> index_to_elastic
    raw_to_formatted_edits --> hourly_pageviews_to_raw --> raw_to_formatted_hourly_pageviews --+
                               raw_to_formatted_hourly_pageviews --> produce_leadlag (jour J-1) --> index_leadlag_hourly
    (produce_pulse --> produce_leadlag : une seule session Spark à la fois)

produce_pulse attend les dumps horaires (vues de tous les articles édités, publiées
vers J+1 02:00) : le top 1000 de l'API ne couvre que ~2 % des articles édités.

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
from lib.hourly_pageviews import hourly_pageviews_to_raw
from lib.hourly_pageviews_formatter import raw_to_formatted_hourly_pageviews
from lib.leadlag import produce_leadlag
from lib.wikidata_fetcher import raw_to_formatted_wikidata, wikidata_to_raw
from lib.elastic_indexer import index_leadlag_hourly_to_elastic, index_to_elastic

# L'API pageviews publie J avec quelques heures de retard : on réessaie toutes les heures
PAGEVIEWS_RETRIES = 8
PAGEVIEWS_RETRY_DELAY = timedelta(hours=1)
# Panne passagère de l'API MediaWiki : chaque lot est déjà réessayé, on retente la tâche plus tard
WIKIDATA_RETRIES = 3
WIKIDATA_RETRY_DELAY = timedelta(minutes=15)

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
    t1b = PythonOperator(
        task_id="pageviews_to_raw",
        python_callable=pageviews_to_raw,
        retries=PAGEVIEWS_RETRIES,
        retry_delay=PAGEVIEWS_RETRY_DELAY,
    )
    t2b = PythonOperator(task_id="raw_to_formatted_pageviews", python_callable=raw_to_formatted_pageviews)

    # Source 3 (dumps horaires) : la dernière heure de J est publiée vers J+1 02:00
    t1c = PythonOperator(
        task_id="hourly_pageviews_to_raw",
        python_callable=hourly_pageviews_to_raw,
        retries=PAGEVIEWS_RETRIES,
        retry_delay=PAGEVIEWS_RETRY_DELAY,
    )
    t2c = PythonOperator(task_id="raw_to_formatted_hourly_pageviews", python_callable=raw_to_formatted_hourly_pageviews)
    t3c = PythonOperator(task_id="produce_leadlag", python_callable=produce_leadlag)
    t4c = PythonOperator(task_id="index_leadlag_hourly", python_callable=index_leadlag_hourly_to_elastic)

    # Source 4 (API MediaWiki) : QID Wikidata des articles édités, pour relier les langues
    t1d = PythonOperator(
        task_id="wikidata_to_raw",
        python_callable=wikidata_to_raw,
        retries=WIKIDATA_RETRIES,
        retry_delay=WIKIDATA_RETRY_DELAY,
    )
    t2d = PythonOperator(task_id="raw_to_formatted_wikidata", python_callable=raw_to_formatted_wikidata)

    # Combine + Index
    # all_done : Wikidata n'est qu'un enrichissement, une panne de l'API ne doit pas bloquer
    # le pulse. Les entrées requises (éditions, vues) manquantes font échouer produce_pulse.
    t3 = PythonOperator(task_id="produce_pulse", python_callable=produce_pulse, trigger_rule="all_done")
    t4 = PythonOperator(task_id="index_to_elastic", python_callable=index_to_elastic)

    # Dépendances
    t1a >> t2a >> t3
    t1b >> t2b >> t3
    t2a >> t1d >> t2d >> t3
    t3 >> t4
    t2a >> t1c >> t2c >> t3c >> t4c
    t2c >> t3
    # Une seule session Spark à la fois sur le worker (driver 1g chacune)
    t3 >> t3c
