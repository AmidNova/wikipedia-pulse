"""Intégrité du DAG : lancé dans l'image Airflow, ignoré là où Airflow n'est pas installé (CI)."""
import importlib

import pytest

pytest.importorskip("airflow")


@pytest.fixture(scope="module")
def dag():
    """Importer le module suffit à détecter une erreur d'import ou de dépendance cyclique."""
    module = importlib.import_module("wikipedia_pulse_dag")
    assert module.dag.dag_id == "wikipedia_pulse"
    return module.dag


def test_pulse_waits_for_every_source(dag):
    upstream = dag.get_task("produce_pulse").upstream_task_ids

    assert upstream == {
        "raw_to_formatted_edits", "raw_to_formatted_pageviews",
        "raw_to_formatted_wikidata", "raw_to_formatted_hourly_pageviews",
    }


def test_only_one_spark_session_at_a_time(dag):
    assert "produce_pulse" in dag.get_task("produce_leadlag").upstream_task_ids


def test_wikidata_outage_does_not_block_the_pulse(dag):
    assert dag.get_task("produce_pulse").trigger_rule == "all_done"


def test_scheduled_run_processes_the_previous_day(dag):
    """Le run lancé le 02/10 à 00:00 doit couvrir [01/10, 02/10) : target_date = 01/10."""
    from datetime import datetime, timezone

    from airflow.timetables.base import DataInterval, TimeRestriction

    from lib.common import target_date

    last = DataInterval(datetime(2026, 9, 30, tzinfo=timezone.utc), datetime(2026, 10, 1, tzinfo=timezone.utc))
    info = dag.timetable.next_dagrun_info(
        last_automated_data_interval=last,
        restriction=TimeRestriction(earliest=None, latest=None, catchup=True),
    )

    assert info.run_after == datetime(2026, 10, 2, tzinfo=timezone.utc)
    assert target_date({"data_interval_start": info.data_interval.start}) == datetime(2026, 10, 1, tzinfo=timezone.utc)


def test_no_catchup(dag):
    assert dag.catchup is False
