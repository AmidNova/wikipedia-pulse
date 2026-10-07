import pandas as pd
import pytest

import lib.elastic_indexer as ei


def test_doc_id_is_deterministic_and_scoped_by_language():
    a = ei.doc_id("20261001", "fr.wikipedia.org", "Paris")

    assert a == ei.doc_id("20261001", "fr.wikipedia.org", "Paris")
    assert a != ei.doc_id("20261001", "en.wikipedia.org", "Paris")
    assert a != ei.doc_id("20261002", "fr.wikipedia.org", "Paris")


def test_actions_carry_an_id_so_reruns_overwrite():
    df = pd.DataFrame([{"title": "Paris", "edit_project": "fr.wikipedia.org", "edit_count": 3}])

    action = next(ei.df_to_actions(df, "wikipedia-trending", "20261001"))

    assert action["_id"] == ei.doc_id("20261001", "fr.wikipedia.org", "Paris")
    assert action["_source"]["edit_count"] == 3


def test_missing_usage_folder_fails_the_task(tmp_path, monkeypatch, context):
    monkeypatch.setattr(ei, "DATALAKE_ROOT", tmp_path)

    with pytest.raises(FileNotFoundError):
        ei.index_trending(es=None, date=context["data_interval_start"])


def test_actions_turn_nan_into_null():
    df = pd.DataFrame([{"title": "Paris", "edit_project": "fr.wikipedia.org", "score": float("nan")}])

    action = next(ei.df_to_actions(df, "wikipedia-trending", "20261001"))

    assert action["_source"]["score"] is None


def test_actions_fall_back_to_project_column_for_hourly_leadlag():
    df = pd.DataFrame([{"title": "Paris", "project": "fr.wikipedia", "best_lag_hours": 3}])

    action = next(ei.df_to_actions(df, "wikipedia-leadlag-hourly", "20261001"))

    assert action["_id"] == ei.doc_id("20261001", "fr.wikipedia", "Paris")


def test_crosslang_events_are_keyed_by_wikidata_entity():
    df = pd.DataFrame([{"wikidata_id": "Q243", "titles": "fr:Tour Eiffel | en:Eiffel Tower", "language_count": 2}])

    action = next(ei.df_to_actions(df, "wikipedia-crosslang", "20261001"))

    assert action["_id"] == ei.doc_id("20261001", "wikidata", "Q243")
