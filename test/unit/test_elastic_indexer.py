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


class FakeIndices:
    def __init__(self):
        self.templates = {}

    def put_index_template(self, name, **body):
        self.templates[name] = body


class FakeES:
    def __init__(self, up=True):
        self.up, self.indices = up, FakeIndices()

    def ping(self):
        return self.up


def test_connect_installs_the_index_template(monkeypatch):
    es = FakeES()
    monkeypatch.setattr(ei, "get_es_client", lambda: es)

    assert ei.connect() is es
    template = es.indices.templates[ei.TEMPLATE_NAME]
    assert template["index_patterns"] == ["wikipedia-*"]
    assert template["template"]["mappings"]["properties"]["date"] == {"type": "date", "format": "yyyyMMdd"}


def test_connect_fails_when_elasticsearch_is_down(monkeypatch):
    monkeypatch.setattr(ei, "get_es_client", lambda: FakeES(up=False))

    with pytest.raises(ConnectionError):
        ei.connect()


def test_every_index_matches_the_template_pattern():
    assert all(index.startswith("wikipedia-") for index in ei.TABLES)
