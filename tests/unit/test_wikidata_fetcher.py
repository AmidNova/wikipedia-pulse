import json

import pandas as pd
import pytest
import requests

import lib.wikidata_fetcher as wf


def test_resolve_follows_normalization_then_redirect():
    query = {
        "normalized": [{"from": "paris_(France)", "to": "Paris (France)"}],
        "redirects": [{"from": "Paris (France)", "to": "Paris"}],
        "pages": [{"title": "Paris", "pageprops": {"wikibase_item": "Q90"}}],
    }

    assert wf.resolve_items(["paris_(France)"], query) == {"paris_(France)": "Q90"}


def test_resolve_missing_page_or_page_without_item_gives_none():
    query = {"pages": [
        {"title": "Inexistante", "missing": True},
        {"title": "Sans item"},
        {"title": "Tour Eiffel", "pageprops": {"wikibase_item": "Q243"}},
    ]}

    assert wf.resolve_items(["Inexistante", "Sans item", "Tour Eiffel"], query) == {
        "Inexistante": None, "Sans item": None, "Tour Eiffel": "Q243",
    }


def test_resolve_survives_a_redirect_loop():
    query = {"redirects": [{"from": "A", "to": "B"}, {"from": "B", "to": "A"}], "pages": []}

    assert wf.resolve_items(["A"], query) == {"A": None}


def test_batches_respect_api_limit():
    sizes = [len(b) for b in wf.batches(list(range(120)))]

    assert sizes == [50, 50, 20]


def test_api_url_uses_the_edit_project_domain():
    assert wf.api_url("fr.wikipedia.org") == "https://fr.wikipedia.org/w/api.php"


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload, self.status_code, self.headers = payload, status, {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(str(self.status_code))

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def post(self, url, data, timeout):
        self.calls.append((url, data))
        return self.responses.pop(0)


def test_maxlag_and_429_are_retried(monkeypatch):
    monkeypatch.setattr(wf.time, "sleep", lambda s: None)
    ok = {"query": {"pages": [{"title": "Paris", "pageprops": {"wikibase_item": "Q90"}}]}}
    session = FakeSession([
        FakeResponse({"error": {"code": "maxlag", "info": "lagged"}}),
        FakeResponse({}, status=429),
        FakeResponse(ok),
    ])

    assert wf.query_batch(session, "fr.wikipedia.org", ["Paris"]) == {"Paris": "Q90"}
    assert len(session.calls) == 3
    assert session.calls[0][1]["maxlag"] == str(wf.MAXLAG_S)


def test_other_api_errors_fail_immediately(monkeypatch):
    session = FakeSession([FakeResponse({"error": {"code": "badvalue", "info": "nope"}})])

    with pytest.raises(RuntimeError, match="badvalue"):
        wf.query_batch(session, "fr.wikipedia.org", ["Paris"])


def test_tasks_write_raw_then_formatted(tmp_path, monkeypatch, context):
    monkeypatch.setattr(wf, "DATALAKE_ROOT", tmp_path)
    monkeypatch.setattr(wf.time, "sleep", lambda s: None)
    edits = tmp_path / "formatted" / "wikimedia_stream" / "Edits" / "20261001" / "edits.snappy.parquet"
    edits.mkdir(parents=True)
    pd.DataFrame({
        "project": ["fr.wikipedia.org", "fr.wikipedia.org", "en.wikipedia.org"],
        "title": ["Tour Eiffel", "Tour Eiffel", "Eiffel Tower"],
    }).to_parquet(edits / "part-0.parquet")
    qids = {"Tour Eiffel": "Q243", "Eiffel Tower": "Q243"}
    monkeypatch.setattr(wf, "query_batch", lambda s, p, titles: {t: qids[t] for t in titles})

    wf.wikidata_to_raw(**context)
    wf.raw_to_formatted_wikidata(**context)

    raw = [json.loads(l) for l in wf.raw_file(context["data_interval_start"]).read_text().splitlines()]
    assert len(raw) == 2  # un appel par article distinct
    out = pd.read_parquet(wf.formatted_file(context["data_interval_start"]))
    assert set(out["wikidata_id"]) == {"Q243"}


def test_raw_task_fails_without_edits(tmp_path, monkeypatch, context):
    monkeypatch.setattr(wf, "DATALAKE_ROOT", tmp_path)

    with pytest.raises(FileNotFoundError):
        wf.wikidata_to_raw(**context)


def test_server_errors_and_unreadable_bodies_are_retried(monkeypatch):
    monkeypatch.setattr(wf.time, "sleep", lambda s: None)

    class Unreadable(FakeResponse):
        def json(self):
            raise ValueError("Expecting value")

    ok = {"query": {"pages": [{"title": "Paris", "pageprops": {"wikibase_item": "Q90"}}]}}
    session = FakeSession([FakeResponse({}, status=503), Unreadable({}), FakeResponse(ok)])

    assert wf.query_batch(session, "fr.wikipedia.org", ["Paris"]) == {"Paris": "Q90"}


def test_retry_after_header_is_honoured(monkeypatch):
    waits = []
    monkeypatch.setattr(wf.time, "sleep", waits.append)
    throttled = FakeResponse({}, status=429)
    throttled.headers = {"Retry-After": "42"}
    ok = {"query": {"pages": [{"title": "Paris", "pageprops": {"wikibase_item": "Q90"}}]}}

    wf.query_batch(FakeSession([throttled, FakeResponse(ok)]), "fr.wikipedia.org", ["Paris"])

    assert waits == [42]


def test_paginated_response_fails_instead_of_dropping_titles():
    session = FakeSession([FakeResponse({"continue": {"ppcontinue": "x"}, "query": {"pages": []}})])

    with pytest.raises(RuntimeError, match="paginée"):
        wf.query_batch(session, "fr.wikipedia.org", ["Paris"])


def test_formatting_fails_when_almost_no_article_has_a_qid(tmp_path, monkeypatch, context):
    monkeypatch.setattr(wf, "DATALAKE_ROOT", tmp_path)
    raw = wf.raw_file(context["data_interval_start"])
    raw.parent.mkdir(parents=True)
    raw.write_text("\n".join(json.dumps({"project": "fr.wikipedia.org", "title": f"T{i}", "wikidata_id": None})
                             for i in range(10)) + "\n")

    with pytest.raises(RuntimeError, match="QID"):
        wf.raw_to_formatted_wikidata(**context)


def test_numeric_looking_titles_stay_strings(tmp_path, monkeypatch, context):
    monkeypatch.setattr(wf, "DATALAKE_ROOT", tmp_path)
    raw = wf.raw_file(context["data_interval_start"])
    raw.parent.mkdir(parents=True)
    raw.write_text(json.dumps({"project": "fr.wikipedia.org", "title": "1984", "wikidata_id": "Q208460"}) + "\n")

    wf.raw_to_formatted_wikidata(**context)

    assert pd.read_parquet(wf.formatted_file(context["data_interval_start"]))["title"].tolist() == ["1984"]
