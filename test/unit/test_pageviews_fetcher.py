import pytest
import requests

import lib.pageviews_fetcher as pf


def test_fetches_the_run_day_not_the_day_before(monkeypatch, context):
    fetched = []
    monkeypatch.setattr(pf, "fetch_top_pageviews", lambda project, date: fetched.append(date) or {})
    monkeypatch.setattr(pf, "save_to_raw", lambda data, project, date: None)

    pf.pageviews_to_raw(**context)

    assert {d.strftime("%Y%m%d") for d in fetched} == {"20261001"}


def test_fails_the_task_when_one_language_fails(monkeypatch, context):
    def fake_fetch(project, date):
        if project == "de.wikipedia":
            raise requests.exceptions.HTTPError("404")
        return {}

    saved = []
    monkeypatch.setattr(pf, "fetch_top_pageviews", fake_fetch)
    monkeypatch.setattr(pf, "save_to_raw", lambda data, project, date: saved.append(project))

    with pytest.raises(RuntimeError, match="de.wikipedia"):
        pf.pageviews_to_raw(**context)

    assert len(saved) == 4  # les autres langues sont quand même sauvegardées
