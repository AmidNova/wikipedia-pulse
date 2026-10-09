import gzip
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
import requests

import lib.hourly_pageviews as hp


def _utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


def test_dump_url_points_to_file_named_after_the_end_of_the_hour():
    assert hp.dump_url(_utc(2026, 10, 2, 11)) == (
        "https://dumps.wikimedia.org/other/pageviews/2026/2026-10/pageviews-20261002-120000.gz"
    )


def test_dump_url_last_hour_of_month_rolls_into_next_month_folder():
    assert hp.dump_url(_utc(2026, 10, 31, 23)) == (
        "https://dumps.wikimedia.org/other/pageviews/2026/2026-11/pageviews-20261101-000000.gz"
    )


@pytest.mark.parametrize("line, expected", [
    ("fr.m Paris 12 0\n", ("fr.wikipedia", "Paris", 12)),
    ("en Albert_Einstein 3 0\n", ("en.wikipedia", "Albert_Einstein", 3)),
    ("commons.m File:X.jpg 1 0\n", None),
    ("fr Paris pas_un_nombre 0\n", None),
    ("fr\n", None),
])
def test_parse_line(line, expected):
    assert hp.parse_line(line) == expected


def test_filter_hour_merges_desktop_and_mobile_and_adds_language_totals():
    lines = ["fr Paris 10 0\n", "fr.m Paris 5 0\n", "fr Lyon 7 0\n", "en Paris 99 0\n"]
    candidates = {("fr.wikipedia", "Paris"), ("de.wikipedia", "Berlin")}

    assert hp.filter_hour(lines, candidates) == {
        ("fr.wikipedia", "Paris"): 15,
        ("fr.wikipedia", hp.TOTAL_TITLE): 22,
        ("en.wikipedia", hp.TOTAL_TITLE): 99,
    }


def test_candidates_use_dump_title_format():
    assert hp.to_candidate("fr.wikipedia.org", "Tour Eiffel") == ("fr.wikipedia", "Tour_Eiffel")


def test_task_writes_24_hours_of_filtered_views(tmp_path, monkeypatch, context):
    monkeypatch.setattr(hp, "DATALAKE_ROOT", tmp_path)
    monkeypatch.setattr(hp, "ensure_published", lambda url: None)
    monkeypatch.setattr(hp, "load_candidates", lambda date: {("fr.wikipedia", "Paris")})
    urls = []

    @contextmanager
    def fake_open(url):
        urls.append(url)
        yield ["fr Paris 2 0\n", "fr Lyon 1 0\n"]

    monkeypatch.setattr(hp, "open_dump", fake_open)

    hp.hourly_pageviews_to_raw(**context)

    out = tmp_path / "raw" / "wikimedia_dumps" / "PageviewsHourly" / "20261001" / "pageviews_hourly.tsv.gz"
    rows = gzip.open(out, "rt", encoding="utf-8").read().splitlines()
    assert len(urls) == 24
    assert urls[-1].endswith("pageviews-20261002-000000.gz")
    assert "2026-10-01 00:00:00\tfr.wikipedia\tParis\t2" in rows
    assert "2026-10-01 00:00:00\tfr.wikipedia\t-\t3" in rows
    assert len(rows) == 48  # article + total de la langue, 24 heures


def test_task_fails_when_a_dump_is_not_published_yet(tmp_path, monkeypatch, context):
    monkeypatch.setattr(hp, "DATALAKE_ROOT", tmp_path)
    monkeypatch.setattr(hp, "ensure_published", lambda url: None)
    monkeypatch.setattr(hp, "load_candidates", lambda date: {("fr.wikipedia", "Paris")})

    def fake_open(url):
        raise requests.exceptions.HTTPError("404")

    monkeypatch.setattr(hp, "open_dump", fake_open)

    with pytest.raises(requests.exceptions.HTTPError):
        hp.hourly_pageviews_to_raw(**context)

    assert not (tmp_path / "raw" / "wikimedia_dumps" / "PageviewsHourly" / "20261001" / "pageviews_hourly.tsv.gz").exists()


def test_task_fails_without_candidates(tmp_path, monkeypatch, context):
    monkeypatch.setattr(hp, "DATALAKE_ROOT", tmp_path)
    monkeypatch.setattr(hp, "load_candidates", lambda date: set())

    with pytest.raises(RuntimeError, match="Aucun article édité"):
        hp.hourly_pageviews_to_raw(**context)


def test_transient_network_error_is_retried_for_that_hour_only(monkeypatch):
    import urllib3

    monkeypatch.setattr(hp.time, "sleep", lambda s: None)
    calls = []

    @contextmanager
    def flaky_open(url):
        calls.append(url)
        if len(calls) == 1:
            raise urllib3.exceptions.ReadTimeoutError(None, url, "Read timed out.")
        yield ["fr Paris 2 0\n"]

    monkeypatch.setattr(hp, "open_dump", flaky_open)

    views = hp.read_hour("u", {("fr.wikipedia", "Paris")})

    assert views[("fr.wikipedia", "Paris")] == 2
    assert len(calls) == 2


def test_unpublished_dump_is_not_retried_in_task(monkeypatch):
    monkeypatch.setattr(hp.time, "sleep", lambda s: None)
    calls = []

    def missing(url):
        calls.append(url)
        raise requests.exceptions.HTTPError("404")

    monkeypatch.setattr(hp, "open_dump", missing)

    with pytest.raises(requests.exceptions.HTTPError):
        hp.read_hour("u", set())

    assert len(calls) == 1


def test_persistent_network_error_gives_up_after_max_attempts(monkeypatch):
    monkeypatch.setattr(hp.time, "sleep", lambda s: None)
    calls = []

    def down(url):
        calls.append(url)
        raise requests.exceptions.ConnectionError("down")

    monkeypatch.setattr(hp, "open_dump", down)

    with pytest.raises(requests.exceptions.ConnectionError):
        hp.read_hour("u", set())

    assert len(calls) == hp.MAX_ATTEMPTS


def test_task_fails_fast_before_downloading_when_last_dump_is_missing(tmp_path, monkeypatch, context):
    monkeypatch.setattr(hp, "DATALAKE_ROOT", tmp_path)
    monkeypatch.setattr(hp, "load_candidates", lambda date: {("fr.wikipedia", "Paris")})
    probed, opened = [], []

    def not_yet(url):
        probed.append(url)
        raise requests.exceptions.HTTPError("404")

    monkeypatch.setattr(hp, "ensure_published", not_yet)
    monkeypatch.setattr(hp, "open_dump", lambda url: opened.append(url))

    with pytest.raises(requests.exceptions.HTTPError):
        hp.hourly_pageviews_to_raw(**context)

    assert probed == ["https://dumps.wikimedia.org/other/pageviews/2026/2026-10/pageviews-20261002-000000.gz"]
    assert opened == []


def test_corrupt_gzip_is_retried(monkeypatch):
    import zlib

    monkeypatch.setattr(hp.time, "sleep", lambda s: None)
    calls = []

    @contextmanager
    def corrupt_once(url):
        calls.append(url)
        if len(calls) == 1:
            raise zlib.error("invalid stored block lengths")
        yield ["fr Paris 2 0\n"]

    monkeypatch.setattr(hp, "open_dump", corrupt_once)

    assert hp.read_hour("u", {("fr.wikipedia", "Paris")})[("fr.wikipedia", "Paris")] == 2
