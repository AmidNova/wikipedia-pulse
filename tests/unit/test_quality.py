from datetime import datetime, timezone

import pandas as pd
import pytest

import lib.quality as q

DAY = datetime(2026, 10, 1, tzinfo=timezone.utc)
ALL_LANGS = {p: 20_000 for p in q.EDIT_PROJECTS}


def _metrics(total=100_000, per_project=None, in_day_share=1.0):
    return {"total": total, "per_project": per_project or ALL_LANGS, "in_day_share": in_day_share}


def test_normal_day_passes():
    q.check_edits(_metrics())


def test_dead_producer_is_caught():
    with pytest.raises(q.DataQualityError, match="producer"):
        q.check_edits(_metrics(total=42))


def test_missing_language_is_caught():
    partial = {p: n for p, n in ALL_LANGS.items() if p != "ru.wikipedia.org"}
    with pytest.raises(q.DataQualityError, match="ru.wikipedia.org"):
        q.check_edits(_metrics(per_project=partial))


def test_date_misalignment_is_caught():
    with pytest.raises(q.DataQualityError, match="datées du jour"):
        q.check_edits(_metrics(in_day_share=0.6))


def test_edits_metrics_counts_in_day_share(spark):
    df = spark.createDataFrame(
        [(datetime(2026, 10, 1, 23, 59), "fr.wikipedia.org"),
         (datetime(2026, 10, 1, 0, 0), "fr.wikipedia.org"),
         (datetime(2026, 10, 2, 0, 1), "en.wikipedia.org"),
         (datetime(2026, 9, 30, 12), "en.wikipedia.org")],
        ["timestamp_utc", "project"],
    )

    m = q.edits_metrics(df, DAY)

    assert m["total"] == 4
    assert m["per_project"] == {"fr.wikipedia.org": 2, "en.wikipedia.org": 2}
    assert m["in_day_share"] == 0.5


def _trending(views, emerging=None, titles=None):
    n = len(views)
    return pd.DataFrame({
        "edit_project": ["fr.wikipedia.org"] * n,
        "title": titles or [f"T{i}" for i in range(n)],
        "pageviews": views,
        "is_emerging": emerging or [False] * n,
    })


def test_healthy_pulse_passes():
    q.check_pulse(_trending([10] * 99 + [0], emerging=[True] + [False] * 99))


def test_top1000_bug_is_caught():
    with pytest.raises(q.DataQualityError, match="vues"):
        q.check_pulse(_trending([10] * 2 + [0] * 98))


def test_duplicate_articles_are_caught():
    with pytest.raises(q.DataQualityError, match="double"):
        q.check_pulse(_trending([10, 10], titles=["Paris", "Paris"]))


def test_runaway_emergence_is_caught():
    with pytest.raises(q.DataQualityError, match="émergents"):
        q.check_pulse(_trending([10] * 10, emerging=[True] + [False] * 9))
