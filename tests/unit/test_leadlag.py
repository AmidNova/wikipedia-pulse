from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

import lib.leadlag as ll

HOURS = ll.WINDOW_HOURS


def _spike(at, height=10.0, base=1.0):
    series = np.full(HOURS, base)
    series[at] = height
    return series


def test_views_peaking_after_edits_is_edit_led():
    metrics = ll.lead_lag_metrics(_spike(10), _spike(13, height=500, base=20))

    assert metrics["best_lag_hours"] == 3
    assert metrics["peak_lag_hours"] == 3
    assert metrics["pattern"] == "edit_led"
    assert metrics["best_corr"] > 0.9


def test_views_peaking_before_edits_is_view_led():
    metrics = ll.lead_lag_metrics(_spike(20), _spike(15, height=500, base=20))

    assert metrics["best_lag_hours"] == -5
    assert metrics["pattern"] == "view_led"


def test_weak_correlation_is_not_significant():
    rng = np.random.default_rng(0)
    edits = rng.poisson(1.0, HOURS).astype(float)
    views = rng.poisson(50.0, HOURS).astype(float)

    metrics = ll.lead_lag_metrics(edits, views)

    assert metrics["p_value"] > ll.ALPHA
    assert metrics["pattern"] == "decorrelated"


def test_strong_lead_is_significant():
    metrics = ll.lead_lag_metrics(_spike(10), _spike(13, height=500, base=20))

    assert metrics["p_value"] < 1e-6


def test_edit_peak_is_searched_in_event_day_only():
    edits = _spike(10)
    edits[30] = 50.0  # gros pic le lendemain : hors jour d'événement
    metrics = ll.lead_lag_metrics(edits, _spike(13, height=500, base=20))

    assert metrics["peak_lag_hours"] == 3


def test_no_views_gives_no_peak():
    metrics = ll.lead_lag_metrics(_spike(10), np.zeros(HOURS))

    assert metrics["peak_lag_hours"] is None
    assert metrics["pattern"] == "decorrelated"


def test_flat_views_are_decorrelated():
    metrics = ll.lead_lag_metrics(_spike(10), np.full(HOURS, 50.0))

    assert metrics["pattern"] == "decorrelated"
    assert np.isnan(metrics["best_corr"])


def test_view_surge_ratio_compares_peak_to_median():
    metrics = ll.lead_lag_metrics(_spike(10), _spike(13, height=500, base=20))

    assert metrics["view_surge_ratio"] == pytest.approx(25.0)


def test_build_leadlag_zero_fills_hours_and_filters_low_activity(spark):
    event_day = datetime(2026, 10, 1, tzinfo=timezone.utc)
    h = lambda n: event_day + timedelta(hours=n)

    edits = spark.createDataFrame(
        [("fr.wikipedia.org", "Paris", h(10))] * 5 + [("fr.wikipedia.org", "Lyon", h(2))] * 2,
        "project STRING, title STRING, timestamp_utc TIMESTAMP",
    )
    views = spark.createDataFrame(
        [(h(n), "fr.wikipedia", "Paris", 500 if n == 13 else 20) for n in range(0, HOURS, 1) if n != 30],
        "hour_utc TIMESTAMP, project STRING, title STRING, views LONG",
    )

    rows = ll.build_leadlag(edits, views, event_day).collect()

    assert [r["title"] for r in rows] == ["Paris"]  # Lyon : 2 éditions seulement
    paris = rows[0]
    assert paris["project"] == "fr.wikipedia"
    assert paris["best_lag_hours"] == 3
    assert paris["pattern"] == "edit_led"
    assert paris["event_edits"] == 5


def test_build_leadlag_removes_the_daily_cycle_of_the_language(spark):
    """Vues qui suivent seulement le cycle jour/nuit de la langue → pas de lead-lag."""
    event_day = datetime(2026, 10, 1, tzinfo=timezone.utc)
    h = lambda n: event_day + timedelta(hours=n)
    cycle = [100 + 900 * (n % 24 >= 18) for n in range(HOURS)]  # soirée chargée

    edits = spark.createDataFrame(
        [("fr.wikipedia.org", "Paris", h(17))] * 5,
        "project STRING, title STRING, timestamp_utc TIMESTAMP",
    )
    views = spark.createDataFrame(
        [(h(n), "fr.wikipedia", "Paris", cycle[n] // 10) for n in range(HOURS)]
        + [(h(n), "fr.wikipedia", "-", cycle[n] * 1000) for n in range(HOURS)],
        "hour_utc TIMESTAMP, project STRING, title STRING, views LONG",
    )

    rows = ll.build_leadlag(edits, views, event_day).collect()

    assert len(rows) == 1
    assert rows[0]["pattern"] == "decorrelated"
    assert rows[0]["title"] == "Paris"
