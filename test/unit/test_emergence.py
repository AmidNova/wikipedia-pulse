from datetime import datetime, timezone

import pandas as pd

import lib.emergence as em

DAYS = ["20260924", "20260925", "20260926", "20260927", "20260928"]


def _today(rows):
    return pd.DataFrame(rows, columns=["project", "title", "edit_count", "unique_editors"])


def _history(rows):
    return pd.DataFrame(rows, columns=["project", "title", "day", "edit_count", "unique_editors"])


def test_never_edited_article_scores_its_raw_count_thanks_to_the_floor():
    today = _today([("fr.wikipedia.org", "Nouveau", 6, 5)])

    out = em.flag_emerging(today, _history([]), DAYS)

    assert out.loc[0, "baseline_editors"] == 0
    assert out.loc[0, "z_editors"] == 5.0
    assert bool(out.loc[0, "is_emerging"])


def test_usually_busy_article_is_not_emerging_on_a_normal_day():
    today = _today([("en.wikipedia.org", "Busy", 12, 6)])
    history = _history([("en.wikipedia.org", "Busy", d, 10, n) for d, n in zip(DAYS, [5, 6, 7, 6, 5])])

    out = em.flag_emerging(today, history, DAYS)

    assert out.loc[0, "baseline_editors"] == 6
    assert out.loc[0, "z_editors"] == 0.0
    assert not out.loc[0, "is_emerging"]


def test_spike_against_own_history_is_emerging():
    today = _today([("en.wikipedia.org", "Busy", 80, 30)])
    history = _history([("en.wikipedia.org", "Busy", d, 10, n) for d, n in zip(DAYS, [5, 6, 7, 6, 5])])

    out = em.flag_emerging(today, history, DAYS)

    assert out.loc[0, "z_editors"] >= em.Z_THRESHOLD
    assert bool(out.loc[0, "is_emerging"])


def test_observed_days_without_edits_count_as_zero():
    today = _today([("fr.wikipedia.org", "Rare", 4, 4)])
    # édité 1 jour sur 5 : médiane 0, l'article reste comparé à son calme habituel
    history = _history([("fr.wikipedia.org", "Rare", DAYS[0], 9, 4)])

    out = em.flag_emerging(today, history, DAYS)

    assert out.loc[0, "baseline_editors"] == 0
    assert out.loc[0, "z_editors"] == 4.0


def test_few_editors_are_never_emerging_even_with_high_z():
    today = _today([("fr.wikipedia.org", "Solo", 50, 2)])
    history = _history([])

    out = em.flag_emerging(today, history, DAYS)

    assert not out.loc[0, "is_emerging"]


def test_short_history_disables_detection():
    today = _today([("fr.wikipedia.org", "Nouveau", 50, 20)])

    out = em.flag_emerging(today, _history([]), DAYS[:2])

    assert out.loc[0, "history_days"] == 2
    assert not out.loc[0, "is_emerging"]
    assert pd.isna(out.loc[0, "z_editors"])


def test_history_is_scoped_by_language():
    today = _today([("fr.wikipedia.org", "Paris", 5, 5)])
    history = _history([("en.wikipedia.org", "Paris", d, 50, 30) for d in DAYS])

    out = em.flag_emerging(today, history, DAYS)

    assert out.loc[0, "baseline_editors"] == 0


def test_observed_days_skip_days_when_pipeline_was_down(tmp_path, monkeypatch):
    monkeypatch.setattr(em, "DATALAKE_ROOT", tmp_path)
    for day in ("20260930", "20260928"):
        (tmp_path / "formatted" / "wikimedia_stream" / "Edits" / day / "edits.snappy.parquet").mkdir(parents=True)

    days = em.observed_days(datetime(2026, 10, 1, tzinfo=timezone.utc))

    assert days == ["20260928", "20260930"]


def test_load_history_counts_editors_per_day_for_todays_articles(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(em, "DATALAKE_ROOT", tmp_path)
    folder = tmp_path / "formatted" / "wikimedia_stream" / "Edits" / "20260930" / "edits.snappy.parquet"
    ts = datetime(2026, 9, 30, 12)
    spark.createDataFrame(
        [(ts, "fr.wikipedia.org", "Paris", "a"), (ts, "fr.wikipedia.org", "Paris", "a"),
         (ts, "fr.wikipedia.org", "Paris", "b"), (ts, "fr.wikipedia.org", "Lyon", "c")],
        ["timestamp_utc", "project", "title", "user"],
    ).write.parquet(str(folder))
    today = spark.createDataFrame([("fr.wikipedia.org", "Paris")], ["project", "title"])

    history = em.load_history(spark, ["20260930"], today)

    assert history.to_dict("records") == [
        {"project": "fr.wikipedia.org", "title": "Paris", "day": "20260930", "edit_count": 3, "unique_editors": 2}
    ]


def test_threshold_uses_exact_z_not_the_rounded_one():
    # historique [1, 1, 1, 0, 0] : médiane 1, MAD 0 → échelle 1 → z = 4 - 1 = 3, sous le seuil
    today = _today([("fr.wikipedia.org", "Bord", 10, 4)])
    history = _history([("fr.wikipedia.org", "Bord", d, 1, 1) for d in DAYS[:3]])

    out = em.flag_emerging(today, history, DAYS)

    assert out.loc[0, "z_editors"] == 3.0
    assert not out.loc[0, "is_emerging"]
