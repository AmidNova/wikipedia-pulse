from datetime import datetime

import pandas as pd

from lib.pulse_combiner import (
    add_language_signature,
    cross_language_events,
    daily_views,
    join_edits_pageviews,
)


def _views(spark, rows):
    return spark.createDataFrame(rows, ["project", "title", "views"])


def _top(spark, rows):
    return spark.createDataFrame(rows, "article STRING, project STRING, views LONG, rank INT")


def test_join_never_matches_across_languages(spark):
    edits = spark.createDataFrame(
        [("Paris", "fr.wikipedia.org", 3), ("Paris", "de.wikipedia.org", 1)],
        ["title", "project", "edit_count"],
    )
    views = _views(spark, [("en.wikipedia", "Paris", 1000), ("fr.wikipedia", "Paris", 50)])
    top = _top(spark, [("Paris", "fr.wikipedia", 50, 9)])

    rows = {r["edit_project"]: r for r in join_edits_pageviews(edits, views, top).collect()}

    assert len(rows) == 2
    assert rows["fr.wikipedia.org"]["pageviews"] == 50
    assert rows["fr.wikipedia.org"]["pageview_rank"] == 9
    assert rows["fr.wikipedia.org"]["pageview_project"] == "fr.wikipedia"
    assert rows["de.wikipedia.org"]["pageviews"] == 0
    assert rows["de.wikipedia.org"]["pageview_rank"] == 9999


def test_articles_outside_the_top_still_get_their_views(spark):
    edits = spark.createDataFrame([("Obscur", "fr.wikipedia.org", 2)], ["title", "project", "edit_count"])

    row = join_edits_pageviews(edits, _views(spark, [("fr.wikipedia", "Obscur", 42)]), _top(spark, [])).first()

    assert row["pageviews"] == 42
    assert row["pageview_rank"] == 9999


def test_daily_views_sums_hours_and_drops_language_totals(spark):
    hourly = spark.createDataFrame(
        [("fr.wikipedia", "Paris", 3), ("fr.wikipedia", "Paris", 4), ("fr.wikipedia", "-", 1000)],
        ["project", "title", "views"],
    )

    rows = [r.asDict() for r in daily_views(hourly).collect()]

    assert rows == [{"project": "fr.wikipedia", "title": "Paris", "views": 7}]


def _trending(rows):
    return pd.DataFrame(rows, columns=[
        "title", "edit_project", "wikidata_id", "first_edit", "edit_count", "unique_editors", "pageviews", "is_emerging",
    ])


T0 = datetime(2026, 10, 1, 8, 0)


def test_language_signature_groups_titles_by_wikidata_entity():
    pdf = _trending([
        ("Eiffel Tower", "en.wikipedia.org", "Q243", T0 + pd.Timedelta(minutes=30), 3, 2, 100, False),
        ("Tour Eiffel", "fr.wikipedia.org", "Q243", T0, 5, 4, 80, True),
        ("Paris", "en.wikipedia.org", "Q90", T0, 1, 1, 10, False),
        ("Paris", "de.wikipedia.org", None, T0, 1, 1, 0, False),  # même titre, entité inconnue
    ])

    out = add_language_signature(pdf).set_index(["edit_project", "title"])

    assert out.loc[("en.wikipedia.org", "Eiffel Tower"), "language_count"] == 2
    assert out.loc[("en.wikipedia.org", "Eiffel Tower"), "first_language"] == "fr.wikipedia.org"
    assert out.loc[("en.wikipedia.org", "Eiffel Tower"), "languages_editing"] == "fr.wikipedia.org,en.wikipedia.org"
    assert out.loc[("en.wikipedia.org", "Eiffel Tower"), "language_lag_minutes"] == 30
    assert out.loc[("fr.wikipedia.org", "Tour Eiffel"), "language_lag_minutes"] == 0
    # "Paris" EN et DE ne sont plus confondus sur le seul titre
    assert out.loc[("en.wikipedia.org", "Paris"), "language_count"] == 1
    assert out.loc[("de.wikipedia.org", "Paris"), "language_count"] == 1


def test_cross_language_events_keep_only_multilingual_entities():
    pdf = add_language_signature(_trending([
        ("Eiffel Tower", "en.wikipedia.org", "Q243", T0 + pd.Timedelta(minutes=45), 3, 2, 100, False),
        ("Tour Eiffel", "fr.wikipedia.org", "Q243", T0, 5, 4, 80, True),
        ("Paris", "en.wikipedia.org", "Q90", T0, 1, 1, 10, False),
    ]))

    events = cross_language_events(pdf)

    assert len(events) == 1
    event = events.iloc[0]
    assert event["wikidata_id"] == "Q243"
    assert event["first_language"] == "fr.wikipedia.org"
    assert event["spread_minutes"] == 45
    assert event["titles"] == "fr:Tour Eiffel | en:Eiffel Tower"
    assert event["total_edits"] == 8
    assert event["sum_editors"] == 6
    assert event["total_pageviews"] == 180
    assert event["emerging_languages"] == 1
    assert bool(event["is_emerging"])


def test_cross_language_events_empty_day_keeps_schema():
    pdf = add_language_signature(_trending([("Paris", "en.wikipedia.org", "Q90", T0, 1, 1, 10, False)]))

    events = cross_language_events(pdf)

    assert events.empty
    assert "wikidata_id" in events.columns
