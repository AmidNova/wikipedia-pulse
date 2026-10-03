from lib.pulse_combiner import join_edits_pageviews


def test_join_never_matches_across_languages(spark):
    edits = spark.createDataFrame(
        [("Paris", "fr.wikipedia.org", 3), ("Paris", "de.wikipedia.org", 1)],
        ["title", "project", "edit_count"],
    )
    pageviews = spark.createDataFrame(
        [("Paris", "en.wikipedia", 1000, 1), ("Paris", "fr.wikipedia", 50, 9)],
        ["article", "project", "views", "rank"],
    )

    rows = {r["edit_project"]: r for r in join_edits_pageviews(edits, pageviews).collect()}

    assert len(rows) == 2
    assert rows["fr.wikipedia.org"]["pageviews"] == 50
    assert rows["fr.wikipedia.org"]["pageview_project"] == "fr.wikipedia"
    assert rows["de.wikipedia.org"]["pageviews"] == 0
    assert rows["de.wikipedia.org"]["pageview_rank"] == 9999
