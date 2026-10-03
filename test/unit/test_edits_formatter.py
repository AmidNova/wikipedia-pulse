from lib.edits_formatter import normalize_edits

SCHEMA = (
    "timestamp LONG, project STRING, title STRING, user STRING, type STRING, "
    "minor BOOLEAN, length_old LONG, length_new LONG, comment STRING"
)


def _row(**overrides):
    row = {
        "timestamp": 1790856000, "project": "fr.wikipedia.org", "title": " Paris ",
        "user": "Alice", "type": "edit", "minor": False,
        "length_old": 100, "length_new": 150, "comment": None,
    }
    return {**row, **overrides}


def test_normalize_drops_duplicate_edits(spark):
    df = spark.createDataFrame([_row(), _row(), _row(user="Bob")], SCHEMA)

    result = normalize_edits(df).collect()

    assert len(result) == 2


def test_normalize_cleans_fields(spark):
    df = spark.createDataFrame([_row()], SCHEMA)

    row = normalize_edits(df).collect()[0]

    assert row["title"] == "Paris"
    assert row["edit_size"] == 50
    assert row["comment"] == ""


REV_SCHEMA = SCHEMA + ", rev_id LONG"


def test_normalize_keeps_distinct_revisions_with_identical_fields(spark):
    df = spark.createDataFrame([_row(rev_id=1), _row(rev_id=2), _row(rev_id=2)], REV_SCHEMA)

    rows = normalize_edits(df).collect()

    assert sorted(r["rev_id"] for r in rows) == [1, 2]


def test_normalize_accepts_legacy_files_without_rev_id(spark):
    df = spark.createDataFrame([_row(), _row()], SCHEMA)

    rows = normalize_edits(df).collect()

    assert len(rows) == 1
    assert rows[0]["rev_id"] is None
