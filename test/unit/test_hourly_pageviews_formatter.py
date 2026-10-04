from datetime import datetime

from lib.hourly_pageviews_formatter import RAW_SCHEMA, normalize_hourly


def test_normalize_parses_hour_as_utc_and_restores_spaces(spark):
    raw = spark.createDataFrame([("2026-10-01 13:00:00", "fr.wikipedia", "Tour_Eiffel", 42)], RAW_SCHEMA)

    row = normalize_hourly(raw).collect()[0]

    assert row["hour_utc"] == datetime(2026, 10, 1, 13, 0)
    assert row["title"] == "Tour Eiffel"
    assert row["views"] == 42
