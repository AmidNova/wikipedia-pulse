from datetime import datetime, timezone

from lib.common import PROJECTS, USER_AGENT, target_date


def test_target_date_is_midnight_utc_of_data_interval_start():
    context = {"data_interval_start": datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)}

    assert target_date(context) == datetime(2026, 10, 1, tzinfo=timezone.utc)


def test_target_date_truncates_manual_trigger_time():
    context = {"data_interval_start": datetime(2026, 10, 3, 14, 37, tzinfo=timezone.utc)}

    assert target_date(context) == datetime(2026, 10, 3, tzinfo=timezone.utc)


def test_user_agent_carries_contact_info():
    assert "https://github.com/AmidNova/wikipedia-pulse" in USER_AGENT


def test_five_languages_are_tracked():
    assert PROJECTS == ["en.wikipedia", "fr.wikipedia", "de.wikipedia", "es.wikipedia", "ru.wikipedia"]


def test_run_without_logical_date_is_refused():
    import pytest

    with pytest.raises(ValueError, match="date logique"):
        target_date({"data_interval_start": None})
