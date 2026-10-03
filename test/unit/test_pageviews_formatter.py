import json

import pytest

import lib.pageviews_formatter as pvf
from lib.common import PROJECTS


def test_formats_every_fetched_language():
    assert pvf.FORMATTER_PROJECTS == [p.replace(".", "_") for p in PROJECTS]


def test_missing_raw_file_fails_the_task(tmp_path, monkeypatch, context):
    monkeypatch.setattr(pvf, "DATALAKE_ROOT", tmp_path)
    monkeypatch.setattr(pvf, "get_spark", lambda app_name: _NoopSpark())

    with pytest.raises(RuntimeError, match="en_wikipedia"):
        pvf.raw_to_formatted_pageviews(**context)


class _NoopSpark:
    class sparkContext:
        @staticmethod
        def setLogLevel(level):
            pass

    def stop(self):
        pass
