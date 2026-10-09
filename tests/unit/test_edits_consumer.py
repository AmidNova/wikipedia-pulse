import json
from datetime import datetime, timezone

import pytest

import lib.edits_consumer as ec


def _ts(*args) -> int:
    return int(datetime(*args, tzinfo=timezone.utc).timestamp())


class FakeRecord:
    def __init__(self, value):
        self.value = value


class FakeConsumer:
    """Imite KafkaConsumer : chaque poll() rend un lot et avance la position."""

    def __init__(self, batches, end_offset):
        self.batches = list(batches)
        self.pos = 0
        self.end_offset = end_offset
        self.committed = False

    def end_offsets(self, partitions):
        return {tp: self.end_offset for tp in partitions}

    def position(self, tp):
        return self.pos

    def poll(self, timeout_ms):
        if not self.batches:
            return {}
        batch = self.batches.pop(0)
        self.pos += len(batch)
        return {"tp0": [FakeRecord(v) for v in batch]}

    def commit(self):
        self.committed = True


def test_edit_day_uses_event_timestamp_in_utc():
    assert ec.edit_day({"timestamp": _ts(2026, 10, 1, 23, 59, 59)}) == "20261001"
    assert ec.edit_day({"timestamp": _ts(2026, 10, 2, 0, 0, 0)}) == "20261002"


def test_group_by_day_splits_edits_across_midnight():
    late = {"title": "A", "timestamp": _ts(2026, 10, 1, 23, 59)}
    early = {"title": "B", "timestamp": _ts(2026, 10, 2, 0, 1)}

    groups = ec.group_by_day([late, early])

    assert groups == {"20261001": [late], "20261002": [early]}


def test_drain_writes_each_edit_to_its_event_day_and_commits(tmp_path, monkeypatch):
    monkeypatch.setattr(ec, "DATALAKE_ROOT", tmp_path)
    batches = [
        [{"title": "A", "timestamp": _ts(2026, 10, 1, 12)}],
        [{"title": "B", "timestamp": _ts(2026, 10, 2, 0, 5)}],
    ]
    consumer = FakeConsumer(batches, end_offset=2)

    counts = ec.drain_to_raw(consumer, ["tp0"])

    assert counts == {"20261001": 1, "20261002": 1}
    day1 = tmp_path / "raw" / "wikimedia_stream" / "Edits" / "20261001" / "edits.ndjson"
    assert json.loads(day1.read_text())["title"] == "A"
    assert consumer.committed


def test_drain_does_not_commit_when_backlog_cannot_be_read(tmp_path, monkeypatch):
    monkeypatch.setattr(ec, "DATALAKE_ROOT", tmp_path)
    monkeypatch.setattr(ec, "POLL_TIMEOUT_MS", 1)
    consumer = FakeConsumer(batches=[], end_offset=5)

    with pytest.raises(TimeoutError):
        ec.drain_to_raw(consumer, ["tp0"])

    assert not consumer.committed


def test_ensure_target_day_present_fails_without_raw_file(tmp_path, monkeypatch, context):
    monkeypatch.setattr(ec, "DATALAKE_ROOT", tmp_path)

    with pytest.raises(FileNotFoundError):
        ec.ensure_target_day_present(context)


def test_drain_skips_invalid_messages_and_still_commits(tmp_path, monkeypatch):
    monkeypatch.setattr(ec, "DATALAKE_ROOT", tmp_path)
    batch = [None, {"title": "sans timestamp"}, {"title": "A", "timestamp": _ts(2026, 10, 1, 12)}]
    consumer = FakeConsumer([batch], end_offset=3)

    counts = ec.drain_to_raw(consumer, ["tp0"])

    assert counts == {"20261001": 1}
    assert consumer.committed


def test_deserialize_returns_none_on_garbage():
    assert ec.deserialize(b"{pas du json") is None
    assert ec.deserialize(b'{"a": 1}') == {"a": 1}
