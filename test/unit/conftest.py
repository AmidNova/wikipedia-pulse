"""Fixtures partagées des tests unitaires (lancés dans le conteneur worker)."""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "dags"))


@pytest.fixture
def context():
    """Contexte Airflow minimal : run du 1er octobre 2026."""
    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    return {"data_interval_start": start}


@pytest.fixture(scope="session")
def spark():
    from lib.spark_session import get_spark

    session = get_spark("WikipediaPulse-Tests", master="local[1]")
    yield session
    session.stop()
