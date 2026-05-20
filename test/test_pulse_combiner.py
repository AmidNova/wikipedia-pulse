import sys
import os
from pathlib import Path
from datetime import datetime, timezone

os.environ["DATALAKE_ROOT"] = str(Path.home() / "airflow/datalake")
os.environ["SPARK_HOME"] = "/opt/spark"

sys.path.insert(0, str(Path(__file__).parent.parent / "dags"))

from lib.pulse_combiner import combine

if __name__ == "__main__":
    date = datetime(2026, 5, 18, tzinfo=timezone.utc)
    combine(date)
    print("\nTest OK")
