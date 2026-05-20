import sys
import os
from datetime import datetime, timezone

os.environ["DATALAKE_ROOT"] = "/opt/airflow/datalake"

sys.path.insert(0, "/opt/airflow/dags")

from lib.pulse_combiner import combine

if __name__ == "__main__":
    date = datetime(2026, 5, 20, tzinfo=timezone.utc)
    combine(date)
    print("\nTest OK")
