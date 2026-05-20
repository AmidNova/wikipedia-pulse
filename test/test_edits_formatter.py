import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/opt/airflow/dags")

import lib.edits_formatter as ef
ef.DATALAKE_ROOT = Path("/opt/airflow/datalake")

from lib.edits_formatter import convert_edits

if __name__ == "__main__":
    date = datetime(2026, 5, 20, tzinfo=timezone.utc)
    convert_edits(date)
    print("\nTest OK")
