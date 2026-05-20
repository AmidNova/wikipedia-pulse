import sys
import os
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/opt/airflow/dags")

import lib.edits_consumer as ec
ec.DATALAKE_ROOT = Path("/opt/airflow/datalake")

from lib.edits_consumer import consume_from_kafka, save_to_raw

if __name__ == "__main__":
    date = datetime.now(timezone.utc)
    edits = consume_from_kafka(max_messages=200)
    if edits:
        save_to_raw(edits, date)
    print("\nTest OK")
