import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, "/opt/airflow/dags")

import lib.edits_fetcher as ef
ef.MAX_EDITS = 50

from lib.edits_fetcher import consume_stream, save_to_raw

if __name__ == "__main__":
    date = datetime.now(timezone.utc)
    edits = consume_stream(max_edits=50)
    save_to_raw(edits, date)
    print("\nTest OK")
