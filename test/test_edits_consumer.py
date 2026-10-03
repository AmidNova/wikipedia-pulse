"""Script manuel : vide le topic Kafka vers la couche raw (nécessite Kafka + producer)."""
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/opt/airflow/dags")

from lib.edits_consumer import edits_stream_to_raw

if __name__ == "__main__":
    edits_stream_to_raw(data_interval_start=datetime.now(timezone.utc) - timedelta(days=1))
    print("\nTest OK")
