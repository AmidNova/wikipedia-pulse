import sys
import os
from datetime import datetime, timezone

os.environ["DATALAKE_ROOT"] = "/opt/airflow/datalake"
os.environ["ES_HOST"] = "http://elasticsearch:9200"

sys.path.insert(0, "/opt/airflow/dags")

from lib.elastic_indexer import index_to_elastic

class FakeDagRun:
    execution_date = datetime(2026, 6, 4, tzinfo=timezone.utc)

if __name__ == "__main__":
    index_to_elastic(dag_run=FakeDagRun())
    print("\nTest OK")
