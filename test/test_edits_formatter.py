import sys
import os
from datetime import datetime, timezone

os.environ["DATALAKE_ROOT"] = "/opt/airflow/datalake"

sys.path.insert(0, "/opt/airflow/dags")

from lib.edits_formatter import get_spark, convert_edits

if __name__ == "__main__":
    date = datetime(2026, 5, 20, tzinfo=timezone.utc)
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    convert_edits(date, spark)
    spark.stop()
    print("\nTest OK")
