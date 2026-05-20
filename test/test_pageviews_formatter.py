import sys
import os
from pathlib import Path
from datetime import datetime, timezone

os.environ["DATALAKE_ROOT"] = "/opt/airflow/datalake"
os.environ["SPARK_HOME"] = "/opt/spark"

sys.path.insert(0, "/opt/airflow/dags")

from lib.pageviews_formatter import get_spark, convert_pageviews

if __name__ == "__main__":
    date = datetime(2026, 5, 18, tzinfo=timezone.utc)
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    for project in ["en_wikipedia", "fr_wikipedia"]:
        convert_pageviews(project, date, spark)
    spark.stop()
    print("\nTest OK")
