"""
spark_session.py

Création de la SparkSession locale, commune à tous les jobs Spark du pipeline.
"""

import os
from pathlib import Path

os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
import pyspark  # noqa: E402

os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)

# Les workers Python de Spark (UDF, applyInPandas) doivent pouvoir importer lib.*
DAGS_DIR = str(Path(__file__).resolve().parents[1])
if DAGS_DIR not in os.environ.get("PYTHONPATH", "").split(os.pathsep):
    os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [DAGS_DIR, os.environ.get("PYTHONPATH")]))

from pyspark.sql import SparkSession  # noqa: E402


def get_spark(app_name: str, master: str = "local[*]") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        # Toutes les dates du datalake sont en UTC : on ne dépend pas du fuseau de la machine
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
