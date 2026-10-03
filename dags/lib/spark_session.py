"""
spark_session.py

Création de la SparkSession locale, commune à tous les jobs Spark du pipeline.
"""

import os

os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
import pyspark  # noqa: E402

os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)

from pyspark.sql import SparkSession  # noqa: E402


def get_spark(app_name: str, master: str = "local[*]") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
