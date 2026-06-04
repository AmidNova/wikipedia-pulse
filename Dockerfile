FROM apache/airflow:2.9.2

USER root

# Java pour PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# JAVA_HOME — chemin réel de l'install Java 17
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# PySpark + dépendances du projet
RUN pip install --no-cache-dir \
    requests "pandas>=2.2" pyarrow kafka-python elasticsearch pyspark==4.1.1 scikit-learn
