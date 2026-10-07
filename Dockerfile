FROM apache/airflow:2.9.2

USER root

# Java pour PySpark ; default-java pointe vers le JDK de l'architecture (amd64 ou arm64)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# Dépendances épinglées du pipeline + pytest (tests lancés dans le conteneur)
COPY requirements.txt requirements-dev.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt
