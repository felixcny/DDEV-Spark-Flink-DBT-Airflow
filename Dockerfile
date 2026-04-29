FROM apache/airflow:2.7.3-python3.10

USER root
# Installation de Java (nécessaire pour Spark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Mise à jour de pip et installation des dépendances
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt