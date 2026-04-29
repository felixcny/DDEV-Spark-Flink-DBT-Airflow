FROM apache/airflow:2.7.0

USER root

# Installation de Java (pour Spark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean

# Retour à l'utilisateur airflow pour installer les libs Python
USER airflow

# Copie du fichier requirements.txt dans le container
COPY requirements.txt /requirements.txt

# Installation des dépendances à partir du fichier
RUN pip install --no-cache-dir -r /requirements.txt