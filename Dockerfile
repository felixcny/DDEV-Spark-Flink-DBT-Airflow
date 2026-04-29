FROM apache/airflow:2.7.3-python3.10

USER root

# 1. Installation des outils système (autorisé en root)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Configuration Java
RUN ln -s $(readlink -f /usr/bin/java | sed "s:/bin/java::") /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

# --- LA CORRECTION EST ICI ---

# On repasse en utilisateur airflow AVANT le pip install
USER airflow

# 3. Installation de JupyterLab et des dépendances
# On utilise --no-cache-dir pour garder l'image légère
RUN pip install --no-cache-dir jupyterlab jupyter_core

# 4. Installation de ton requirements.txt
COPY --chown=airflow:0 requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# On définit le dossier de travail
WORKDIR /opt/airflow