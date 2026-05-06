# Data Development Final Project

## Conception d’un pipeline de données modulaire avec traitement batch et streaming

Ce projet met en place une architecture Data Engineering modulaire permettant d’ingérer, stocker, transformer et modéliser des données de trajets Yellow Taxi NYC ainsi que des données météorologiques en temps réel simulé.

L’objectif final est d’alimenter un entrepôt PostgreSQL et des modèles analytiques dbt pour analyser la mobilité à New York et l’impact potentiel de la météo sur les trajets.


## Rôle dans le projet

Les flux métiers sont répartis ainsi :

Étudiant 1 CUNY Felix : flux batch Yellow Taxi NYC.
Étudiant 2 Didyme-erwin Dide: flux streaming météo.
Étudiant 3 HABRAN Karl : orchestration Airflow, dbt, analyse et architecture.

## Stack technique

- Python
- Apache Airflow
- PySpark
- Spark Streaming
- PostgreSQL
- pgAdmin
- MinIO / système de fichiers local
- dbt
- Docker / Docker Compose

---

## Architecture globale


Sources de données
│
├── Yellow Taxi NYC Parquet
│      ↓
│   Script d’ingestion taxi
│      ↓
│   Data Lake / MinIO
│      ↓
│   Spark Batch
│      ↓
│   PostgreSQL : fact_taxi_trips
│
└── OpenWeatherMap API / JSON simulé
       ↓
    Script d’ingestion météo
       ↓
    Data Lake local : data/raw/weather
       ↓
    Spark Streaming
       ↓
    PostgreSQL : weather (vue compatible : dim_weather)

PostgreSQL
   ↓
dbt
   ↓
Modèles analytiques :
- trip_enriched
- trip_summary_per_hour
- high_value_customers


---

## Services Docker

Le projet utilise plusieurs services Docker :

| Service | Rôle |
|---|---|
| postgres | Entrepôt de données et base Airflow |
| airflow_webserver | Interface Airflow |
| airflow_scheduler | Planification et exécution des DAGs |
| minio | Stockage type data lake |
| pgadmin_container | Interface de visualisation PostgreSQL |
| jupyter_spark | Environnement notebook / Spark |


---

## Lancement du projet

Depuis la racine du projet :

```bash
docker-compose up --build -d
```

Vérifier les conteneurs :

```bash
docker ps
```

Les conteneurs principaux attendus sont :

```text
airflow_webserver
airflow_scheduler
postgres
minio
pgadmin_container
```

---

## Accès aux interfaces

### Airflow

URL :

```text
http://localhost:8082
```

Identifiants :

```text
username: admin
password: admin
```

### pgAdmin

URL :

```text
http://localhost:5050
```

Identifiants pgAdmin :

```text
email: admin@admin.com
password: admin
```

Connexion PostgreSQL dans pgAdmin :

```text
Host: postgres
Port: 5432
Database: airflow
Username: airflow
Password: airflow
```

### MinIO

URL :

```text
http://localhost:9001
```

Identifiants :

```text
username: minioadmin
password: minioadmin
```

---

## Airflow

Deux DAGs principaux sont disponibles :

### `weather_streaming_pipeline`

Ce DAG orchestre le flux météo :

```text
ingest_weather → transform_weather
```

- `ingest_weather` récupère ou simule les données météo.
- `transform_weather` lance le traitement Spark Streaming.
- Les données sont écrites dans PostgreSQL.

Résultat validé :

```text
ingest_weather: success
transform_weather: running
```

Le statut `running` est attendu car le job Spark Streaming reste actif pour surveiller les nouveaux fichiers JSON.

### `taxi_batch_pipeline`

Ce DAG orchestre le flux batch taxi :

```text
[ingest_taxi, ingest_taxi_zones] → transform_taxi → run_dbt
```

- `ingest_taxi` télécharge les données taxi.
- `ingest_taxi_zones` télécharge les zones taxi.
- `transform_taxi` lance la transformation Spark batch.
- `run_dbt` exécute les modèles dbt.

Important : le modèle dbt dépend de la table `fact_taxi_trips`, qui doit être produite par le flux batch taxi.

---

## Pipeline météo validé

Le pipeline météo a été validé de bout en bout.

La table PostgreSQL créée est :

```text
weather
```
Pour respecter le nom demandé dans le sujet (`dim_weather`), une vue PostgreSQL compatible a été créée :

```sql
CREATE VIEW dim_weather AS
SELECT * FROM weather;
```

Cette approche permet de conserver le pipeline météo existant tout en respectant le cahier des charges.

Elle contient notamment les colonnes :

- temperature
- humidity
- wind_speed
- condition
- timestamp
- loaded_at
- weather_category
- observation_hour
- day_of_week

Exemple de transformation :

```text
condition = Clear → weather_category = Clair
```

---

## dbt

Le projet dbt se trouve dans :

```text
dbt_project/
```

La connexion dbt à PostgreSQL est validée avec :

```bash
dbt debug
```

Résultat obtenu :

```text
Connection test: OK connection ok
```

Les modèles dbt prévus sont :

- `trip_enriched`
- `trip_summary_per_hour`
- `high_value_customers`

### Dépendance actuelle

Le modèle `trip_enriched` dépend de :

```text
fact_taxi_trips
weather / dim_weather
```

---

## État actuel du projet

| Composant | Statut |
|---|---|
| Docker Compose | ✅ |
| Airflow Webserver | ✅ |
| Airflow Scheduler | ✅ |
| DAG météo | ✅ |
| DAG taxi | ✅ |
| Spark Streaming météo | ✅ |
| Spark Batch taxi | ✅ |
| PostgreSQL | ✅ |
| pgAdmin | ✅ |
| dbt debug | ✅ |
| dbt run | ✅ |
| dbt test | ✅ |
| fact_taxi_trips | ✅ |
| weather | ✅ |
| dim_weather | ✅ |
| trip_enriched | ✅ |
| trip_summary_per_hour | ✅ |
| high_value_customers | ✅ |

---


## Exécution des scripts batch taxi

Voici les étapes pour exécuter manuellement le pipeline batch taxi :

### 1. Création du bucket MinIO

```text
nyc-taxi/trajets
```

### 2. Ingestion des données taxi

```bash
docker exec -it airflow_webserver python /opt/airflow/scripts/ingestion_taxi.py
```

### 3. Ingestion du mapping des zones

```bash
docker exec -it airflow_webserver python /opt/airflow/scripts/ingestion_mappingzonestaxi.py
```

### 4. Transformation Spark batch

```bash
docker exec -it airflow_webserver python /opt/airflow/scripts/traitement_spark_taxi.py
```

### 5. Configuration PostgreSQL dans pgAdmin

- Add New Server
- Name: Projet Taxi
- Host: postgres
- Port: 5432
- Database: airflow
- Username: airflow
- Password: airflow

### 6. Vérification du chargement des données

```bash
docker exec -it airflow_webserver psql -h postgres -U airflow -d airflow -c "\dt"
```

---

# Conclusion

Le projet a permis de construire un pipeline Data Engineering complet utilisant :

* Python
* Apache Airflow
* PySpark
* Spark Streaming
* PostgreSQL
* dbt
* Docker
* MinIO

Le pipeline traite simultanément :

* des données batch historiques Yellow Taxi NYC
* des données météo temps réel simulées

Les données sont ensuite enrichies et modélisées dans dbt afin de produire des indicateurs analytiques permettant d’étudier l’impact des conditions météo sur la mobilité urbaine à New York.

Résultats techniques validés :

* Pipeline batch taxi fonctionnel
* Pipeline météo streaming fonctionnel
* PostgreSQL opérationnel
* DAGs Airflow opérationnels
* Modèles dbt générés avec succès
* Tests dbt validés
* Plus de 3,7 millions de trajets chargés dans l’entrepôt de données



