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
    PostgreSQL : weather / dim_weather

PostgreSQL
   ↓
dbt
   ↓
Modèles analytiques :
- trip_enriched
- trip_summary_per_hour
- high_value_customers
```

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

Actuellement, la table météo est bien présente dans PostgreSQL, mais la table `fact_taxi_trips` n’est pas encore disponible. Elle doit être produite par le flux batch taxi.

L’erreur attendue tant que le flux taxi n’a pas produit sa table est :

```text
relation "public.fact_taxi_trips" does not exist
```

Cela indique que dbt est correctement configuré mais que la source taxi n’est pas encore disponible dans l’entrepôt.

---

## Questions analytiques

### Spark batch taxi

#### Quelle est la distribution des durées de trajets ?

La distribution des durées de trajets doit être calculée à partir de `fact_taxi_trips`, en utilisant la différence entre l’heure de dépôt et l’heure de prise en charge. Une fois la table taxi disponible, cette analyse permet d’identifier les trajets courts, moyens et longs.

#### Les longs trajets reçoivent-ils plus de pourboires ?

Cette analyse repose sur la comparaison entre les tranches de distance et le pourcentage de pourboire. Le modèle dbt `trip_enriched` est prévu pour intégrer ces champs et permettre cette comparaison.

#### Quelles sont les heures de prise en charge les plus chargées ?

Cette question se traite par une agrégation du nombre de trajets par heure de prise en charge. Le modèle `trip_summary_per_hour` sert à produire ce type d’indicateur.

#### Existe-t-il une corrélation entre la distance du trajet et le pourcentage de pourboire ?

La corrélation peut être analysée à partir de `trip_distance` et `tip_percentage` dans la table taxi enrichie. Cette partie dépend de la disponibilité de la table `fact_taxi_trips`.

---

### Spark Streaming météo

#### Quelle est la température moyenne lors des pics de trajets ?

Cette analyse nécessite la jointure entre les trajets taxi et les données météo par heure. Elle sera réalisée dans `trip_enriched`, puis agrégée dans `trip_summary_per_hour`.

#### Quel est l’impact du vent ou de la pluie sur le nombre de trajets ?

Le modèle `trip_summary_per_hour` permet de comparer le nombre de trajets selon la catégorie météo, la température, l’humidité et la vitesse du vent.

---

### dbt / Analyse

#### Quels comportements de trajets observe-t-on selon les types de météo ?

Les modèles dbt permettent de comparer les trajets par catégorie météo : clair, pluvieux, nuageux ou orageux. L’objectif est d’observer si certains types de météo modifient le volume des trajets, leur durée ou le montant des pourboires.

#### À quelle heure observe-t-on le plus de clients à haute valeur ?

Le modèle `high_value_customers` identifie les groupes de passagers associés à un nombre élevé de trajets, une dépense totale importante et un pourcentage moyen de pourboire élevé.

#### La météo influence-t-elle le comportement en matière de pourboires ?

Cette analyse compare le pourcentage moyen de pourboire selon la catégorie météo. Elle dépend de la jointure entre `fact_taxi_trips` et les données météo.

---

## État actuel du projet

| Composant | Statut |
|---|---|
| Docker Compose | OK |
| Airflow Webserver | OK |
| Airflow Scheduler | OK |
| DAG météo | OK |
| Spark Streaming météo | OK |
| PostgreSQL météo | OK |
| pgAdmin | OK |
| dbt debug | OK |
| dbt run | Bloqué tant que `fact_taxi_trips` est absente |
| Flux taxi batch | À finaliser / dépend de l’étudiant 1 |

---

## Limites connues

- La table météo est actuellement nommée `weather`. Pour être conforme au sujet, elle peut être renommée ou exposée via dbt comme `dim_weather`.
- La table `fact_taxi_trips` n’est pas encore présente dans PostgreSQL au moment du test dbt.
- Les modèles dbt sont prêts, mais leur exécution complète dépend de la disponibilité des données taxi transformées.

---

## Prochaines améliorations

- Renommer ou modéliser `weather` en `dim_weather`.
- Finaliser le job Spark batch taxi pour produire `fact_taxi_trips`.
- Ajouter un checkpoint Spark Streaming pour éviter les doublons.
- Ajouter des tests dbt sur les clés, les valeurs nulles et les métriques principales.
- Ajouter un dashboard final avec Metabase, Superset ou un notebook.

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

## Conclusion

La partie orchestration, streaming météo, connexion PostgreSQL et configuration dbt est fonctionnelle. Le pipeline météo a été validé de bout en bout avec des données visibles dans PostgreSQL via pgAdmin.

La modélisation dbt est prête et dépend maintenant de la disponibilité de la table batch taxi `fact_taxi_trips` pour produire les modèles analytiques finaux.


