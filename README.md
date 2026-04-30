-Création du Bucket dans le DataLAke (MinIO): nyc-taxi/trajets

-Initialisation de nos deux  fichiers .py pour chercher les données de trajets et le fichier de mapping des zones dans notre DataLakeS3 avec MinIO

#Exécuter le fichier d’ingestion des des parquets de données dans le bucket
docker exec -it airflow_webserver python /opt/airflow/scripts/ingestion_taxi.py

#Exécuter le fichier d’ingestion  de données de mapping des zones dans le bucket
docker exec -it airflow_webserver python /opt/airflow/scripts/ingestion_mappingzonestaxi.py

#Exécuter le fichier d’exploration avec Spark
 docker exec -it airflow_webserver python /opt/airflow/scripts/traitement_spark_taxi.py

#Configurer la base de données dans pgadmin
	-Add New Server
	-Genereal : Name: Projet Taxi
	-HostNAme: postgres
	-Port: 5432
	-Maintenance Database: airflow
	-Username: airflow
	-Password: airflow

#Exécuter le script de traitement spark et regarder le chargement de la table en cours dans la BDD
docker exec -it airflow_webserver python /opt/airflow/scripts/traitement_spark_taxi.py
