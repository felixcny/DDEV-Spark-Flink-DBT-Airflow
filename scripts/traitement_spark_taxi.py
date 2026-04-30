#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


# Création de la session Spark avec S3 ET Postgres
spark = SparkSession.builder \
    .appName("NYC_Taxi_Processing") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4," +
            "com.amazonaws:aws-java-sdk-bundle:1.12.262," +
            "org.postgresql:postgresql:42.7.2") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# In[3]:


df = spark.read.parquet("s3a://nyc-taxi/trajets/yellow_tripdata_2026-01.parquet", header=True, inferSchema=True)


# In[4]:


#pour un meilleur affichage
df.show(5, vertical=True, truncate=False)


# In[ ]:











# On va afficher le nomle nombre de cellules vides par colonne

# In[5]:


from pyspark.sql import functions as F

# On compte uniquement les NULL (marche pour les dates, les nombres et le texte)
df_empty_counts = df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c) 
    for c in df.columns
])

# Affichage horizontal lisible
df_empty_counts.toPandas()


# En visualisant ce résultat, nous voyons que certains trajets n'ont donc pas de passager_count défini. On va donc faire le traitement des calculs de temps de trajets... en partant du principe qu'il y a au moins un passager vu que (tpep_pickup_datetime) et (tpep_dropoff_datetime) ne contiennent pas de valeures nulles

# In[ ]:





# In[6]:


#Maintenant on calcul la durée de trajet pour chaque colonne
duree_trajet = (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60

df.select(
    "tpep_pickup_datetime", 
    "tpep_dropoff_datetime", 
    duree_trajet.alias("durée trajet (minutes)") 
).show(5)


# On voit bien que la colonne (durée) affiche bien la durée des trajets en minutes. On passe ensuite aux autres colonnes.

# In[ ]:





# On va maintenant catégoriser les distances parcourues en catégories

# In[7]:


#on convertit les miles en kilometres
distance_parcourue = F.col("trip_distance") * 1.609

#on attribue les catégories
categorie_distance = (F.when(distance_parcourue <= 2, "0-2 km")
                  .when((distance_parcourue > 2) & (distance_parcourue <= 5), "2-5 km")
                  .otherwise(">5 km"))

df.select(
    "trip_distance", 
    categorie_distance.alias("categorie distance") 
).show(5)


# On voit que le résultat est bon. Passons maintenant au Type de paiement (via table de correspondance). Pour ce faire on regarde le Data Dictionnary sur le site.

# In[8]:


# On fait la correspondance
type_paiement = (F.when(F.col("payment_type") == 0, "Flex Fare trip")
                       .when(F.col("payment_type") == 1, "Credit card")
                       .when(F.col("payment_type") == 2, "Cash")
                       .when(F.col("payment_type") == 3, "No charge")
                       .when(F.col("payment_type") == 4, "Dispute")
                       .when(F.col("payment_type") == 5, "Unknown")
                       .when(F.col("payment_type") == 6, "Voided trip")
                       .otherwise("Other/Unknown"))

df.select(
    "payment_type",
    type_paiement.alias("type de paiement")
).show(10)


# On voit que cela a bien fonctionné.On passe ensuite au calcul du pourcentage de pourboire

# In[ ]:





# In[9]:


# Calcul du pourcentage de pourboire en verifiant que fareamount different de 0
pourcentage_pourboire = (F.when(F.col("fare_amount") > 0, 
                               (F.col("tip_amount") / F.col("fare_amount")) * 100)
                         .otherwise(0))

df.select(
    "fare_amount", 
    "tip_amount", 
    pourcentage_pourboire.alias("pourcentage pourboire")
).show(10)


# On voit que cela a bien fonctionné.On passe ensuite au calcul de  Heure de prise en charge, jour de la semaine

# In[ ]:





# In[ ]:





# In[10]:


# On extrait l'heure de la date
heure_prisencharge = F.hour("tpep_pickup_datetime")

# On extrait le jour
jour_prisencharge = F.dayofweek("tpep_pickup_datetime")

# On convertit les numeros jour en texte
jour_prisencharge = (F.when(jour_prisencharge == 1, "Dimanche")
                  .when(jour_prisencharge == 2, "Lundi")
                  .when(jour_prisencharge == 3, "Mardi")
                  .when(jour_prisencharge == 4, "Mercredi")
                  .when(jour_prisencharge == 5, "Jeudi")
                  .when(jour_prisencharge == 6, "Vendredi")
                  .when(jour_prisencharge == 7, "Samedi"))
df.select(
    "tpep_pickup_datetime",
    heure_prisencharge.alias("heure prise en charge"),
    jour_prisencharge.alias("jour prise en charge")
).show(10)


# On voit que cela a bien fonctionné. On passe ensuite au calcul de Informations de zone (via pickup_location_id et table des zones si dispo).
# On va le faire en téléchargeant la table de mapping des zones sur le sites

# In[ ]:





# In[11]:


# on le récuprère puis on le lit
df_zones = spark.read.csv("s3a://nyc-taxi/trajets/taxi_zone_lookup.csv", header=True, inferSchema=True)

# on affiche voir
df_zones.show(5)


# Maintenant on va faire la jointure de cette table avec notre Dataframe initial 
# ainsi que la jointure avec toutes les colonnes précédemment faites
# 

# In[12]:


df_zones = df_zones.withColumn("LocationID", F.col("LocationID").cast("int"))

df_jointure_zones = df.join(
    df_zones, 
    df.PULocationID == df_zones.LocationID, 
    how="left"
).drop("LocationID") # évier le doublon de colonnes

df_jointure_zones.show(5, vertical=True, truncate=False)


# on rajoute toutes les autres colonnes précédentes

# In[22]:


# L'ouverture de la parenthèse avant "df" permet de sauter des lignes librement
df_final = (df_jointure_zones.withColumn("categorie_distance", categorie_distance)
              .withColumn("duree_trajet", duree_trajet) 
              .withColumn("pourcentage_pourboire", pourcentage_pourboire) 
              .withColumn("heure_prisencharge", heure_prisencharge) 
              .withColumn("jour_prisencharge", jour_prisencharge)
              .withColumn("type_paiement", type_paiement))

# Affiche le premier record en entier
df_final.show(1, vertical=True)


# In[ ]:





# In[ ]:


#MAINTENANT QUE NOS TESTS DE TRANSFORMATIONS ONT ETE FAITS ONT VA CREE NOS SCRIPTS PY DE TRANSFORMATIONS ET Sauvegarder le résultat dans PostgreSQL sous le nom fact_taxi_trips

# Configuration de la base de données
db_properties = {
    "user": "airflow",      
    "password": "airflow", 
    "driver": "org.postgresql.Driver"
}

# L'URL JDBC
jdbc_url = "jdbc:postgresql://postgres:5432/airflow"

try:
    print("Début de l'écriture dans PostgreSQL (table: fact_taxi_trips)...")
    
    (df_final.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "fact_taxi_trips")
        .option("user", db_properties["user"])
        .option("password", db_properties["password"])
        .option("driver", db_properties["driver"])
        .mode("overwrite")
        .save())
    
    print("✅ Sauvegarde terminée avec succès dans PostgreSQL !")

except Exception as e:
    print(f"❌ Erreur lors de l'écriture vers Postgres : {e}")
