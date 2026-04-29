from pyspark.sql import SparkSession

# Création de la session Spark 
spark = SparkSession.builder \
    .appName("NYC_Taxi_Processing") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

#Lecture de notre fichier
try:
    df = spark.read.parquet("s3a://nyc-taxi/trajets/yellow_tripdata_2026-01.parquet")
    
    #Affichage
    df.show(5)
    print(f"Nombre de lignes chargées : {df.count()}")

except Exception as e:
    print(f"Erreur lors de la lecture : {e}")
