from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

DATA_PATH = os.getenv("WEATHER_PATH", "/opt/airflow/data/raw/weather")

schema = StructType([
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType())
    ])),
    StructField("wind", StructType([
        StructField("speed", DoubleType())
    ])),
    StructField("weather", ArrayType(StructType([
        StructField("main", StringType()),
        StructField("id", IntegerType())
    ]))),
    StructField("dt", IntegerType())
])

def process_weather():
    spark = SparkSession.builder \
        .appName("weatherTransform") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()
    raw_stream = spark.readStream.format("json") \
        .schema(schema) \
        .option("multiline", "true") \
        .load(DATA_PATH)

    transformed_df = raw_stream.select(
        col("main.temp").alias("temperature"),
        col("main.humidity").alias("humidity"),
        col("wind.speed").alias("wind_speed"),
        col("weather.main").getItem(0).alias("condition"),
        from_unixtime(col("dt")).cast("timestamp").alias("timestamp"),
        current_timestamp().alias("loaded_at")
    )

    enriched_df = transformed_df.withColumn(
        "weather_category",
        when(col("condition") == "Clear", "Clair")
        .when(col("condition") == "Clouds", "Nuageux")
        .when(col("condition").isin("Rain", "Drizzle"), "Pluvieux")
        .otherwise("Orageux")
    ).withColumn("observation_hour", hour(col("timestamp"))) \
     .withColumn("day_of_week", dayofweek(col("timestamp")))

    def write_to_postgres(batch_df, batch_id):
        (batch_df.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/airflow")
            .option("dbtable", "weather")
            .option("user", "airflow")
            .option("password", "airflow")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save())
        print(f"📥 Batch {batch_id} inséré avec succès dans Postgres.")

    query = enriched_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .start()
    
    print("🚀 Début du traitement streaming vers PostgreSQL...")
    query.awaitTermination()

if __name__ == "__main__":
    process_weather()