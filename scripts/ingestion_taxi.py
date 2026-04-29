import requests
import boto3
from botocore.client import Config

# Configuration
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
BUCKET_NAME = "nyc-taxi"
OBJECT_NAME = "trajets/yellow_tripdata_2026-01.parquet"

def download_and_upload():
    # 1. Téléchargement
    print("Téléchargement des données...")
    response = requests.get(URL)
    
    # 2. Connexion à MinIO (via le protocole S3)
    s3 = boto3.resource('s3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    # 3. Upload
    print("Upload vers MinIO...")
    s3.Bucket(BUCKET_NAME).put_object(Key=OBJECT_NAME, Body=response.content)
    print("Terminé !")

if __name__ == "__main__":
    download_and_upload()
