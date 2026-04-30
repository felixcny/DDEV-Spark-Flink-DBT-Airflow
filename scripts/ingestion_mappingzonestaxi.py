import requests
import boto3
from botocore.client import Config

# Configuration
URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
BUCKET_NAME = "nyc-taxi"
OBJECT_NAME = "trajets/taxi_zone_lookup.csv"

def download_and_upload():
    # 1. Téléchargement
    print("Téléchargement des données de la table mapping...")
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
    print("Upload des données de trajets vers MinIO...")
    s3.Bucket(BUCKET_NAME).put_object(Key=OBJECT_NAME, Body=response.content)
    print("Terminé !")


if __name__ == "__main__":
    download_and_upload()
