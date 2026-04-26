import os
import requests
from dotenv import load_dotenv
from datetime import datetime
import json

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = "New York"

WEATHER_DIR = os.path.join(BASE_DIR, "data", "raw", "weather")

def fetch_weather_data():
    """Récupère les données météo de NYC [cite: 104-105]."""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    
    try:
        response = requests.get(url)
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de la requête API : {e}")
        return None

def save_data(data):
    """Enregistre chaque réponse sous forme de fichier JSON horodaté."""
    if not data:
        print("❌ Aucune donnée à sauvegarder.")
        return

    os.makedirs(WEATHER_DIR, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"weather_{timestamp}.json"
    file_path = os.path.join(WEATHER_DIR, filename)
    
    with open(file_path, "w", encoding='utf-8') as f:
        json.dump(data, f, indent=4)
        
    print(f"✅ Données météo enregistrées dans : {file_path}")

if __name__ == "__main__":
    print(f"Lancement de l'ingestion météo pour {CITY}...")
    weather_data = fetch_weather_data()
    save_data(weather_data)