
from fastapi import FastAPI
from time import time
from ingestions.WeatherDataIngestor import WeatherDataIngestor
from ingestions.AirQualityDataIngestior import AirQualityIngestor
import psycopg2
import glob
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL=os.getenv("DATABASE_URL")

app = FastAPI()

CITIES = [
    {"name": "Stockholm", "lat": 59.3293, "lon": 18.0686},
    {"name": "Gothenburg", "lat": 57.7089, "lon": 11.9746},
    {"name": "Malmö", "lat": 55.6059, "lon": 13.0007},
    {"name": "Uppsala", "lat": 59.8586, "lon": 17.6389},
    {"name": "Linköping", "lat": 58.4108, "lon": 15.6214},
    {"name": "Örebro", "lat": 59.2741, "lon": 15.2066},
    {"name": "Västerås", "lat": 59.6099, "lon": 16.5448},
    {"name": "Norrköping", "lat": 58.5877, "lon": 16.1924},
]

@app.on_event("startup")
def run_migrations():
    
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    # Skapa tabell om inte migrationslogg finns
    cur.execute("""
        CREATE TABLE IF NOT EXISTS migrations (
            id SERIAL PRIMARY KEY,
            filename TEXT UNIQUE,
            applied_at TIMESTAMP DEFAULT NOW()
        );
    """)

    # Hitta alla SQL-filer
    for file in sorted(glob.glob("migrations/*.sql")):
        cur.execute("SELECT 1 FROM migrations WHERE filename = %s", (file,))
        if cur.fetchone():
            continue  # redan körd

        with open(file, "r") as f:
            sql = f.read()
            cur.execute(sql)

        cur.execute("INSERT INTO migrations (filename) VALUES (%s)", (file,))

    conn.commit()
    cur.close()
    conn.close()


@app.get("/ingest")
async def route():
    try: 
        weather = WeatherDataIngestor(CITIES).process_cities()
        aq = AirQualityIngestor(CITIES).process_cities()

        return {
            "status": "Success",
            "weather": weather,
            "air_quality": aq,
            "timestamp": int(time())
        }
    except Exception as err:
        return {
            "status": "Error",
            "message": str(err),
            "timestamp": int(time())
        }