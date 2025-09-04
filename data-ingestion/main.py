
from fastapi import FastAPI
from time import time
from ingestions.WeatherDataIngestion import WeatherDataIngestion
import psycopg2
import glob
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL=os.getenv("DATABASE_URL")

app = FastAPI()



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
        weather = WeatherDataIngestion().process_cities()

        """ aq = AirQualityIngestion()
        aq_results = aq.process_all_cities() """

        return {
            "status": "Success",
            "weather": weather,
           """  "air_quality": aq_results, """
            "timestamp": int(time())
        }
    except Exception as err:
        return {
            "status": "Error",
            "message": str(err),
            "timestamp": int(time())
        }