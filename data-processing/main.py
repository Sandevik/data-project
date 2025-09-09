
from fastapi import FastAPI
from time import time
import psycopg2
import glob
from dotenv import load_dotenv
import os
from processors.WeatherDataProcessor import WeatherDataProcessor
from processors.AirQualityProcessor import AirQualityDataProcessor

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


@app.get("/process/weather")
async def route():
    res = WeatherDataProcessor().fetch_data().process_data().save_data()
    return res.result

@app.get("/process/all")
async def route():
    pass

@app.get("/process/aq")
async def route():
    res = AirQualityDataProcessor().fetch_data().process_data().save_data()
    return res.result