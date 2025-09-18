
from fastapi import FastAPI, Request
import glob
from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import datetime
import pickle

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


@app.post("/predict")
def route():
    pass

@app.get("/predictions/latest")
def route():
    pass


with open('models/aqi_predictor_1758189715.pkl', 'rb') as f:
    model = pickle.load(f)




@app.post("/predict/simple/aqi")
async def route(request: Request):
    try:
        data = await request.json()

        features = ["temperature", "humidity", "pressure", "wind_speed"]
        X = pd.DataFrame([data])[features]
        print(X.head())

        prediction = model.predict(X)[0]
        aqi = max(1, min(5, round(prediction)))
        aqi_labels = {1: "Good", 2: "Fair", 3: "Moderate", 4: "Poor", 5: "Very Poor"}

        return {
            "status": "success",
            "prediction": {"aqi": int(aqi), "aqi_label": aqi_labels[aqi]},
            "timestamp": int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)
        }
    
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)
        }

