from fastapi import FastAPI, Request
import glob
from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import datetime
import pickle
from trainers.CombinedTrainer import CombinedTrainer

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

PREDICTION_MODEL = "aqi_predictor_1758189715"

app = FastAPI()

@app.on_event("startup")
def run_migrations():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS migrations (
            id SERIAL PRIMARY KEY,
            filename TEXT UNIQUE,
            applied_at TIMESTAMP DEFAULT NOW()
        );
    """)

    for file in sorted(glob.glob("migrations/*.sql")):
        cur.execute("SELECT 1 FROM migrations WHERE filename = %s", (file,))
        if cur.fetchone():
            continue
        with open(file, "r") as f:
            sql = f.read()
            cur.execute(sql)
        cur.execute("INSERT INTO migrations (filename) VALUES (%s)", (file,))

    conn.commit()
    cur.close()
    conn.close()

# Train a model once at startup
trainer = CombinedTrainer().fetch_training_data().extract_features().train()

@app.post("/predict")
async def predict_route(request: Request):
    body = await request.json()
    try:
        prediction = trainer.predict(body)
        return {"prediction": prediction}
    except ValueError as e:
        return {"error": str(e)}

@app.get("/predictions/latest")
def latest_prediction():
    # TODO: implement fetch from DB if needed
    return {"status": "not implemented"}

# Load simple AQI model
with open(f'models/{PREDICTION_MODEL}.pkl', 'rb') as f:
    simple_model = pickle.load(f)

@app.post("/predict/simple/aqi")
async def simple_aqi_route(request: Request):
    timestamp = int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)
    try:
        data = await request.json()
        features = ["temperature", "humidity", "pressure", "wind_speed"]

        X = pd.DataFrame([data])[features]
        prediction = simple_model.predict(X)[0]
        aqi = max(1, min(5, round(prediction)))
        aqi_labels = {1: "Good", 2: "Fair", 3: "Moderate", 4: "Poor", 5: "Very Poor"}

        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        values = (
            float(X.iloc[0]["temperature"]),
            float(X.iloc[0]["humidity"]),
            float(X.iloc[0]["pressure"]),
            float(X.iloc[0]["wind_speed"]),
            PREDICTION_MODEL,
            aqi,
            aqi_labels[aqi],
            timestamp
        )

        cur.execute(
            """
            INSERT INTO simple_aqi_predictions 
            (temperature, humidity, pressure, wind_speed, prediction_model, predicted_aqi, predicted_aqi_label, timestamp) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            values
        )

        conn.commit()
        cur.close()
        conn.close()

        return {
            "status": "success",
            "prediction": {"aqi": int(aqi), "aqi_label": aqi_labels[aqi]},
            "timestamp": timestamp
        }

    except Exception as e:
        return {"status": "error", "message": str(e), "timestamp": timestamp}