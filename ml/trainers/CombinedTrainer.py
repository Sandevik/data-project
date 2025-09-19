from .Trainer import Trainer
import json
import pandas as pd
import datetime
import pickle
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler

MODEL_NAME = "advanced_model_"

class CombinedTrainer(Trainer):
    data: pd.DataFrame | None

    def __init__(self, model_uri: str | None = None):
        self.model = None
        self.scaler = None
        self.feature_names = None

        if model_uri:
            try:
                with open(model_uri, "rb") as f:
                    self.model = pickle.load(f)
            except Exception:
                print("⚠️ Could not load model from", model_uri)

        super().__init__()

    def fetch_training_data(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT json_data FROM combined_processed_ingestion_data 
            ORDER BY ingestion_timestamp DESC
        """)
        res = cursor.fetchall()
        json_strings = [r[0] for r in res]
        records = [json.loads(js) if isinstance(js, str) else js for js in json_strings]
        self.data = pd.DataFrame(records)
        return self

    def extract_features(self):
        if self.data is None:
            raise ValueError("Training data has not been fetched yet. Please fetch and try again.")
        excluded = ["temp", "temp_max", "temp_min", "feels_like", "temp_pollution_ratio"]
        wanted_features = [col for col in self.data.columns if col not in excluded]
        self.training_data = self.data[wanted_features]
        self.target = self.data["temp"]
        return self

    def train(self):
        if self.training_data is None or self.training_data.empty:
            raise ValueError("No training data has been extracted, please extract features.")
        if self.target is None or self.target.empty:
            raise ValueError("No target column found in the data.")

        # Drop geolocation columns if they exist
        final_df = self.training_data.drop(columns=["lat", "lon"], errors="ignore")

        # One-hot encode categoricals
        categorical_cols = ["day", "month", "year"]
        existing_cats = [c for c in categorical_cols if c in final_df.columns]
        final_df = pd.get_dummies(final_df, columns=existing_cats, drop_first=True, dtype=int)

        # Normalize numeric columns
        numerical_cols = [
            "co", "no", "o3", "aqi", "nh3", "no2", "so2", "pm10", "pm2_5",
            "clouds", "sunrise", "sunset", "humidity", "pressure", "wind_deg",
            "sea_level", "grnd_level", "visibility", "pollution_weather_index",
            "wind_pollution_clearance", "wind_speed"
        ]
        self.scaler = StandardScaler()
        num_existing = [c for c in numerical_cols if c in final_df.columns]
        final_df[num_existing] = self.scaler.fit_transform(final_df[num_existing])

        # Train model
        model = RandomForestRegressor(n_estimators=55, random_state=42)
        model.fit(final_df, self.target)

        # Save artifacts
        self.model = model
        self.feature_names = final_df.columns.tolist()
        self.num_cols = num_existing
        return self

    def predict(self, values: dict) -> float:
        if not self.model:
            raise ValueError("Model not found. Train or load a model first.")
        if not self.feature_names:
            raise ValueError("Feature names are missing. Train the model first.")

        # Validate features
        missing = [f for f in self.feature_names if f not in values]
        extra = [f for f in values if f not in self.feature_names]

        if missing:
            raise ValueError(
                f"Missing features: {missing}. "
                f"You must include ALL of: {self.feature_names}"
            )
        if extra:
            raise ValueError(
                f"Unexpected features: {extra}. "
                f"Expected only: {self.feature_names}"
            )

        # Build DataFrame in correct order
        X = pd.DataFrame([[values[f] for f in self.feature_names]], columns=self.feature_names)

        # Apply scaling
        if self.scaler and self.num_cols:
            X[self.num_cols] = self.scaler.transform(X[self.num_cols])

        pred = self.model.predict(X)
        return float(pred[0])

    def save_model(self):
        path = f"./models/{MODEL_NAME}{int(datetime.datetime.now().timestamp()*1000)}.pkl"
        with open(path, "wb") as f:
            pickle.dump(self.model, f)
        return self