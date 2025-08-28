# quick_model_setup.py - Create basic models to get the system working
import pickle
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler, LabelEncoder
from pathlib import Path
from datetime import datetime

print("🤖 Creating basic ML models for SWAB Platform...")

# Create models directory
models_dir = Path("./ml-model/models")
models_dir.mkdir(exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# 1. Create a simple AQI predictor
print("📊 Creating AQI predictor...")
aqi_model = RandomForestRegressor(n_estimators=50, random_state=42)

# Train with dummy data (replace with real data later)
X_dummy = np.random.randn(100, 4)  # temperature, humidity, pressure, wind_speed
y_dummy = np.random.randint(1, 6, 100)  # AQI values 1-5

aqi_model.fit(X_dummy, y_dummy)

with open(models_dir / f"aqi_predictor_{timestamp}.pkl", "wb") as f:
    pickle.dump(aqi_model, f)
print(f"✅ Saved aqi_predictor_{timestamp}.pkl")

# 2. Create weather scaler
print("📏 Creating weather scaler...")
scaler = StandardScaler()
scaler.fit(X_dummy)

with open(models_dir / f"weather_scaler_{timestamp}.pkl", "wb") as f:
    pickle.dump(scaler, f)
print(f"✅ Saved weather_scaler_{timestamp}.pkl")

# 3. Create label encoder for risk categories
print("🏷️ Creating risk label encoder...")
label_encoder = LabelEncoder()
risk_labels = ["Low", "Medium", "High", "Very High"] * 25  # 100 samples
label_encoder.fit(risk_labels)

with open(models_dir / f"risk_label_encoder_{timestamp}.pkl", "wb") as f:
    pickle.dump(label_encoder, f)
print(f"✅ Saved risk_label_encoder_{timestamp}.pkl")

# 4. Create health risk classifier (optional for now)
print("🏥 Creating health risk classifier...")
from sklearn.ensemble import GradientBoostingClassifier

health_classifier = GradientBoostingClassifier(n_estimators=50, random_state=42)
health_classifier.fit(X_dummy, label_encoder.transform(risk_labels))

with open(models_dir / f"health_risk_classifier_{timestamp}.pkl", "wb") as f:
    pickle.dump(health_classifier, f)
print(f"✅ Saved health_risk_classifier_{timestamp}.pkl")

# 5. Create comfort index predictor
print("😌 Creating comfort predictor...")
comfort_model = RandomForestRegressor(n_estimators=50, random_state=42)
comfort_scores = np.random.randint(20, 90, 100)  # Comfort scores 20-90
comfort_model.fit(X_dummy, comfort_scores)

with open(models_dir / f"comfort_index_predictor_{timestamp}.pkl", "wb") as f:
    pickle.dump(comfort_model, f)
print(f"✅ Saved comfort_index_predictor_{timestamp}.pkl")

print(f"\n🎉 Basic models created successfully!")
print(f"📁 Models saved in: {models_dir}")
print(f"\n🚀 Now you can start the services:")
print("   docker-compose up -d")
print("\n🔗 Service URLs:")
print("   Dashboard: http://localhost:8084")
print("   API: http://localhost:8083")
print("   Combined ML: http://localhost:8085")
print("   Data Ingestion: http://localhost:8080")
print("   Data Processing: http://localhost:8081")
