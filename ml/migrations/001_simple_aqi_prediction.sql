CREATE TABLE IF NOT EXISTS simple_aqi_predictions (
    uuid UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    temperature DOUBLE PRECISION NOT NULL,
    pressure INT NOT NULL,
    humidity DOUBLE PRECISION NOT NULL,
    wind_speed DOUBLE PRECISION NOT NULL,

    prediction_model TEXT NOT NULL,

    predicted_aqi SMALLINT NOT NULL,
    predicted_aqi_label TEXT NOT NULL,

    timestamp BIGINT NOT NULL
);