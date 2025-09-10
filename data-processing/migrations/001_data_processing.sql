CREATE TABLE IF NOT EXISTS processed_weather_ingestion_data (
  ingestion_data_uuid UUID NOT NULL PRIMARY KEY REFERENCES weather_ingestion_data(uuid) ON DELETE CASCADE,
  json_data JSONB,
  processed_timestamp BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_air_quality_ingestion_data (
  ingestion_data_uuid UUID NOT NULL PRIMARY KEY REFERENCES air_quality_ingestion_data(uuid) ON DELETE CASCADE,
  json_data JSONB,
  processed_timestamp BIGINT NOT NULL
);


CREATE TABLE IF NOT EXISTS combined_processed_ingestion_data (
  uuid UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  weather_ingestion_uuid UUID NOT NULL REFERENCES weather_ingestion_data(uuid) ON DELETE CASCADE,
  aq_ingestion_uuid UUID NOT NULL REFERENCES air_quality_ingestion_data(uuid) ON DELETE CASCADE,
  json_data JSONB,
  ingestion_timestamp BIGINT NOT NULL,
  UNIQUE (weather_ingestion_uuid, aq_ingestion_uuid)
);