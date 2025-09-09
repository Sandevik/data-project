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
  processed_weather_ingestion_data_uuid UUID NOT NULL REFERENCES processed_weather_ingestion_data(uuid) ON DELETE CASCADE,
  processed_air_quality_ingestion_data_uuid UUID NOT NULL REFERENCES processed_air_quality_ingestion_data(uuid) ON DELETE CASCADE,
  json_data JSONB,
  combined_timestamp BIGINT NOT NULL,
  UNIQUE (processed_weather_ingestion_data_uuid, processed_air_quality_ingestion_data_uuid, uuid)
);