from .DataProcessor import DataProcessor
import pandas as pd
from time import time
from typing import TypedDict
import json
from psycopg2.extras import Json

class UnProcessedData(TypedDict):
    city_name: str
    combined_timestamp: int
    weather_ingestion_uuid: str
    aq_ingestion_uuid: str
    # Weather-kolumner
    lat: float
    lon: float
    temp: float
    feels_like: float
    temp_min: float
    temp_max: float
    pressure: int
    humidity: int
    sea_level: int
    grnd_level: int
    visibility: int
    wind_speed: float
    wind_deg: float
    clouds: int
    weather_main: str
    weather_description: str
    sunrise: int
    sunset: int
    # Air Quality-kolumner
    aqi: int
    co: float
    no: float
    no2: float
    o3: float
    so2: float
    pm2_5: float
    pm10: float
    nh3: float

class CombinedDataProcessor(DataProcessor):
    unprocessed_data: list[UnProcessedData] = []
    weather_df: pd.DataFrame | None = None
    aq_df: pd.DataFrame | None = None
    processed_data: pd.DataFrame | None = None

    def __init__(self, timestamp: int = int(time())):
        super().__init__()
        self.timestamp = timestamp


    def fetch_data(self) -> "CombinedDataProcessor":
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                w.city_name,
                w.ingestion_timestamp,
                w.uuid AS weather_ingestion_uuid,
                aq.uuid AS aq_ingestion_uuid,

                -- Weather-kolumner
                w.lat,
                w.lon,
                w.temp,
                w.feels_like,
                w.temp_min,
                w.temp_max,
                w.pressure,
                w.humidity,
                w.sea_level,
                w.grnd_level,
                w.visibility,
                w.wind_speed,
                w.wind_deg,
                w.clouds,
                w.weather_main,
                w.weather_description,
                w.sunrise,
                w.sunset,

                aq.aqi,
                aq.co,
                aq.no,
                aq.no2,
                aq.o3,
                aq.so2,
                aq.pm2_5,
                aq.pm10,
                aq.nh3

            FROM weather_ingestion_data w
            JOIN air_quality_ingestion_data aq
                ON w.city_name = aq.city_name
               AND w.ingestion_timestamp = aq.ingestion_timestamp
            ORDER BY w.ingestion_timestamp DESC;
        """)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in rows]
        self.unprocessed_data = data
        cursor.close()
        return self

    def process_data(self) -> "CombinedDataProcessor":
        if len(self.unprocessed_data) == 0:
           raise ValueError("No data to process. Fetch data first.")

        merged_df = pd.DataFrame(self.unprocessed_data)

        merged_df["pollution_weather_index"] = (merged_df["aqi"] * merged_df["humidity"] / 100)

        # Temperature-pollution correlation
        merged_df["temp_pollution_ratio"] = merged_df["temp"] / (merged_df["pm2_5"] + 1)

        # Wind effect on pollution
        merged_df["wind_pollution_clearance"] = merged_df["wind_speed"] / (merged_df["aqi"] + 1)

        # Weather severity with pollution
        merged_df["environmental_stress"] = (merged_df["aqi"] + (merged_df["humidity"] > 80).astype(int) + (merged_df["wind_speed"] < 2).astype(int) + (merged_df["clouds"] > 80).astype(int))

        merged_df = pd.get_dummies(data=merged_df, columns=["weather_main", "weather_description", "city_name"], drop_first=True, dtype=int)

        merged_df.columns = merged_df.columns.str.replace(" ", "_")

        self.processed_data = merged_df
        return self    


    def save_data(self) -> "CombinedDataProcessor":
        insert_query = """
            INSERT INTO combined_processed_ingestion_data
                (weather_ingestion_uuid, aq_ingestion_uuid, json_data, ingestion_timestamp)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (weather_ingestion_uuid, aq_ingestion_uuid) DO UPDATE
                SET json_data = EXCLUDED.json_data,
                    ingestion_timestamp = EXCLUDED.ingestion_timestamp
                WHERE combined_processed_ingestion_data.uuid IS DISTINCT FROM EXCLUDED.uuid
            RETURNING json_build_object(
                'weather_ingestion_uuid', weather_ingestion_uuid,
                'aq_ingestion_uuid', aq_ingestion_uuid,
                'json_data', json_data,
                'ingestion_timestamp', ingestion_timestamp
            );
        """

        

        cursor = self.conn.cursor()
        for _, row in self.processed_data.iterrows():
            d_row = row.drop(labels=["weather_ingestion_uuid", "aq_ingestion_uuid"])
            cursor.execute(
                insert_query,
                (
                    row["weather_ingestion_uuid"],
                    row["aq_ingestion_uuid"],
                    d_row.to_json(),
                    row["ingestion_timestamp"]
                )
            )

        self.conn.commit()
        cursor.close()
        return self
