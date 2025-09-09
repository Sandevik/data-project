from .DataProcessor import DataProcessor
from typing import TypedDict
import pandas as pd
from time import time

class UnProcessedData(TypedDict):
    uuid: str 
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
    city_name: str 
    ingestion_timestamp: int 
    data_source: str
    timestamp: int



class WeatherDataProcessor(DataProcessor):

    unprocessed_data: UnProcessedData | None = None
    processed_data: pd.DataFrame | None = None

    def __init__(self, timestamp: int = int(time())):
        super().__init__()
        self.timestamp = timestamp


    def fetch_data(self) -> "WeatherDataProcessor":
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM weather_ingestion_data ORDER BY ingestion_timestamp DESC;")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in rows]
        self.unprocessed_data = data
        cursor.close()
        return self

    def process_data(self) -> pd.DataFrame:
        if not self.unprocessed_data:
            raise ValueError("No data to process. Fetch data first.")
        df = pd.DataFrame(self.unprocessed_data)

        df = pd.get_dummies(data=df, columns=["weather_main", "weather_description", "city_name"], drop_first=True, dtype=int)

        df.drop_duplicates(inplace=True)
        self.processed_data = df
        return self    

    def save_data(self) -> "WeatherDataProcessor":
        if self.processed_data is None or self.processed_data.empty:
            raise ValueError("No data to save. Process data first.")

        cursor = self.conn.cursor()

        self.processed_data.drop(columns=["data_source"], inplace=True, errors='ignore')
        self.processed_data.columns = self.processed_data.columns.str.replace(" ", "_")
        
        insert_query = """
        INSERT INTO processed_weather_ingestion_data (
            ingestion_data_uuid,
            json_data,
            processed_timestamp
        ) VALUES (%s, %s, %s)
        ON CONFLICT (ingestion_data_uuid) DO UPDATE
        SET json_data = EXCLUDED.json_data,
            processed_timestamp = EXCLUDED.processed_timestamp
        RETURNING json_build_object(
            'ingestion_data_uuid', ingestion_data_uuid,
            'json_data', json_data,
            'processed_timestamp', processed_timestamp
        );
        """
        res = []
        for _, row in self.processed_data.iterrows():
            d_row = row.drop(labels=["uuid"])
            values = (
                row["uuid"],
                d_row.to_json(),
                self.timestamp
            )
            cursor.execute(insert_query, values)
            res.append(cursor.fetchone())
        

        self.result = res
        self.conn.commit()
        cursor.close()
        return self
