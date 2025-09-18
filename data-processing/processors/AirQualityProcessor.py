from .DataProcessor import DataProcessor
from typing import TypedDict
import pandas as pd
from time import time
import datetime

class UnProcessedData(TypedDict):
    uuid: str 
    lat: float 
    lon: float 
    aqi: int
    co: float
    no: float
    no2: float
    o3: float
    so2: float
    pm2_5: float
    pm10: float
    nh3: float
    ingestion_timestamp: int
    data_source: str



class AirQualityDataProcessor(DataProcessor):

    unprocessed_data: UnProcessedData | None = None
    processed_data: pd.DataFrame | None = None

    def __init__(self, timestamp: int = int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)):
        super().__init__()
        self.timestamp = timestamp


    def fetch_data(self) -> "AirQualityDataProcessor":
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM air_quality_ingestion_data ORDER BY ingestion_timestamp DESC;")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in rows]
        self.unprocessed_data = data
        cursor.close()
        return self

    def process_data(self) -> "AirQualityDataProcessor":
        if not self.unprocessed_data:
            raise ValueError("No data to process. Fetch data first.")
        df = pd.DataFrame(self.unprocessed_data)
        df = pd.get_dummies(columns=["city_name"] , data=df, drop_first=True, dtype=int)
        
        df["month"] = pd.to_datetime(df["ingestion_timestamp"], unit="ms").dt.month
        df["day"] = pd.to_datetime(df["ingestion_timestamp"], unit="ms").dt.day
        df["year"] = pd.to_datetime(df["ingestion_timestamp"], unit="ms").dt.year
        df.drop_duplicates(inplace=True)
        self.processed_data = df
        return self    

    def save_data(self) -> "AirQualityDataProcessor":
        if self.processed_data is None or self.processed_data.empty:
            raise ValueError("No data to save. Process data first.")

        cursor = self.conn.cursor()

        self.processed_data.drop(columns=["data_source"], inplace=True, errors='ignore')
        self.processed_data.columns = self.processed_data.columns.str.replace(" ", "_")
        
        insert_query = """
        INSERT INTO processed_air_quality_ingestion_data (
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
            d_row = row.drop(labels=["uuid", "ingestion_timestamp"])
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
