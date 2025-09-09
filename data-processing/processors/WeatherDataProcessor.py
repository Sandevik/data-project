from .DataProcessor import DataProcessor
from typing import TypedDict
import pandas as pd

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

    def __init__(self):
        super().__init__()


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

        # This is commented due to it being a primary key in the table,
        # It should be removed in the training phase instead
        #df.drop(columns=["uuid"], inplace=True)
        
        # Normalize values
        for col in df.columns:
            if df[col].dtype in [int, float]:
                df[col] = (df[col] - df[col].mean()) / df[col].std()

        # Get dummies for categorical variables after normatization to avoid dummy variable trap
        df = pd.get_dummies(data=df, columns=["weather_main", "weather_description", "city_name", "data_source"], drop_first=True, dtype=int)
        print(df.head())

        #df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)

        print(df.head())
        return df
    

    def save_data(self) -> None:
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO processed_weather_ingestion_data (uuid, lat, lon, temp, feels_like, temp_min, temp_max, pressure, humidity, sea_level, grnd_level, visibility, wind_speed, wind_deg, clouds, weather_main, weather_description, sunrise, sunset, city_name, ingestion_timestamp, data_source, timestamp) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", ())
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in rows]
        self.unprocessed_data = data
        cursor.close()
