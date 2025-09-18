import os
import json
import logging
from time import time
from requests import get, RequestException, Response
import psycopg2
from psycopg2.extras import execute_values
from .DataIngestor import DataIngestor
import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherDataIngestor(DataIngestor):
    timestamp: int
    def __init__(self, cities: list[dict], timestamp: int = int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)):
        super().__init__(cities)
        self.timestamp = timestamp
    
    def fetch_data(self, lat, lon, city_name) -> dict:
        """Fetch latest weather data from OpenWeatherMap API"""
        try:
            url = "https://api.openweathermap.org/data/2.5/weather"
            params = {
                "lat": lat,
                "lon": lon,
                "appid": self.api_key,
                "units": "metric"
            }

            res: Response = get(url, params=params, timeout=60)
            res.raise_for_status()
            data = res.json()

            data["ingestion_timestamp"] = self.timestamp
            data["city_name"] = city_name
            data["data_source"] = "openweathermap"
            logger.info(f"Retrieved weather info for {city_name}")
            return data
        except RequestException as err:
            logger.error(f"Failed to retrieve weather info for {city_name}: {err}")
            raise
        except Exception as err:
            logger.error(f"Unexpected error for {city_name}: {err}")
            raise

    def flatten_data(self, data: dict) -> dict:
        """Flatten OpenWeatherMap response into database-ready dict"""
        flat = {}
        flat["lat"] = data.get("coord", {}).get("lat")
        flat["lon"] = data.get("coord", {}).get("lon")

        main = data.get("main", {})
        flat["temp"] = main.get("temp")
        flat["feels_like"] = main.get("feels_like")
        flat["temp_min"] = main.get("temp_min")
        flat["temp_max"] = main.get("temp_max")
        flat["pressure"] = main.get("pressure")
        flat["humidity"] = main.get("humidity")
        flat["sea_level"] = main.get("sea_level", 0)
        flat["grnd_level"] = main.get("grnd_level", 0)

        flat["visibility"] = data.get("visibility", 0)

        wind = data.get("wind", {})
        flat["wind_speed"] = wind.get("speed", 0.0)
        flat["wind_deg"] = wind.get("deg", 0.0)

        flat["clouds"] = data.get("clouds", {}).get("all", 0)

        weather = data.get("weather", [{}])[0]
        flat["weather_main"] = weather.get("main", "")
        flat["weather_description"] = weather.get("description", "")

        sys_data = data.get("sys", {})
        flat["sunrise"] = sys_data.get("sunrise", 0)
        flat["sunset"] = sys_data.get("sunset", 0)

        flat["city_name"] = data.get("name", "")
        flat["ingestion_timestamp"] = data.get("ingestion_timestamp")
        flat["data_source"] = data.get("data_source", "openweathermap")

        return flat

    def validate_data(self, data: dict) -> dict:
        """Validate required fields"""
        required_fields = [
            "lat", "lon",
            "temp", "feels_like", "temp_min", "temp_max", "pressure", "humidity",
            "sea_level", "grnd_level",
            "visibility",
            "wind_speed", "wind_deg",
            "clouds",
            "weather_main", "weather_description",
            "sunrise", "sunset",
            "city_name", "ingestion_timestamp", "data_source"
        ]

        for field in required_fields:
            if field not in data or data[field] is None:
                raise ValueError(f"Missing or None field: {field}")

        # Simple temperature sanity check
        if not (-50 <= data["temp"] <= 50):
            raise ValueError(f"Temperature out of range for {data['city_name']}: {data['temp']}")

        return data

    def save(self, weather_data):
        """Save weather data to DB; accepts a dict or list of dicts"""
        try:
            if isinstance(weather_data, dict):
                weather_data = [weather_data]

            cur = self.conn.cursor()
            query = """
            INSERT INTO weather_ingestion_data (
                lat, lon, temp, feels_like, temp_min, temp_max,
                pressure, humidity, sea_level, grnd_level, visibility,
                wind_speed, wind_deg, clouds, weather_main, weather_description,
                sunrise, sunset, city_name, ingestion_timestamp, data_source
            ) VALUES %s
            """

            values = [
                (
                    d["lat"], d["lon"], d["temp"], d["feels_like"], d["temp_min"], d["temp_max"],
                    d["pressure"], d["humidity"], d["sea_level"], d["grnd_level"], d["visibility"],
                    d["wind_speed"], d["wind_deg"], d["clouds"], d["weather_main"], d["weather_description"],
                    d["sunrise"], d["sunset"], d["city_name"], d["ingestion_timestamp"], d["data_source"]
                )
                for d in weather_data
            ]

            # Use execute_values for bulk insert
            execute_values(cur, query, values)
            self.conn.commit()
            logger.info(f"[Ingestion]: Saved {len(weather_data)} records to database")
        except Exception as err:
            logger.error(f"[Ingestion]: Error saving data to database: {err}")
            self.conn.rollback()

    def process_cities(self):
        """Fetch, flatten, validate, and save data for all cities"""
        logger.info("SWAB Weather Data Ingestion Starting...")
        results = []

        for i, city in enumerate(self.cities, 1):
            logger.info(f"Processing city {i}/{len(self.cities)}: {city['name']}")
            try:
                raw_data = self.fetch_data(city["lat"], city["lon"], city["name"])
                flat = self.flatten_data(raw_data)
                flat = self.validate_data(flat)
                self.save(flat)

                results.append({
                    "city": city["name"],
                    "status": "success",
                    "timestamp": flat["ingestion_timestamp"],
                    "temperature": flat["temp"],
                    "weather": flat["weather_description"]
                })

            except Exception as e:
                logger.error(f"Failed to process {city['name']}: {e}")
                results.append({"city": city["name"], "status": "error", "error": str(e)})

        # Summary
        success_count = sum(1 for r in results if r["status"] == "success")
        logger.info(f"INGESTION SUMMARY: {success_count}/{len(self.cities)} cities successful")
        return results