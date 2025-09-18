import logging
from time import time
from requests import get, RequestException, Response
from psycopg2.extras import execute_values
from .DataIngestor import DataIngestor
import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AirQualityIngestor(DataIngestor):
    timestamp: int
    def __init__(self, cities, timestamp: int = int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)):
        super().__init__(cities)
        self.timestamp = timestamp

    def fetch_data(self, lat, lon, city_name):
        """Fetch air quality data from OpenWeatherMap API"""
        try:
            url = "https://api.openweathermap.org/data/2.5/air_pollution"
            params = {
                "lat": lat,
                "lon": lon,
                "appid": self.api_key,
                "units": "metric"
            }

            res: Response = get(url, params=params, timeout=60)
            res.raise_for_status()
            data = res.json()

            # Lägg på metadata
            data["ingestion_timestamp"] = self.timestamp
            data["city_name"] = city_name
            data["data_source"] = "openweathermap_air_quality"

            logger.info(f"Retrieved air quality info for {city_name}")
            return data

        except RequestException as err:
            logger.error(f"Failed to retrieve air quality info for {city_name}: {err}")
            raise
        except Exception as err:
            logger.error(f"Unexpected error for {city_name}: {err}")
            raise

    def flatten_data(self, data: dict) -> dict:
        """Flatten OpenWeatherMap air pollution response into database-ready dict"""
        flat = {}

        # Koordinater
        flat["lat"] = data.get("coord", {}).get("lat")
        flat["lon"] = data.get("coord", {}).get("lon")

        # Första elementet i listan
        first_entry = data.get("list", [{}])[0]

        # Huvuddata
        main = first_entry.get("main", {})
        flat["aqi"] = main.get("aqi", 0)

        # Komponenter
        components = first_entry.get("components", {})
        flat["co"] = components.get("co", 0.0)
        flat["no"] = components.get("no", 0.0)
        flat["no2"] = components.get("no2", 0.0)
        flat["o3"] = components.get("o3", 0.0)
        flat["so2"] = components.get("so2", 0.0)
        flat["pm2_5"] = components.get("pm2_5", 0.0)
        flat["pm10"] = components.get("pm10", 0.0)
        flat["nh3"] = components.get("nh3", 0.0)

        # Tidsstämpel från API
        flat["timestamp"] = first_entry.get("dt", 0)

        # Metadata
        flat["city_name"] = data.get("city_name", "")
        flat["ingestion_timestamp"] = data.get("ingestion_timestamp")
        flat["data_source"] = data.get("data_source", "openweathermap_air_quality")

        return flat

    def validate_data(self, data: dict) -> dict:
        """Validate required fields for air quality data"""
        required_fields = [
            "lat", "lon",
            "aqi",
            "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3",
            "timestamp", "ingestion_timestamp", "city_name", "data_source"
        ]

        for field in required_fields:
            if field not in data or data[field] is None:
                raise ValueError(f"Missing or None field: {field}")

        # AQI sanity check (1-5)
        if not (1 <= data["aqi"] <= 5):
            raise ValueError(f"AQI out of range: {data['aqi']}")

        return data

    def save(self, air_quality_data):
        """Save air quality data to DB; accepts a dict or list of dicts"""
        try:
            if isinstance(air_quality_data, dict):
                air_quality_data = [air_quality_data]

            cur = self.conn.cursor()
            query = """
            INSERT INTO air_quality_ingestion_data (
                lat, long, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3,
                city_name, ingestion_timestamp, data_source
            ) VALUES %s
            """

            values = [
                (
                    d["lat"], d["lon"], d["aqi"],
                    d["co"], d["no"], d["no2"], d["o3"], d["so2"],
                    d["pm2_5"], d["pm10"], d["nh3"],
                    d["city_name"],
                    d["ingestion_timestamp"],
                    d["data_source"]
                )
                for d in air_quality_data
            ]

            execute_values(cur, query, values)
            self.conn.commit()
            logger.info(f"[Ingestion]: Saved {len(air_quality_data)} records to database")
        except Exception as err:
            logger.error(f"[Ingestion]: Error saving data to database: {err}")
            self.conn.rollback()

    def process_cities(self):
        """Fetch, flatten, validate, and save air quality data for all cities"""
        logger.info("SWAB Air Quality Data Ingestion Starting...")
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
                    "aqi": flat["aqi"]
                })
            except Exception as e:
                logger.error(f"Failed to process {city['name']}: {e}")
                results.append({"city": city["name"], "status": "error", "error": str(e)})

        success_count = sum(1 for r in results if r["status"] == "success")
        logger.info(f"INGESTION SUMMARY: {success_count}/{len(self.cities)} cities successful")
        return results