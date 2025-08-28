import os
from requests import Response, get, RequestException
from time import time
from logger import logger
import json

class WeatherDataIngestion:
    def __init__ (self):
        self.api_key = os.getenv("WEATHER_API_KEY")

        self.cities = [
            {"name": "Stockholm", "lat": 59.3293, "lon": 18.0686},
            {"name": "Gothenburg", "lat": 57.7089, "lon": 11.9746},
            {"name": "Malmö", "lat": 55.6059, "lon": 13.0007},
            {"name": "Uppsala", "lat": 59.8586, "lon": 17.6389},
            {"name": "Linköping", "lat": 58.4108, "lon": 15.6214},
            {"name": "Örebro", "lat": 59.2741, "lon": 15.2066},
            {"name": "Västerås", "lat": 59.6099, "lon": 16.5448},
            {"name": "Norrköping", "lat": 58.5877, "lon": 16.1924},
        ]
    
    def fetch_data(self, lat, long, city_name) -> dict:
        """Fetches the latest weather data from OpenWeatherMap API"""
        try:
            url = "https://api.openweathermap.org/data/2.5/weather"
            params = {
                "lat": lat,
                "long": long,
                "appid": self.api_key,
                "units": "metric"
            }

            res: Response = get(url, params=params, timeout=60)
            res.raise_for_status()
            data = res.json()

            data["ingenstion_timestamp"] = time()
            data["city_name"] = city_name
            data["data_source"] = "openweathermap"
            logger.info(f"Retrieved weather info for {city_name}")
            return data
        except RequestException as err:
            logger.error(f"Failed to retrive weather info for {city_name}, {err}")
            raise
        except Exception as err:
            logger.error(f"Unexpected error for {city_name}, {err}")
    
    def validate_data(self, data) -> None: 
        required_fields = ["main", "weather", "wind", "sys", "name"]

        for field in required_fields:
            if field not in data:
                raise ValueError(f"Required field missing, {field}")
            
        temp = data["main"].get("temp")
        if temp is None or not (-50 >= temp >= 50):
            raise ValueError(f"Temprature invalidation error for {data["city_name"]}, temp: {temp}")
        
        logger.info(f"Validation for data succeeded for {data["city_name"]}")
        return

    def save_to_json(self, weather_data):
        try:
            os.makedirs("/app/data", exist_ok=True)

            timestamp = str(int(time()))
            file_name = f"/app/data/weather_{weather_data["city_name"]}_{timestamp}.json"
            with open(file_name, "w") as f:
                json.dump(weather_data, indent=2)
            f.close()
            logger.info(f"Saved {weather_data["name"]} data to {file_name}")
        except Exception as err: 
            logger.error(f"Error saving JSON data to, {err}")
        
    def process_cities(self):
        """Fetches and saves data for all cities"""
        print("SWAB Weather Data Ingestion Starting...")
        print("-" * 50)
        
        results = []
        successful = 0


        for i, city in enumerate(self.cities, 1):
            print(f"\n Processing city {i}/{len(self.cities)}: {city['name']}")

            try:
                # Fetch data
                weather_data = self.fetch_data(
                    city["lat"], city["lon"], city["name"]
                )

                # Validate data
                self.validate_data(weather_data)

                # Save to JSON (later we'll save to BigQuery)
                filename = self.save_to_json(weather_data)

                # Display summary
                temp = weather_data["main"]["temp"]
                desc = weather_data["weather"][0]["description"]
                humidity = weather_data["main"]["humidity"]

                print(f"  Temperature: {temp}°C")
                print(f"  Weather: {desc}")
                print(f"  Humidity: {humidity}%")

                results.append(
                    {
                        "city": city["name"],
                        "status": "success",
                        "timestamp": weather_data["ingestion_timestamp"],
                        "temperature": temp,
                        "weather": desc,
                        "filename": filename,
                    }
                )

                successful += 1

            except Exception as e:
                logger.error(f" Failed to process {city['name']}: {e}")
                results.append(
                    {"city": city["name"], "status": "error", "error": str(e)}
                )

        # Summary
        print(f"\n INGESTION SUMMARY")
        print("-" * 50)
        print(f" Successful: {successful}/{len(self.cities)} cities")
        print(f" Failed: {len(self.cities) - successful}/{len(self.cities)} cities")

        if successful > 0:
            print(f"\n Data saved to /app/data/ directory")
            print(f" Next: Set up data processing pipeline")

        return results


