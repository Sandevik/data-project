
from fastapi import FastAPI
from time import time
from ingestions.WeatherDataIngestion import WeatherDataIngestion

app = FastAPI()


@app.get("/ingest")
async def route():
    try: 
        weather = WeatherDataIngestion().process_cities()

        """ aq = AirQualityIngestion()
        aq_results = aq.process_all_cities() """

        return {
            "status": "Success",
            "weather": weather,
           """  "air_quality": aq_results, """
            "timestamp": int(time())
        }
    except Exception as err:
        return {
            "status": "Error",
            "message": str(err),
            "timestamp": int(time())
        }