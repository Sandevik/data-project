import os
import json
import logging
from time import time
from requests import get, RequestException, Response
import psycopg2
from abc import ABC, abstractmethod

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataIngestor(ABC):
    cities: list[dict]
    api_key: str
    def __init__(self, cities: list[dict]):
        self.cities = cities
        self.api_key = os.getenv("WEATHER_API_KEY")
        self.conn = psycopg2.connect(os.getenv("DATABASE_URL"))

    @abstractmethod
    def fetch_data(self):
        pass

    @abstractmethod
    def flatten_data(self):
        pass

    @abstractmethod
    def validate_data(self):
        pass

    @abstractmethod
    def save(self):
        pass
