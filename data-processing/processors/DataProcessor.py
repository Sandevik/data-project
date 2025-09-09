from abc import ABC, abstractmethod
import psycopg2
import os

class DataProcessor(ABC):
    conn: psycopg2.extensions.connection
    def __init__(self):
        self.conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    
    @abstractmethod
    def fetch_data(self) -> None:
        pass

    @abstractmethod
    def process_data(self) -> None:
        pass

    @abstractmethod
    def save_data(self) -> None:
        pass
    



