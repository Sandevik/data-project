from abc import ABC, abstractmethod
import psycopg2 
import os

class Trainer(ABC):
    conn: psycopg2.extensions.connection
    
    def __init__(self):
        self.conn = psycopg2.connect(os.getenv("DATABASE_URL"))

    @abstractmethod
    def fetch_training_data(self):
        pass

    @abstractmethod
    def extract_features(self):
        pass

    @abstractmethod
    def train(self):
        pass

    @abstractmethod
    def save_model(self):
        pass

