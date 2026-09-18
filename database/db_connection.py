import psycopg2
from load_dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env file

def get_lina_connection():
    """Creates and returns a database connection."""
    try:
        POSTGRES_CONFIG = {
            'dbname': 'models_price',
            'user': os.getenv('user'),
            'password': os.getenv('password'),
            'host': os.getenv('host'),
            'port': os.getenv('port')
        }
        
        # Connect to the database
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        
        print("Database connection established successfully!")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None