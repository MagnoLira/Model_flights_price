import psycopg2


def get_lina_connection():
    """Creates and returns a database connection."""
    try:
        POSTGRES_CONFIG = {
            'dbname': 'models_price',
            'user': 'postgres',
            'password': 'postgres',
            'host': '192.168.0.33',
            'port': '5432'
        }
        
        # Connect to the database
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        
        print("Database connection established successfully!")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None