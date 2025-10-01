import os
import psycopg2

def get_db_connection():
    conn = psycopg2.connect(
        dbname=os.environ.get("DB_NAME"),
        user=os.environ.getenv("DB_USER"),
        password=os.environ.getenv("DB_PASSWORD"),
        host=os.environ.getenv("DB_HOST"),
        port=os.environ.getenv("DB_PORT")
    )
    return conn