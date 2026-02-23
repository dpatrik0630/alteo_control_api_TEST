import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool

# Globális pool
db_pool = ThreadedConnectionPool(
    minconn=5,
    maxconn=20,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)

def get_db_connection():
    return db_pool.getconn()

def release_db_connection(conn):
    db_pool.putconn(conn)