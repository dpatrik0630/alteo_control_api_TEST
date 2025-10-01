import psycopg2

def get_db_connection():
    conn = psycopg2.connect(
        dbname="aramut",
        user="postgres",
        password="mj46-pr23",
        host="100.115.164.70",
        port="5432"
    )
    return conn