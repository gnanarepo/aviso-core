import psycopg2
from psycopg2 import OperationalError

url = "postgres://waqas:waqas79543@localhost:5502/postgres"

try:
    print("Attempting to connect to the database...")
    conn = psycopg2.connect(url)
    print("Connection successful.")
    conn.close()
except OperationalError as e:
    print("Connection failed:", e)