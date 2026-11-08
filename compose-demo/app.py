import psycopg2
import os

db_host = os.getenv("DB_HOST", "db")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("POSTGRES_DB", "products")
db_user = os.getenv("POSTGRES_USER", "postgres")
db_pass = os.getenv("POSTGRES_PASSWORD", "postgres")
