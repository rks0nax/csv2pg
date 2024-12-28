from contextlib import contextmanager
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

@contextmanager
def get_db():
  conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT") or 5432,
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
  )
  try:
    yield conn
  finally:
    conn.close()

def get_all_schemas(conn: psycopg2.extensions.connection) -> list[tuple[str]]:
    cursor = conn.cursor()
    cursor.execute("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;")
    # Exclude the default schemas
    default_schemas = ('information_schema', 'pg_catalog', 'pg_toast')
    schemas = [schema for schema in cursor.fetchall() if schema[0] not in default_schemas]
    return schemas

def get_all_tables(conn: psycopg2.extensions.connection, schema: str) -> list[tuple[str]]:
    cursor = conn.cursor()
    cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}' ORDER BY table_name;")
    tables = cursor.fetchall()
    return tables

def truncate_table(conn: psycopg2.extensions.connection, schema: str, table: str):
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {schema}.{table};")
    conn.commit()
    cursor.close()

def get_all_columns(conn: psycopg2.extensions.connection, schema: str, table: str) -> list[tuple[str, str]]:
    cursor = conn.cursor()
    cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}';")
    columns = cursor.fetchall()
    return columns