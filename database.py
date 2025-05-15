# database.py
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_gp_config():
    return {
        "host":     os.getenv("GP_HOST"),
        "dbname":   os.getenv("GP_DB"),
        "user":     os.getenv("GP_USER"),
        "password": os.getenv("GP_PASSWORD"),
        "port":     int(os.getenv("GP_PORT", 7830))
    }

def connect_to_greenplum():
    cfg = get_gp_config()
    return psycopg2.connect(**cfg)

def table_exists(conn, schema, table_name):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s)",
            (schema, table_name)
        )
        return cur.fetchone()[0]

def get_table_columns(conn, schema, table_name):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type, character_maximum_length, "
            "numeric_precision, numeric_scale, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s "
            "ORDER BY ordinal_position",
            (schema, table_name)
        )
        return cur.fetchall()
