"""
Databricks helpers for SQL connector-based operations.
"""

import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

try:
    from databricks import sql
except ImportError:
    sql = None


# --------------------------------------------------------------------
# Connection Info
# --------------------------------------------------------------------
DATABRICKS_SERVER = os.getenv("DATABRICKS_SERVER")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

if not DATABRICKS_TOKEN:
    raise EnvironmentError("❌ Missing DATABRICKS_TOKEN in .env")


def get_conn():
    """Create Databricks SQL connection."""
    return sql.connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )


# --------------------------------------------------------------------
# Detect Active Catalog + Schema
# --------------------------------------------------------------------
def detect_namespace():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT current_catalog(), current_schema()")
    catalog, schema = cur.fetchall()[0]

    cur.close()
    conn.close()

    return catalog, schema


# --------------------------------------------------------------------
# Upload Parsed Records
# --------------------------------------------------------------------
def upload_parsed_records(file_records, table_name="parsed_files"):
    catalog, schema = detect_namespace()

    full_table = f"{catalog}.{schema}.{table_name}"

    conn = get_conn()
    cur = conn.cursor()

    # Create table if missing
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            file_name STRING,
            file_type STRING,
            content STRING,
            parsed_at TIMESTAMP
        )
    """)

    insert_query = f"""
        INSERT INTO {full_table}
        (file_name, file_type, content, parsed_at)
        VALUES (?, ?, ?, ?)
    """

    now = datetime.utcnow().isoformat(" ")
    rows = [(r["file_name"], r["file_type"], r["content"], now) for r in file_records]

    cur.executemany(insert_query, rows)

    cur.close()
    conn.close()

    print(f"✅ Uploaded {len(rows)} rows into {full_table}")


# --------------------------------------------------------------------
# List Tables
# --------------------------------------------------------------------
def list_tables():
    catalog, schema = detect_namespace()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"SHOW TABLES IN {catalog}.{schema}")
    rows = cur.fetchall()

    # Table name is column index 1
    tables = [r[1] for r in rows if r[1]]

    cur.close()
    conn.close()

    return tables


# --------------------------------------------------------------------
# Preview Table
# --------------------------------------------------------------------
def preview_table(table_name, limit=20):
    catalog, schema = detect_namespace()
    full_table = f"{catalog}.{schema}.{table_name}"

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM {full_table} LIMIT {limit}")
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description]

    cur.close()
    conn.close()

    return columns, rows


# --------------------------------------------------------------------
# Delete Table
# --------------------------------------------------------------------
def drop_table(table_name):
    catalog, schema = detect_namespace()
    full_table = f"{catalog}.{schema}.{table_name}"

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {full_table}")

    cur.close()
    conn.close()
    return True
