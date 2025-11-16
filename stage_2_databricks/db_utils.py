"""
Databricks SQL Helper Utilities
Fully patched & Railway-ready.
"""

import os
from datetime import datetime
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# Load Databricks Credentials
# ============================================================
DATABRICKS_SERVER = os.getenv("DATABRICKS_SERVER")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


def validate_env():
    missing = []
    if not DATABRICKS_SERVER:
        missing.append("DATABRICKS_SERVER")
    if not DATABRICKS_HTTP_PATH:
        missing.append("DATABRICKS_HTTP_PATH")
    if not DATABRICKS_TOKEN:
        missing.append("DATABRICKS_TOKEN")

    if missing:
        raise EnvironmentError(
            f"❌ Missing Databricks environment variables: {', '.join(missing)}"
        )


validate_env()


# ============================================================
# Stable SQL Connect
# ============================================================
def get_conn():
    """
    Returns a fresh Databricks SQL connector instance.
    """
    return sql.connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )


# ============================================================
# Detect Current Catalog & Schema
# ============================================================
def detect_namespace():
    """
    Returns tuple: (catalog, schema)
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT current_catalog(), current_schema()")
    catalog, schema = cur.fetchall()[0]

    cur.close()
    conn.close()

    return catalog, schema


# ============================================================
# Upload Parsed Records
# ============================================================
def upload_parsed_records(file_records, table_name="parsed_files"):
    """
    Upload parsed info as rows into a Databricks table.
    file_records = [
        {
            "file_name": "...",
            "file_type": "...",
            "content": "...text...",
        }
    ]
    """

    catalog, schema = detect_namespace()
    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"

    conn = get_conn()
    cur = conn.cursor()

    # CREATE TABLE
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            file_name STRING,
            file_type STRING,
            content STRING,
            parsed_at TIMESTAMP
        )
    """)

    # INSERT
    insert_sql = f"""
        INSERT INTO {full_table} (file_name, file_type, content, parsed_at)
        VALUES (?, ?, ?, ?)
    """

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    rows = [
        (rec["file_name"], rec["file_type"], rec["content"], now)
        for rec in file_records
    ]

    cur.executemany(insert_sql, rows)

    cur.close()
    conn.close()

    print(f"✅ Uploaded {len(rows)} rows to {full_table}")


# ============================================================
# List Tables
# ============================================================
def list_tables():
    catalog, schema = detect_namespace()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"SHOW TABLES IN `{catalog}`.`{schema}`")
    result = cur.fetchall()

    tables = [row[1] for row in result]

    cur.close()
    conn.close()

    return tables


# ============================================================
# Preview Table
# ============================================================
def preview_table(table_name, limit=20):
    catalog, schema = detect_namespace()
    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM {full_table} LIMIT {limit}")
    rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]

    cur.close()
    conn.close()

    return columns, rows


# ============================================================
# Delete
# ============================================================
def drop_table(table_name):
    catalog, schema = detect_namespace()
    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {full_table}")

    cur.close()
    conn.close()

    return True

