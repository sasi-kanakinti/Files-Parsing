"""
Databricks SQL Helper Utilities
Fully patched & Railway-ready with Base64-safe content storage.
"""

import os
import base64
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
    """Returns a fresh Databricks SQL connector."""
    return sql.connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )


# ============================================================
# Detect Catalog / Schema
# ============================================================
def detect_namespace():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT current_catalog(), current_schema()")
    catalog, schema = cur.fetchall()[0]

    cur.close()
    conn.close()

    return catalog, schema


# ============================================================
# Upload Parsed Output (Base64 SAFE)
# ============================================================
def upload_parsed_records(file_records, table_name="parsed_files"):
    catalog, schema = detect_namespace()
    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"

    conn = get_conn()
    cur = conn.cursor()

    # Create table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            file_name STRING,
            file_type STRING,
            content_base64 STRING,
            parsed_at TIMESTAMP
        )
    """)

    insert_sql = f"""
        INSERT INTO {full_table}
        (file_name, file_type, content_base64, parsed_at)
        VALUES (%s, %s, %s, %s)
    """

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for rec in file_records:
        encoded = base64.b64encode(
            rec["content"].encode("utf-8")
        ).decode("utf-8")

        row = (
            rec["file_name"],
            rec["file_type"],
            encoded,
            now
        )

        # IMPORTANT: use execute() for each row
        cur.execute(insert_sql, row)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Uploaded {len(file_records)} rows → {full_table}")


# ============================================================
# List Tables
# ============================================================
def list_tables():
    catalog, schema = detect_namespace()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"SHOW TABLES IN `{catalog}`.`{schema}`")
    tables = [row[1] for row in cur.fetchall()]

    cur.close()
    conn.close()

    return tables


# ============================================================
# Preview Table (AUTO-DECODE BASE64)
# ============================================================
def preview_table(table_name, limit=50):
    catalog, schema = detect_namespace()
    full_table = f"`{catalog}`.`{schema}`.`{table_name}`"

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM {full_table} LIMIT {limit}")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    # Decode Base64 content if column exists
    decoded_rows = []
    for row in rows:
        row = list(row)
        if "content_base64" in columns:
            idx = columns.index("content_base64")
            try:
                row[idx] = base64.b64decode(row[idx]).decode("utf-8")
            except:
                row[idx] = "[Base64 Decode Error]"
        decoded_rows.append(row)

    cur.close()
    conn.close()

    return columns, decoded_rows


# ============================================================
# Delete Table
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
