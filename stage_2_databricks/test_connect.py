"""
Simple Databricks SQL connection test
Run this locally (NOT on Railway) to verify credentials.
"""

import os
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

SERVER = os.getenv("DATABRICKS_SERVER")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
TOKEN = os.getenv("DATABRICKS_TOKEN")

if not SERVER or not HTTP_PATH or not TOKEN:
    raise EnvironmentError("Missing Databricks environment variables.")

print("🔍 Testing Databricks SQL connection...")

try:
    conn = sql.connect(
        server_hostname=SERVER,
        http_path=HTTP_PATH,
        access_token=TOKEN
    )

    cur = conn.cursor()

    cur.execute("SELECT current_catalog(), current_schema()")

    catalog, schema = cur.fetchall()[0]

    print("✅ Connected successfully!")
    print(f"📁 Catalog : {catalog}")
    print(f"📂 Schema  : {schema}")

    cur.close()
    conn.close()

except Exception as e:
    print("❌ Connection failed!")
    print("Error:", e)
