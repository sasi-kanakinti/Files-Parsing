"""
Databricks helpers for both Spark and SQL connector paths.
"""

import os
import traceback
from datetime import datetime
from dotenv import load_dotenv

# --- Load .env file for GUI usage ---
load_dotenv()

# Try to import Databricks SQL connector
try:
    from databricks import sql
except ImportError:
    sql = None

# Try to import pyspark
try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None

# ------------------------------------------------------
# Databricks SQL connection info (same as test_connect.py)
# ------------------------------------------------------
DATABRICKS_SERVER = "dbc-bd1c6d29-5a6c.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/5aaa9aaa404e72b9"
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

if not DATABRICKS_TOKEN:
    raise EnvironmentError("❌ Missing DATABRICKS_TOKEN. Please set it in your .env file.")

# ------------------------------------------------------
# Connection helper
# ------------------------------------------------------
def get_conn():
    """Create and return a Databricks SQL connection."""
    try:
        conn = sql.connect(
            server_hostname=DATABRICKS_SERVER,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        )
        return conn
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        raise

# ------------------------------------------------------
# Upload parsed records directly using SQL connector
# ------------------------------------------------------
def upload_parsed_records(file_records, table_name="parsed_files"):
    """
    Upload parsed records (list of dicts) to Databricks SQL table.
    Each record must have keys: file_name, file_type, content.
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        # Ensure table exists
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                file_name STRING,
                file_type STRING,
                content STRING,
                parsed_at TIMESTAMP
            )
        """)

        insert_q = f"""
            INSERT INTO {table_name} (file_name, file_type, content, parsed_at)
            VALUES (%s, %s, %s, %s)
        """
        now = datetime.utcnow().isoformat(sep=" ")
        rows = [(r["file_name"], r["file_type"], r["content"], now) for r in file_records]

        cur.executemany(insert_q, rows)
        print(f"✅ Uploaded {len(rows)} records to Databricks table '{table_name}'")

    except Exception as e:
        print(f"❌ Upload failed: {e}")
        traceback.print_exc()
        raise
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

# ------------------------------------------------------
# Spark helpers (optional)
# ------------------------------------------------------
def init_spark(app_name="FileParserDatabricks"):
    """Initialize a local SparkSession if available."""
    if SparkSession is None:
        print("⚠️ pyspark not installed — skipping Spark initialization.")
        return None
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("✅ Spark session created successfully.")
        return spark
    except Exception as e:
        print(f"❌ Spark initialization failed: {e}")
        traceback.print_exc()
        return None
def create_parsed_table_if_not_exists(spark, table_name):
    """Creates a table in Databricks if it doesn't already exist."""
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                file_name STRING,
                file_type STRING,
                content STRING
            )
        """)
        print(f"✅ Table '{table_name}' is ready.")
    except Exception as e:
        print(f"❌ Failed to create table {table_name}: {e}")


def write_to_databricks(spark, data_path, table_name="parsed_files"):
    """
    Unified write: use Spark if available, else fall back to SQL upload.
    """
    try:
        if spark is not None and SparkSession is not None and isinstance(spark, SparkSession):
            ext = os.path.splitext(data_path)[1].lower()
            if ext == ".csv":
                df = spark.read.option("header", True).csv(data_path)
            elif ext == ".parquet":
                df = spark.read.parquet(data_path)
            else:
                df = spark.read.text(data_path)  

            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            print(f"✅ Data written to Databricks via Spark table '{table_name}'.")
            return True

        # fallback: upload via SQL connector
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"File not found: {data_path}")

        with open(data_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        record = [{
            "file_name": os.path.basename(data_path),
            "file_type": os.path.splitext(data_path)[1].lower(),
            "content": content,
        }]
        upload_parsed_records(record, table_name)
        return True

    except Exception as e:
        print(f"❌ write_to_databricks failed: {e}")
        traceback.print_exc()
        return False
