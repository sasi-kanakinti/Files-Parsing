# stage_2_databricks/db_utils.py
"""
Optimized Databricks helpers:
 - batched insert with SQL connector
 - Spark path uses createDataFrame + saveAsTable (if Spark available)
 - safer connection handling
"""
import os
import traceback
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Try to import Databricks SQL connector
try:
    from databricks import sql
except Exception:
    sql = None

# Try to import pyspark
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import DataFrame as SparkDataFrame
except Exception:
    SparkSession = None
    SparkDataFrame = None

DATABRICKS_SERVER = os.getenv("DATABRICKS_SERVER", "dbc-bd1c6d29-5a6c.cloud.databricks.com")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/5aaa9aaa404e72b9")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

if not DATABRICKS_TOKEN:
    # Do not raise on import to allow offline development; raise when used.
    DATABRICKS_TOKEN = None

def get_conn():
    if sql is None:
        raise ImportError("databricks-sql-connector is not installed.")
    if not DATABRICKS_TOKEN:
        raise EnvironmentError("Missing DATABRICKS_TOKEN in environment.")
    return sql.connect(
        server_hostname=DATABRICKS_SERVER,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
        timeout=30
    )

def upload_parsed_records(file_records, table_name="parsed_files", batch_size: int = 200):
    """
    Upload parsed records (list of dicts) to Databricks SQL table in batches.
    Each record must have keys: file_name, file_type, content.
    """
    if sql is None:
        raise ImportError("databricks-sql-connector is required for upload_parsed_records.")

    if not DATABRICKS_TOKEN:
        raise EnvironmentError("Missing DATABRICKS_TOKEN in environment.")

    try:
        conn = get_conn()
        cur = conn.cursor()

        # ensure table exists
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                file_name STRING,
                file_type STRING,
                content STRING,
                parsed_at TIMESTAMP
            )
        """)

        insert_q = f"""INSERT INTO {table_name} (file_name, file_type, content, parsed_at)VALUES(?, ?, ?, ?)"""

        now = datetime.utcnow().isoformat(sep=" ")
        # chunk rows
        rows = [(r.get("file_name"), r.get("file_type"), r.get("content"), now) for r in file_records]
        total = len(rows)
        for i in range(0, total, batch_size):
            chunk = rows[i: i + batch_size]
            cur.executemany(insert_q, chunk)
        cur.close()
        conn.close()
        print(f"✅ Uploaded {total} records to Databricks table '{table_name}'.")
        return True
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        traceback.print_exc()
        raise

def init_spark(app_name="FileParserDatabricks"):
    if SparkSession is None:
        print("⚠️ pyspark not installed — skipping Spark initialization.")
        return None
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        print("✅ Spark session created.")
        return spark
    except Exception as e:
        print(f"❌ Spark init failed: {e}")
        traceback.print_exc()
        return None

def write_to_databricks(spark, data, table_name="parsed_files"):
    """
    If spark is provided and 'data' is a pandas DataFrame or path, write via Spark.
    Otherwise fallback to upload_parsed_records (SQL connector).
    - data can be: pandas.DataFrame, path-to-file (csv, parquet, txt)
    """
    try:
        if spark is not None and SparkSession is not None and isinstance(spark, SparkSession):
            # convert pandas DF to spark DF if needed
            if hasattr(data, "to_dict"):  # likely pandas
                import pandas as pd
                df_pd = data
                sdf = spark.createDataFrame(df_pd)
                sdf.write.format("delta").mode("overwrite").saveAsTable(table_name)
                print(f"✅ Wrote pandas DataFrame to Delta table '{table_name}' via Spark.")
                return True
            # if data is a path, use spark reading
            if isinstance(data, str):
                ext = os.path.splitext(data)[1].lower()
                if ext == ".csv":
                    df_spark = spark.read.option("header", True).csv(data)
                elif ext == ".parquet":
                    df_spark = spark.read.parquet(data)
                else:
                    df_spark = spark.read.text(data)
                df_spark.write.format("delta").mode("overwrite").saveAsTable(table_name)
                print(f"✅ Wrote file to Delta table '{table_name}' via Spark.")
                return True

        # fallback: SQL connector
        if isinstance(data, str) and os.path.exists(data):
            with open(data, "r", encoding="utf-8", errors="ignore") as fh:
                content = fh.read()
            rec = [{"file_name": os.path.basename(data), "file_type": os.path.splitext(data)[1].lower(), "content": content}]
            upload_parsed_records(rec, table_name=table_name)
            return True
        elif hasattr(data, "to_dict"):
            # pandas DataFrame fallback
            df = data
            records = []
            now = datetime.utcnow().isoformat(sep=" ")
            for _, r in df.iterrows():
                records.append({
                    "file_name": r.get("file_name"),
                    "file_type": r.get("file_type"),
                    "content": r.get("content")
                })
            upload_parsed_records(records, table_name=table_name)
            return True

        raise ValueError("Unsupported data type for write_to_databricks.")
    except Exception as e:
        print(f"❌ write_to_databricks failed: {e}")
        traceback.print_exc()
        return False
