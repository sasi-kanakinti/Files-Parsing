from databricks import sql
import os

conn = sql.connect(
    server_hostname="dbc-bd1c6d29-5a6c.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/5aaa9aaa404e72b9",
    access_token=os.getenv("DATABRICKS_TOKEN")
)
print("✅ Connected successfully!")
conn.close()
