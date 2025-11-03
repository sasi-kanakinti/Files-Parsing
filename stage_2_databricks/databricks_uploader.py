# # stage_2_databricks/databricks_uploader.py
# import os
# import traceback
# from .db_utils import init_spark, write_to_databricks


# def upload_parsed_output():
#     """
#     Handles reading a parsed file and uploading it to Databricks via CLI.
#     """
#     try:
#         outputs_dir = "outputs"

#         # Ensure 'outputs' directory exists
#         if not os.path.exists(outputs_dir):
#             print("⚠️ 'outputs/' folder not found. Please run stage 1 first.")
#             return

#         files = [f for f in os.listdir(outputs_dir) if os.path.isfile(os.path.join(outputs_dir, f))]
#         if not files:
#             print("⚠️ No files found in 'outputs/' directory.")
#             return

#         print("\n📁 Available files in outputs:")
#         for i, f in enumerate(files, start=1):
#             print(f"{i}. {f}")

#         try:
#             choice = int(input("\nSelect a file by number: ")) - 1
#             if choice < 0 or choice >= len(files):
#                 print("❌ Invalid selection.")
#                 return
#         except ValueError:
#             print("❌ Please enter a valid number.")
#             return

#         file_path = os.path.join(outputs_dir, files[choice])

#         table_name = input("Enter Databricks table name to save data: ").strip()
#         if not table_name:
#             print("⚠️ Table name cannot be empty.")
#             return

#         print(f"\n🚀 Starting upload of '{file_path}' to Databricks table '{table_name}'...")

#         # Initialize Spark
#         spark = init_spark()
#         if not spark:
#             print("❌ Could not start Spark session.")
#             return

#         # Upload to Databricks
#         write_to_databricks(spark, file_path, table_name)

#         # Stop Spark safely
#         spark.stop()
#         print("🛑 Spark session closed.")
#         print(f"✅ Upload complete! Table '{table_name}' created/updated successfully.")

#     except Exception as e:
#         print(f"❌ Error during Databricks upload: {e}")
#         traceback.print_exc()


# if __name__ == "__main__":
#     print("🚀 Starting Databricks uploader...")
#     upload_parsed_output()


# inside databricks_uploader.py
from .db_utils import upload_parsed_records
import json
import os

def upload_parsed_output_cli():
    outputs_dir = "outputs"
    files = [f for f in os.listdir(outputs_dir) if os.path.isfile(os.path.join(outputs_dir, f))]
    for i, f in enumerate(files, 1):
        print(f"{i}. {f}")
    choice = int(input("Select file number (or 0 to upload all): "))
    to_upload = []

    if choice == 0:
        selected = files
    else:
        selected = [files[choice - 1]]

    for fn in selected:
        path = os.path.join(outputs_dir, fn)
        ext = os.path.splitext(fn)[1].lower()
        with open(path, "r", encoding="utf-8") as fh:
            content = fh.read()
        to_upload.append({
            "file_name": fn,
            "file_type": ext,
            "content": content
        })

    upload_parsed_records(to_upload)


# ✅ Entry point — this actually runs the upload when the file is executed
if __name__ == "__main__":
    upload_parsed_output_cli()
