# stage_2_databricks/databricks_uploader.py
import os
from .db_utils import upload_parsed_records, write_to_databricks, init_spark

def upload_parsed_output_cli():
    outputs_dir = "outputs"
    if not os.path.exists(outputs_dir):
        print("⚠️ 'outputs/' not found. Run stage 1 first.")
        return

    files = [f for f in os.listdir(outputs_dir) if os.path.isfile(os.path.join(outputs_dir, f))]
    if not files:
        print("⚠️ No files in outputs/")
        return

    for i, f in enumerate(files, 1):
        print(f"{i}. {f}")
    choice = input("Select file number (or 0 to upload all): ").strip()
    try:
        choice = int(choice)
    except ValueError:
        print("❌ Invalid choice.")
        return

    selected = files if choice == 0 else [files[choice - 1]]
    to_upload = []
    for fn in selected:
        path = os.path.join(outputs_dir, fn)
        with open(path, "r", encoding="utf-8") as fh:
            content = fh.read()
        to_upload.append({"file_name": fn, "file_type": os.path.splitext(fn)[1].lower(), "content": content})

    # prefer SQL connector for small sets; for large sets recommend Spark path:
    try:
        upload_parsed_records(to_upload)
    except Exception as e:
        print("Connector upload failed; attempting Spark fallback.")
        spark = init_spark()
        if spark:
            # write each file as a simple text table via write_to_databricks fallback
            for fn in selected:
                p = os.path.join(outputs_dir, fn)
                write_to_databricks(spark, p, table_name="parsed_files")
            spark.stop()

if __name__ == "__main__":
    upload_parsed_output_cli()
