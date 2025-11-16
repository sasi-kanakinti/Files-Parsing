# ============================================================
# app.py — FINAL, FULLY PATCHED, RAILWAY-STABLE VERSION
# ============================================================

import os
import uuid
from datetime import datetime
from flask import (
    Flask, request, render_template, redirect, url_for,
    send_from_directory, flash, jsonify
)

# Stage 1 parsing
from stage_1_parsing import process_folder, save_parsed_data

# Databricks utilities
from stage_2_databricks.db_utils import (
    upload_parsed_records, list_tables, preview_table, drop_table
)

# Flask App
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecretkey")

# =======================================
# SAFE DIRECTORIES FOR RAILWAY
# =======================================
UPLOAD_ROOT = "/tmp/uploads"
OUTPUTS_DIR = "/tmp/outputs"
IMAGES_ROOT = "/tmp/images"

os.makedirs(UPLOAD_ROOT, exist_ok=True)
os.makedirs(OUTPUTS_DIR, exist_ok=True)
os.makedirs(IMAGES_ROOT, exist_ok=True)


# =======================================
# HOME PAGE
# =======================================
@app.route("/")
def index():
    return render_template("index.html")


# =======================================
# FILE UPLOAD
# =======================================
@app.route("/upload", methods=["POST"])
def upload_files():
    if "files" not in request.files:
        flash("No files uploaded.", "danger")
        return redirect(url_for("index"))

    files = request.files.getlist("files")
    if not files:
        flash("Please select at least one file.", "warning")
        return redirect(url_for("index"))

    session_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
    dest_dir = os.path.join(UPLOAD_ROOT, session_id)
    os.makedirs(dest_dir, exist_ok=True)

    saved = []
    for f in files:
        if f and f.filename:
            file_path = os.path.join(dest_dir, f.filename)
            f.save(file_path)
            saved.append(f.filename)

    if not saved:
        flash("Failed to save uploaded files.", "danger")
        return redirect(url_for("index"))

    return redirect(url_for("parse_results", session_id=session_id))


# =======================================
# PARSE RESULTS PAGE
# =======================================
@app.route("/parse/<session_id>")
def parse_results(session_id):
    upload_dir = os.path.join(UPLOAD_ROOT, session_id)

    if not os.path.isdir(upload_dir):
        flash("Invalid session ID.", "danger")
        return redirect(url_for("index"))

    # Process uploaded folder
    try:
        parsed_df = process_folder(upload_dir, session_id)
    except Exception as e:
        return render_template("error.html", error=f"Parser failed: {e}")

    # Save parsed text output
    output_filename = f"parsed_output_{session_id}.txt"
    output_path = os.path.join(OUTPUTS_DIR, output_filename)

    try:
        save_parsed_data(parsed_df, output_path)
    except Exception:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("")

    # Build display records
    parsed_records = []
    for _, row in parsed_df.iterrows():
        text = str(row.get("content", ""))
        snippet = text[:1000] + "..." if len(text) > 1000 else text

        parsed_records.append({
            "file_name": row["file_name"],
            "file_type": row["file_type"],
            "content": text,
            "snippet": snippet,
            "images": row["images"],
        })

    uploaded_files = os.listdir(upload_dir)

    return render_template(
        "results.html",
        session_id=session_id,
        parsed_records=parsed_records,
        uploaded_files=uploaded_files,
        output_file=output_filename
    )


# =======================================
# DOWNLOAD OUTPUT FILE
# =======================================
@app.route("/download/<filename>")
def download_output(filename):
    return send_from_directory(OUTPUTS_DIR, filename, as_attachment=True)


# =======================================
# SERVE EXTRACTED IMAGES
# =======================================
@app.route("/images/<path:image>")
def serve_image(image):
    return send_from_directory(IMAGES_ROOT, image)


# =======================================
# DOWNLOAD ORIGINAL UPLOAD
# =======================================
@app.route("/uploads/<session_id>/<filename>")
def download_uploaded_file(session_id, filename):
    folder = os.path.join(UPLOAD_ROOT, session_id)
    return send_from_directory(folder, filename, as_attachment=True)


# =======================================
# PREVIEW PDF / DOCUMENT
# =======================================
@app.route("/preview/<session_id>/<filename>")
def preview_uploaded_file(session_id, filename):
    folder = os.path.join(UPLOAD_ROOT, session_id)
    return send_from_directory(folder, filename)


# =======================================
# UPLOAD TO DATABRICKS
# =======================================
@app.route("/upload_to_databricks", methods=["POST"])
def upload_to_databricks():
    data = request.get_json() or request.form
    table_name = data.get("table_name")
    file_name = data.get("output_file")

    if not table_name:
        return jsonify({"error": "Table name required"}), 400
    if not file_name:
        return jsonify({"error": "Output file missing"}), 400

    file_path = os.path.join(OUTPUTS_DIR, file_name)

    if not os.path.exists(file_path):
        return jsonify({"error": "Output file not found"}), 404

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    try:
        upload_parsed_records(
            [{"file_name": file_name, "file_type": ".txt", "content": content}],
            table_name=table_name
        )
        return jsonify({"message": f"Uploaded to Databricks: {table_name}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =======================================
# LIST TABLES
# =======================================
@app.route("/db/tables")
def db_tables():
    try:
        tables = list_tables()
        return render_template("db_tables.html", tables=tables)
    except Exception as e:
        return render_template("error.html", error=str(e))


# =======================================
# PREVIEW TABLE
# =======================================
@app.route("/db/table/<table_name>")
def db_table_preview(table_name):
    try:
        columns, rows = preview_table(table_name)
        return render_template(
            "db_table_preview.html",
            table_name=table_name,
            columns=columns,
            rows=rows
        )
    except Exception as e:
        return render_template("error.html", error=str(e))


# =======================================
# DELETE TABLE
# =======================================
@app.route("/db/table/<table_name>/delete", methods=["POST"])
def db_table_delete(table_name):
    try:
        drop_table(table_name)
        flash("Table deleted successfully.", "success")
        return redirect(url_for("db_tables"))
    except Exception as e:
        return render_template("error.html", error=str(e))


# =======================================
# MAIN
# =======================================
if __name__ == "__main__":
    app.run(debug=True, port=5000)
