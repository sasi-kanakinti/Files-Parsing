# ============================================================
# app.py — FINAL RAILWAY DEPLOYMENT VERSION (BUG-FIXED)
# ============================================================

import os
import uuid
from datetime import datetime
from flask import (
    Flask, request, render_template, redirect, url_for,
    send_from_directory, flash, jsonify
)

# stage 1 parsing
from stage_1_parsing import process_folder, save_parsed_data

# databricks utilities
from stage_2_databricks.db_utils import (
    upload_parsed_records, list_tables, preview_table, drop_table
)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecretkey")

# ======================================================
# FIXED DIRECTORIES (Railway SAFE)
# ======================================================
UPLOAD_ROOT = "/tmp/uploads"
OUTPUTS_DIR = "/tmp/outputs"      # << FIXED
IMAGES_ROOT = "/tmp/outputs/images"

os.makedirs(UPLOAD_ROOT, exist_ok=True)
os.makedirs(OUTPUTS_DIR, exist_ok=True)
os.makedirs(IMAGES_ROOT, exist_ok=True)


# ======================================================
# HOME
# ======================================================
@app.route("/")
def index():
    return render_template("index.html")


# ======================================================
# UPLOAD
# ======================================================
@app.route("/upload", methods=["POST"])
def upload_files():
    if "files" not in request.files:
        flash("No files uploaded", "danger")
        return redirect(url_for("index"))

    files = request.files.getlist("files")

    if not files:
        flash("Select at least one file", "warning")
        return redirect(url_for("index"))

    session_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
    dest = os.path.join(UPLOAD_ROOT, session_id)
    os.makedirs(dest, exist_ok=True)

    saved = []
    for f in files:
        if f.filename:
            path = os.path.join(dest, f.filename)
            f.save(path)
            saved.append(f.filename)

    if not saved:
        flash("Files could not be saved", "danger")
        return redirect(url_for("index"))

    return redirect(url_for("parse_results", session_id=session_id))


# ======================================================
# PARSE RESULTS
# ======================================================
@app.route("/parse/<session_id>")
def parse_results(session_id):
    folder = os.path.join(UPLOAD_ROOT, session_id)

    if not os.path.isdir(folder):
        flash("Session not found", "danger")
        return redirect(url_for("index"))

    # process
    try:
        parsed_df = process_folder(folder, session_id)
    except Exception as e:
        return render_template("error.html", error=f"Parser failed: {e}")

    # save output file
    output_filename = f"parsed_output_{session_id}.txt"
    output_path = os.path.join(OUTPUTS_DIR, output_filename)

    try:
        save_parsed_data(parsed_df, output_path)   # << FIXED PATH
    except Exception:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("")

    # prepare records
    records = []
    for _, row in parsed_df.iterrows():
        text = str(row.get("content", ""))
        snippet = text[:1000] + "..." if len(text) > 1000 else text

        records.append({
            "file_name": row["file_name"],
            "file_type": row["file_type"],
            "content": text,
            "snippet": snippet,
            "images": row.get("images", [])
        })

    return render_template(
        "results.html",
        session_id=session_id,
        parsed_records=records,
        output_file=output_filename
    )


# ======================================================
# DOWNLOAD PARSED OUTPUT
# ======================================================
@app.route("/download/<filename>")
def download_output(filename):
    return send_from_directory(OUTPUTS_DIR, filename, as_attachment=True)


# ======================================================
# SERVE IMAGES
# ======================================================
@app.route("/images/<session_id>/<filename>")
def serve_image(session_id, filename):
    folder = os.path.join(IMAGES_ROOT, session_id)

    if not os.path.isdir(folder):
        return ("Image folder not found", 404)

    return send_from_directory(folder, filename)


# ======================================================
# ORIGINAL FILE DOWNLOAD
# ======================================================
@app.route("/uploads/<session_id>/<filename>")
def download_uploaded_file(session_id, filename):
    folder = os.path.join(UPLOAD_ROOT, session_id)
    return send_from_directory(folder, filename, as_attachment=True)


# ======================================================
# PDF PREVIEW
# ======================================================
@app.route("/preview/<session_id>/<filename>")
def preview_uploaded_file(session_id, filename):
    folder = os.path.join(UPLOAD_ROOT, session_id)
    return send_from_directory(folder, filename)


# ======================================================
# UPLOAD TO DATABRICKS
# ======================================================
@app.route("/upload_to_databricks", methods=["POST"])
def upload_to_databricks():
    data = request.get_json() or request.form
    table = data.get("table_name")
    output_filename = data.get("output_file")

    if not table or not output_filename:
        return jsonify({"error": "Missing table or file"}), 400

    file_path = os.path.join(OUTPUTS_DIR, output_filename)

    if not os.path.exists(file_path):
        return jsonify({"error": "Output file not found"}), 404

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    try:
        upload_parsed_records(
            [{"file_name": output_filename, "file_type": ".txt", "content": content}],
            table_name=table
        )
        return jsonify({"message": f"Uploaded to Databricks table {table}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ======================================================
# DATABRICKS TABLE LIST
# ======================================================
@app.route("/db/tables")
def db_tables():
    try:
        tables = list_tables()
        return render_template("db_tables.html", tables=tables)
    except Exception as e:
        return render_template("error.html", error=str(e))


@app.route("/db/table/<table_name>")
def db_table_preview(table_name):
    try:
        cols, rows = preview_table(table_name)
        return render_template("db_table_preview.html", columns=cols, rows=rows, table_name=table_name)
    except Exception as e:
        return render_template("error.html", error=str(e))


@app.route("/db/table/<table_name>/delete", methods=["POST"])
def db_table_delete(table_name):
    drop_table(table_name)
    flash("Table deleted", "success")
    return redirect(url_for("db_tables"))


# ======================================================
# RUN
# ======================================================
if __name__ == "__main__":
    app.run(debug=True, port=5000)
