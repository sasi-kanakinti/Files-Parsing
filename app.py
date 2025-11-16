# ============================================================
# app.py — FINAL RAILWAY DEPLOYMENT VERSION (100% Working)
# ============================================================

import os
import uuid
import shutil
from datetime import datetime
from flask import (
    Flask, request, render_template, redirect, url_for,
    send_from_directory, flash, jsonify
)

# stage 1 parsing
from stage_1_parsing import process_folder, save_parsed_data

# databricks
from stage_2_databricks.db_utils import (
    upload_parsed_records, list_tables, preview_table, drop_table
)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecretkey")

# ================================
# STORAGE DIRECTORIES (RAILWAY SAFE)
# ================================
UPLOAD_ROOT = "/tmp/uploads"
OUTPUTS_DIR = "/tmp/outputs"
IMAGES_ROOT = "/tmp/Outputs"

# ensure directories exist
os.makedirs(UPLOAD_ROOT, exist_ok=True)
os.makedirs(OUTPUTS_DIR, exist_ok=True)
os.makedirs(IMAGES_ROOT, exist_ok=True)


# ================================
# HOME PAGE
# ================================
@app.route("/")
def index():
    return render_template("index.html")


# ================================
# UPLOAD FILES
# ================================
@app.route("/upload", methods=["POST"])
def upload_files():
    if "files" not in request.files:
        flash("No files uploaded.", "danger")
        return redirect(url_for("index"))

    files = request.files.getlist("files")
    if not files:
        flash("Select at least one file.", "warning")
        return redirect(url_for("index"))

    session_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]

    dest_dir = os.path.join(UPLOAD_ROOT, session_id)
    os.makedirs(dest_dir, exist_ok=True)

    saved = []
    for f in files:
        if f and f.filename:
            f.save(os.path.join(dest_dir, f.filename))
            saved.append(f.filename)

    if not saved:
        flash("Files could not be saved.", "danger")
        return redirect(url_for("index"))

    # redirect to result page
    return redirect(url_for("parse_results", session_id=session_id))


# ================================
# PARSE RESULTS
# ================================
@app.route("/parse/<session_id>")
def parse_results(session_id):
    upload_folder = os.path.join(UPLOAD_ROOT, session_id)

    if not os.path.isdir(upload_folder):
        flash("Session not found.", "danger")
        return redirect(url_for("index"))

    # run parser
    try:
        parsed_df = process_folder(upload_folder, session_id)
    except Exception as e:
        return render_template("error.html", error=f"Parsing failed: {e}")

    # save parsed text output
    output_name = f"parsed_output_{session_id}.txt"
    output_path = os.path.join(OUTPUTS_DIR, output_name)

    try:
        save_parsed_data(parsed_df, output_name)
    except:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("")

    # build parsed records
    records = []
    for _, row in parsed_df.iterrows():
        txt = str(row.get("content", ""))
        snippet = txt[:1000] + "..." if len(txt) > 1000 else txt

        records.append({
            "file_name": row["file_name"],
            "file_type": row["file_type"],
            "content": txt,
            "snippet": snippet,
            "images": row.get("images", [])
        })

    # categorize images
    images_by_type = {"pdf": [], "word": [], "excel": []}
    for r in records:
        ext = r["file_type"].lower()

        if ext == ".pdf":
            bucket = "pdf"
        elif ext in [".doc", ".docx"]:
            bucket = "word"
        elif ext in [".xls", ".xlsx"]:
            bucket = "excel"
        else:
            continue

        for img_path in r["images"]:
            filename = os.path.basename(img_path)
            img_url = url_for("serve_image", typ=bucket, session_id=session_id, filename=filename)

            images_by_type[bucket].append({
                "url": img_url,
                "name": filename,
                "path": img_path
            })

    uploaded_files = os.listdir(upload_folder)

    return render_template(
        "results.html",
        session_id=session_id,
        parsed_records=records,
        uploaded_files=uploaded_files,
        output_file=output_name,
        images_by_type=images_by_type
    )


# ================================
# DOWNLOAD PARSED OUTPUT
# ================================
@app.route("/download/<filename>")
def download_output(filename):
    return send_from_directory(OUTPUTS_DIR, filename, as_attachment=True)


# ================================
# SERVE EXTRACTED IMAGES
# ================================
@app.route("/images/<typ>/<session_id>/<filename>")
def serve_image(typ, session_id, filename):
    folder = os.path.join(IMAGES_ROOT, f"{typ}_images", session_id)

    if not os.path.isdir(folder):
        return ("Image folder not found", 404)

    return send_from_directory(folder, filename)


# ================================
# ORIGINAL UPLOADED FILE DOWNLOAD
# ================================
@app.route("/uploads/<session_id>/<filename>")
def download_uploaded_file(session_id, filename):
    folder = os.path.join(UPLOAD_ROOT, session_id)
    return send_from_directory(folder, filename, as_attachment=True)


# ================================
# PREVIEW PDFs INLINE
# ================================
@app.route("/preview/<session_id>/<filename>")
def preview_uploaded_file(session_id, filename):
    folder = os.path.join(UPLOAD_ROOT, session_id)

    if filename.lower().endswith(".pdf"):
        return send_from_directory(folder, filename)

    return send_from_directory(folder, filename, as_attachment=True)


# ================================
# UPLOAD TO DATABRICKS
# ================================
@app.route("/upload_to_databricks", methods=["POST"])
def upload_to_databricks():
    data = request.get_json() or request.form
    table = data.get("table_name")
    output_filename = data.get("output_file")

    if not table:
        return jsonify({"error": "table_name required"}), 400
    if not output_filename:
        return jsonify({"error": "output_file required"}), 400

    file_path = os.path.join(OUTPUTS_DIR, output_filename)

    if not os.path.exists(file_path):
        return jsonify({"error": "output file not found"}), 404

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    try:
        upload_parsed_records(
            [{"file_name": output_filename, "file_type": ".txt", "content": content}],
            table_name=table
        )
        return jsonify({"message": f"Uploaded to Databricks table '{table}'"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ================================
# DATABRICKS TABLE BROWSER
# ================================
@app.route("/db/tables")
def db_tables():
    try:
        return render_template("db_tables.html", tables=list_tables())
    except Exception as e:
        return render_template("error.html", error=str(e))


@app.route("/db/table/<table_name>")
def db_table_preview(table_name):
    try:
        cols, rows = preview_table(table_name)
        return render_template("db_table_preview.html", table_name=table_name, columns=cols, rows=rows)
    except Exception as e:
        return render_template("error.html", error=str(e))


@app.route("/db/table/<table_name>/delete", methods=["POST"])
def db_table_delete(table_name):
    try:
        drop_table(table_name)
        flash("Table deleted", "success")
        return redirect(url_for("db_tables"))
    except Exception as e:
        return render_template("error.html", error=str(e))


# ================================
# RUN
# ================================
if __name__ == "__main__":
    app.run(debug=True, port=5000)
