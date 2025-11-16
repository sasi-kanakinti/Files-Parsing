import os
import uuid
import shutil
from datetime import datetime
from flask import (
    Flask, request, render_template,
    redirect, url_for, send_from_directory,
    flash, jsonify
)

from stage_1_parsing import process_folder, save_parsed_data
from stage_2_databricks.db_utils import (
    upload_parsed_records,
    list_tables, preview_table, drop_table
)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecretkey")

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

UPLOAD_ROOT = os.path.join(BASE_DIR, "uploads")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")          # parsed text output
IMAGES_ROOT = os.path.join(BASE_DIR, "Outputs")          # images saved by parsers

os.makedirs(UPLOAD_ROOT, exist_ok=True)
os.makedirs(OUTPUTS_DIR, exist_ok=True)


# ---------------------------------------------------------
# HOME
# ---------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")


# ---------------------------------------------------------
# FILE UPLOAD
# ---------------------------------------------------------
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

    saved_files = []
    for f in files:
        if f and f.filename:
            f.save(os.path.join(dest_dir, f.filename))
            saved_files.append(f.filename)

    if not saved_files:
        flash("Files could not be saved.", "danger")
        return redirect(url_for("index"))

    return redirect(url_for("parse_results", session_id=session_id))


# ---------------------------------------------------------
# PARSE FILES + SHOW RESULTS
# ---------------------------------------------------------
@app.route("/parse/<session_id>")
def parse_results(session_id):
    upload_folder = os.path.join(UPLOAD_ROOT, session_id)

    if not os.path.isdir(upload_folder):
        flash("Invalid session", "danger")
        return redirect(url_for("index"))

    # Parse with session-aware parsers
    parsed_df = process_folder(upload_folder, session_id)

    # Save parsed output
    output_name = f"parsed_output_{session_id}.txt"
    output_path = save_parsed_data(parsed_df, output_name)

    # Build records for UI
    records = []
    for _, row in parsed_df.iterrows():
        content = str(row["content"])
        snippet = content[:1000] + "..." if len(content) > 1000 else content

        records.append({
            "file_name": row["file_name"],
            "file_type": row["file_type"],
            "content": content,
            "snippet": snippet,
            "images": row.get("images", [])
        })

    # Build images_by_type for UI display
    images_by_type = {"pdf": [], "word": [], "excel": []}

    for rec in records:
        ext = rec["file_type"].lower()
        if ext == ".pdf":
            bucket = "pdf"
        elif ext in (".doc", ".docx"):
            bucket = "word"
        elif ext in (".xls", ".xlsx"):
            bucket = "excel"
        else:
            continue

        for img_path in rec["images"]:
            filename = os.path.basename(img_path)

            img_url = url_for(
                "serve_image",
                typ=bucket,
                session_id=session_id,
                filename=filename
            )

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
        output_file=os.path.basename(output_path),   # <-- IMPORTANT FIX
        images_by_type=images_by_type
    )


# ---------------------------------------------------------
# DOWNLOAD PARSED OUTPUT
# ---------------------------------------------------------
@app.route("/download/<filename>")
def download_output(filename):
    return send_from_directory(OUTPUTS_DIR, filename, as_attachment=True)


# ---------------------------------------------------------
# SERVE EXTRACTED IMAGES
# ---------------------------------------------------------
@app.route("/images/<typ>/<session_id>/<filename>")
def serve_image(typ, session_id, filename):
    folder = os.path.join(IMAGES_ROOT, f"{typ}_images", session_id)

    if not os.path.isdir(folder):
        return ("Image folder not found.", 404)

    return send_from_directory(folder, filename)


# ---------------------------------------------------------
# ORIGINAL UPLOADED FILE DOWNLOAD
# ---------------------------------------------------------
@app.route("/uploads/<session_id>/<filename>")
def download_uploaded_file(session_id, filename):
    return send_from_directory(os.path.join(UPLOAD_ROOT, session_id), filename, as_attachment=True)


# ---------------------------------------------------------
# PREVIEW PDF INLINE
# ---------------------------------------------------------
@app.route("/preview/<session_id>/<filename>")
def preview_uploaded_file(session_id, filename):
    folder = os.path.join(UPLOAD_ROOT, session_id)
    if filename.lower().endswith(".pdf"):
        return send_from_directory(folder, filename)
    return send_from_directory(folder, filename, as_attachment=True)


# ---------------------------------------------------------
# UPLOAD PARSED TEXT TO DATABRICKS
# ---------------------------------------------------------
@app.route("/upload_to_databricks", methods=["POST"])
def upload_to_databricks():
    data = request.get_json() or request.form

    table_name = data.get("table_name")
    output_file = data.get("output_file")

    if not table_name:
        return jsonify({"error": "table_name is required"}), 400
    if not output_file:
        return jsonify({"error": "output_file is required"}), 400

    file_path = os.path.join(OUTPUTS_DIR, output_file)

    if not os.path.exists(file_path):
        return jsonify({"error": "Output file not found"}), 404

    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            content = fh.read()

        upload_parsed_records(
            [{
                "file_name": output_file,
                "file_type": ".txt",
                "content": content
            }],
            table_name=table_name
        )

        return jsonify({"message": "Upload successful!"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------
# DATABRICKS TABLE VIEWS
# ---------------------------------------------------------
@app.route("/db/tables")
def db_tables():
    return render_template("db_tables.html", tables=list_tables())


@app.route("/db/table/<table_name>")
def db_table_preview(table_name):
    cols, rows = preview_table(table_name)
    return render_template("db_table_preview.html", table_name=table_name, columns=cols, rows=rows)


@app.route("/db/table/<table_name>/delete", methods=["POST"])
def db_table_delete(table_name):
    drop_table(table_name)
    flash("Table deleted", "success")
    return redirect(url_for("db_tables"))


# ---------------------------------------------------------
# RUN APP
# ---------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000)
