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
    list_tables,
    preview_table,
    drop_table
)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecretkey")

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# where uploaded files are kept
UPLOAD_ROOT = os.path.join(BASE_DIR, "uploads")
# where parsed text outputs and images are kept (parsers write under 'outputs/<*_images>')
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
# image-serving root — point to same outputs directory so serve_image finds images
IMAGES_ROOT = OUTPUTS_DIR

os.makedirs(UPLOAD_ROOT, exist_ok=True)
os.makedirs(OUTPUTS_DIR, exist_ok=True)


# -------------------------
# Home
# -------------------------
@app.route("/")
def index():
    return render_template("index.html")


# -------------------------
# Upload Handling
# -------------------------
@app.route("/upload", methods=["POST"])
def upload_files():
    if "files" not in request.files:
        flash("No files uploaded.", "danger")
        return redirect(url_for("index"))

    files = request.files.getlist("files")
    if not files:
        flash("Select at least one file.", "warning")
        return redirect(url_for("index"))

    # Unique session ID
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

    return redirect(url_for("parse_results", session_id=session_id))


# -------------------------
# Parse & Display Results
# -------------------------
@app.route("/parse/<session_id>")
def parse_results(session_id):
    upload_folder = os.path.join(UPLOAD_ROOT, session_id)

    if not os.path.isdir(upload_folder):
        flash("Session not found.", "danger")
        return redirect(url_for("index"))

    # Parse with session-aware process_folder (your process_folder expects session_id)
    try:
        # if your process_folder signature differs, adapt accordingly
        parsed_df = process_folder(upload_folder, session_id)
    except TypeError:
        # fallback if process_folder doesn't take session_id
        parsed_df = process_folder(upload_folder)
    except Exception as e:
        return render_template("error.html", error=str(e))

    # Save parsed output file
    output_name = f"parsed_output_{session_id}.txt"
    try:
        output_path = save_parsed_data(parsed_df, output_name)
    except Exception:
        # fallback: create an empty file so UI doesn't break
        output_path = os.path.join(OUTPUTS_DIR, output_name)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("")
        flash("Used fallback to create parsed output file.", "warning")

    output_filename = os.path.basename(output_path)

    # Build records and convert image paths into {url, name, path}
    records = []
    for _, row in parsed_df.iterrows():
        txt = str(row.get("content", ""))
        snippet = txt[:1000] + "..." if len(txt) > 1000 else txt

        images_out = []
        for img_path in (row.get("images") or []):
            # determine bucket (pdf/word/excel) from path or file_type
            filename = os.path.basename(img_path)
            # try to infer type from path parts
            lower = img_path.replace("\\", "/").lower()
            if "/pdf_images/" in lower:
                typ = "pdf"
            elif "/word_images/" in lower:
                typ = "word"
            elif "/excel_images/" in lower:
                typ = "excel"
            else:
                # fallback: use file_type field
                ft = (row.get("file_type") or "").lower()
                if ft == ".pdf":
                    typ = "pdf"
                elif ft in (".doc", ".docx"):
                    typ = "word"
                elif ft in (".xls", ".xlsx"):
                    typ = "excel"
                else:
                    typ = "pdf"

            # Build serving URL for the image: /images/<typ>/<session_id>/<filename>
            try:
                img_url = url_for("serve_image", typ=typ, session_id=session_id, filename=filename)
            except Exception:
                img_url = f"/images/{typ}/{session_id}/{filename}"

            images_out.append({"url": img_url, "name": filename, "path": img_path})

        records.append({
            "file_name": row.get("file_name"),
            "file_type": row.get("file_type"),
            "content": txt,
            "snippet": snippet,
            "images": images_out
        })

    uploaded_files = sorted(os.listdir(upload_folder)) if os.path.isdir(upload_folder) else []

    return render_template(
        "results.html",
        session_id=session_id,
        parsed_records=records,
        uploaded_files=uploaded_files,
        output_file=output_filename,   # <-- template expects 'output_file'
        output_filepath=output_path    # optional full path if you need it in template
    )


# -------------------------
# Download parsed output
# -------------------------
@app.route("/download/<filename>")
def download_output(filename):
    return send_from_directory(OUTPUTS_DIR, filename, as_attachment=True)


# -------------------------
# Databricks Upload
# -------------------------
@app.route("/upload_to_databricks", methods=["POST"])
def upload_to_databricks():
    data = request.get_json(silent=True) or request.form or {}
    table_name = data.get("table_name")
    output_file = data.get("output_file")

    if not table_name:
        return jsonify({"error": "table_name is required"}), 400
    if not output_file:
        return jsonify({"error": "output_file is required"}), 400

    file_path = os.path.join(OUTPUTS_DIR, os.path.basename(output_file))

    if not os.path.exists(file_path):
        return jsonify({"error": "Output file not found"}), 404

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        upload_parsed_records(
            [{"file_name": os.path.basename(output_file), "file_type": ".txt", "content": content}],
            table_name=table_name
        )

        return jsonify({"message": f"Uploaded to table {table_name}"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -------------------------
# Serve Extracted Images
# -------------------------
@app.route("/images/<typ>/<session_id>/<filename>")
def serve_image(typ, session_id, filename):
    # images are expected to be saved under: outputs/<typ>_images/<session_id>/<filename>
    folder = os.path.join(IMAGES_ROOT, f"{typ}_images", session_id)
    if not os.path.isdir(folder):
        return ("Folder not found", 404)
    return send_from_directory(folder, filename)


# -------------------------
# Upload File Downloads
# -------------------------
@app.route("/uploads/<session_id>/<filename>")
def download_uploaded_file(session_id, filename):
    folder = os.path.join(UPLOAD_ROOT, session_id)
    return send_from_directory(folder, filename, as_attachment=True)


# -------------------------
# Preview PDF Inline
# -------------------------
@app.route("/preview/<session_id>/<filename>")
def preview_uploaded_file(session_id, filename):
    folder = os.path.join(UPLOAD_ROOT, session_id)
    if filename.lower().endswith(".pdf"):
        return send_from_directory(folder, filename)
    return send_from_directory(folder, filename, as_attachment=True)


# -------------------------
# Databricks Explorer
# -------------------------
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


# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    app.run(debug=True, port=int(os.environ.get("PORT", 5000)))
