# app.py
import os
import uuid
import shutil
from datetime import datetime
from flask import Flask, request, render_template, redirect, url_for, send_from_directory, flash, jsonify

# Import your existing modules
from stage_1_parsing import process_folder, save_parsed_data
from stage_2_databricks.db_utils import upload_parsed_records

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "supersecretkey")
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Where uploaded files are temporarily stored for parsing
UPLOAD_ROOT = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOAD_ROOT, exist_ok=True)

OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUTPUTS_DIR, exist_ok=True)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload_files():
    """
    Save uploaded files into a new timestamped folder and redirect to parse view.
    """
    if "files" not in request.files:
        flash("No file part in the request.", "danger")
        return redirect(url_for("index"))

    files = request.files.getlist("files")
    if not files or files == [None] or files == []:
        flash("Please choose at least one file to upload.", "warning")
        return redirect(url_for("index"))

    # create unique folder for this upload
    session_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
    dest_folder = os.path.join(UPLOAD_ROOT, session_id)
    os.makedirs(dest_folder, exist_ok=True)

    saved_files = []
    for f in files:
        if f and f.filename:
            filename = f.filename
            safe_path = os.path.join(dest_folder, filename)
            f.save(safe_path)
            saved_files.append(filename)

    if not saved_files:
        flash("No valid files were uploaded.", "danger")
        return redirect(url_for("index"))

    return redirect(url_for("parse_results", session_id=session_id))


@app.route("/parse/<session_id>")
def parse_results(session_id):
    """
    Call your process_folder logic on the upload folder, then save parsed outputs and show results.
    """
    folder_path = os.path.join(UPLOAD_ROOT, session_id)
    if not os.path.exists(folder_path):
        flash("Upload session not found.", "danger")
        return redirect(url_for("index"))

    # Use your existing process_folder function which returns a pandas DataFrame
    try:
        parsed_df = process_folder(folder_path)
    except Exception as e:
        flash(f"Error while parsing files: {e}", "danger")
        return render_template("error.html", error=str(e))

    # Save a text backup inside outputs. Provide a safe filename
    safe_output_name = f"parsed_output_{session_id}.txt"
    try:
        # call your save_parsed_data which accepts parsed_data (DataFrame) and output_file name
        save_parsed_data(parsed_df, output_file=safe_output_name)
    except Exception as e:
        # fallback: write minimal output
        fallback_path = os.path.join(OUTPUTS_DIR, safe_output_name)
        with open(fallback_path, "w", encoding="utf-8") as fh:
            for _, row in parsed_df.iterrows():
                fh.write(f"=== {row['file_name']} ({row['file_type']}) ===\n")
                fh.write(str(row["content"]) + "\n\n")
        flash("Could not use save_parsed_data directly; created a fallback output file.", "warning")

    # Create list of records for the UI and for potential Databricks upload
    records = []
    for _, row in parsed_df.iterrows():
        content_str = str(row["content"])
        snippet = (content_str[:1000] + "...") if len(content_str) > 1000 else content_str
        records.append({
            "file_name": row["file_name"],
            "file_type": row["file_type"],
            "content": content_str,
            "snippet": snippet
        })

    output_filepath = os.path.join(OUTPUTS_DIR, safe_output_name)
    return render_template("results.html", session_id=session_id, records=records, output_file=os.path.basename(output_filepath))


@app.route("/download/<filename>")
def download_output(filename):
    return send_from_directory(OUTPUTS_DIR, filename, as_attachment=True)


@app.route("/upload_to_databricks", methods=["POST"])
def upload_to_databricks():
    """
    Receives JSON or form data; reads parsed output file and uploads to Databricks using db_utils.upload_parsed_records
    """
    table_name = request.form.get("table_name") or request.json.get("table_name")
    output_file = request.form.get("output_file") or request.json.get("output_file")

    if not table_name:
        return jsonify({"success": False, "error": "table_name is required"}), 400
    if not output_file:
        return jsonify({"success": False, "error": "output_file is required"}), 400

    output_path = os.path.join(OUTPUTS_DIR, output_file)
    if not os.path.exists(output_path):
        return jsonify({"success": False, "error": "Output file not found."}), 404

    # Read the output file and build records
    try:
        with open(output_path, "r", encoding="utf-8", errors="ignore") as fh:
            content = fh.read()
        record = [{
            "file_name": output_file,
            "file_type": os.path.splitext(output_file)[1].lower(),
            "content": content
        }]
        # upload_parsed_records expects a list of dicts; optional table_name param not included in signature
        upload_parsed_records(record, table_name=table_name)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

    return jsonify({"success": True, "message": f"Uploaded {output_file} to Databricks table {table_name}"})


@app.route("/cleanup/<session_id>", methods=["POST"])
def cleanup(session_id):
    """Delete temporary upload folder (useful after download/upload)."""
    folder_path = os.path.join(UPLOAD_ROOT, session_id)
    try:
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
    # Set debug=True for development; change in production
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
