# stage_1_parsing/process_files.py

import os
from typing import List, Dict, Callable
import pandas as pd
from io import StringIO
from concurrent.futures import ProcessPoolExecutor, as_completed

from .pdf_parser import parse_pdf
from .word_parser import parse_word
from .excel_parser import parse_excel

# parser registry
PARSERS: Dict[str, Callable] = {
    ".pdf": parse_pdf,
    ".docx": parse_word,
    ".doc": parse_word,
    ".xlsx": parse_excel,
    ".xls": parse_excel,
}

def _process_single(file_path: str, session_id: str) -> Dict:
    """
    Worker: parse a single file and return dict including images.
    """
    ext = os.path.splitext(file_path)[1].lower()
    base = os.path.basename(file_path)

    parser = PARSERS.get(ext)
    if parser is None:
        return {
            "file_name": base,
            "file_type": ext,
            "content": "",
            "images": [],
            "error": f"Unsupported type: {ext}",
        }

    try:
        content, images = parser(file_path, session_id=session_id)
        return {
            "file_name": base,
            "file_type": ext,
            "content": content,
            "images": images,
            "error": None,
        }
    except Exception as e:
        return {
            "file_name": base,
            "file_type": ext,
            "content": "",
            "images": [],
            "error": str(e),
        }


def process_folder(folder_path: str, session_id: str, max_workers: int = None) -> pd.DataFrame:
    """
    Process all files in a folder and return a DataFrame including images.
    """
    files = []

    for entry in os.listdir(folder_path):
        full = os.path.join(folder_path, entry)
        if os.path.isfile(full):
            ext = os.path.splitext(full)[1].lower()
            if ext in PARSERS:
                files.append(full)

    if not files:
        return pd.DataFrame(columns=["file_name", "file_type", "content", "images", "error"])

    # multiprocess
    if max_workers is None:
        import multiprocessing
        max_workers = min(4, multiprocessing.cpu_count() or 1)

    results = []
    with ProcessPoolExecutor(max_workers=max_workers) as exe:
        futures = {exe.submit(_process_single, f, session_id): f for f in files}
        for fut in as_completed(futures):
            results.append(fut.result())

    df = pd.DataFrame(results)
    return df[["file_name", "file_type", "content", "images", "error"]]


def save_parsed_data(parsed_data: pd.DataFrame, output_file: str = None) -> str:
    """
    Write parsed text output.
    """
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)

    if not output_file:
        output_file = "parsed_output.txt"

    output_path = os.path.join(output_dir, output_file)

    buf = StringIO()
    for _, row in parsed_data.iterrows():
        buf.write(f"=== {row['file_name']} ({row['file_type']}) ===\n")
        if row["error"]:
            buf.write(f"[ERROR] {row['error']}\n\n")
            continue
        buf.write(str(row["content"]) + "\n\n")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(buf.getvalue())

    return output_path
