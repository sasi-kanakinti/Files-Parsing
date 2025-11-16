# stage_1_parsing/process_files.py
"""
Optimized process_folder implementation:
 - Uses a parser registry (ext -> function)
 - Uses concurrent.futures.ProcessPoolExecutor for parallel parsing
 - Provides memory-friendly save_parsed_data using StringIO
"""
import os
from typing import List, Dict, Callable
import pandas as pd
from io import StringIO
import math
from concurrent.futures import ProcessPoolExecutor, as_completed

# local imports
from .pdf_parser import parse_pdf
from .word_parser import parse_word
from .excel_parser import parse_excel

# parser registry
PARSERS: Dict[str, Callable] = {
    ".pdf": lambda p: parse_pdf(p, extract_images=False),
    ".docx": parse_word,
    ".doc": parse_word,
    ".xlsx": parse_excel,
    ".xls": parse_excel,
}

def _process_single(file_path: str) -> Dict:
    """
    Worker function to parse one file. Designed to be picklable for ProcessPoolExecutor.
    """
    ext = os.path.splitext(file_path)[1].lower()
    base = os.path.basename(file_path)
    try:
        parser = PARSERS.get(ext)
        if parser is None:
            return {"file_name": base, "file_type": ext, "content": "", "error": f"unsupported: {ext}"}
        content = parser(file_path)
        return {"file_name": base, "file_type": ext, "content": content, "error": None}
    except Exception as e:
        return {"file_name": base, "file_type": ext, "content": "", "error": str(e)}

def process_folder(folder_path: str, max_workers: int = None) -> pd.DataFrame:
    """
    Process all supported files in a folder in parallel and return a DataFrame.
    - max_workers: None -> default to number of CPUs
    """
    files = []
    for entry in os.listdir(folder_path):
        full = os.path.join(folder_path, entry)
        if os.path.isfile(full):
            ext = os.path.splitext(full)[1].lower()
            if ext in PARSERS:
                files.append(full)

    if not files:
        # return empty df with expected columns
        return pd.DataFrame(columns=["file_name", "file_type", "content", "error"])

    # limit workers to avoid overloading I/O on small machines
    if max_workers is None:
        import multiprocessing
        max_workers = min(4, (multiprocessing.cpu_count() or 1))

    results: List[Dict] = []
    # Use ProcessPoolExecutor for CPU-bound parsing (fitz/docx/excel)
    with ProcessPoolExecutor(max_workers=max_workers) as exe:
        futures = {exe.submit(_process_single, f): f for f in files}
        for fut in as_completed(futures):
            try:
                res = fut.result()
            except Exception as e:
                res = {"file_name": os.path.basename(futures[fut]), "file_type": "", "content": "", "error": str(e)}
            results.append(res)

    df = pd.DataFrame(results)
    # Keep column ordering stable
    df = df[["file_name", "file_type", "content", "error"]]
    return df

def save_parsed_data(parsed_data: pd.DataFrame, output_file: str = None) -> str:
    """
    Save parsed data to outputs/ directory quickly using a StringIO buffer.
    Returns the output path.
    """
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)

    if not output_file:
        output_file = "parsed_output.txt"
    else:
        # ensure extension
        if not os.path.splitext(output_file)[1]:
            output_file = output_file + ".txt"

    output_path = os.path.join(output_dir, output_file)

    buf = StringIO()
    for _, row in parsed_data.iterrows():
        fn = row.get("file_name", "")
        ft = row.get("file_type", "")
        content = row.get("content", "")
        error = row.get("error", None)
        buf.write(f"=== {fn} ({ft}) ===\n")
        if error:
            buf.write(f"[ERROR] {error}\n\n")
            continue
        buf.write(str(content) + "\n\n")

    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(buf.getvalue())

    return output_path
