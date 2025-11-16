# stage_1_parsing/excel_parser.py
import os
from io import StringIO
from typing import Optional
import pandas as pd
from openpyxl import load_workbook

def parse_excel(file_path: str, extract_images: bool = False) -> str:
    """
    Parse an Excel file and return CSV-like string.
    - Avoids double-loading by using openpyxl for images and pandas for data.
    - If extract_images=False it skips image extraction to save time.
    """
    # Read with pandas (handles many formats and datatypes)
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
    except Exception:
        # fallback: try without engine
        df = pd.read_excel(file_path)

    buffer = StringIO()
    df.to_csv(buffer, index=False)
    csv_content = buffer.getvalue()

    if extract_images:
        images_dir = os.path.join("outputs", "excel_images")
        os.makedirs(images_dir, exist_ok=True)
        try:
            wb = load_workbook(file_path, data_only=True)
            for sheet_name in wb.sheetnames:
                sheet = wb[sheet_name]
                # openpyxl stores images under sheet._images (private API)
                for i, image in enumerate(getattr(sheet, "_images", []) or []):
                    image_filename = f"{os.path.splitext(os.path.basename(file_path))[0]}_{sheet_name}_{i+1}.png"
                    image_path = os.path.join(images_dir, image_filename)
                    try:
                        image.image.save(image_path)
                    except Exception:
                        # some image types don't provide .image - attempt bytes
                        try:
                            with open(image_path, "wb") as fh:
                                fh.write(image.ref)
                        except Exception:
                            pass
        except Exception:
            # non-fatal: many workbooks don't have images
            pass

    return csv_content
