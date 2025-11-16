# stage_1_parsing/excel_parser.py

import os
from io import StringIO
import pandas as pd
from openpyxl import load_workbook
from typing import Tuple, List


def parse_excel(file_path: str, session_id: str) -> Tuple[str, List[str]]:
    """
    Extract CSV-like text + images from Excel.
    Images saved under: /tmp/outputs/images/excel_images/<session_id>/
    """

    saved_images = []

    # Railway-safe final location
    images_dir = os.path.join("/tmp/outputs/images", "excel_images", session_id)
    os.makedirs(images_dir, exist_ok=True)

    # -------- TEXT PARSE (Pandas) --------
    df = pd.read_excel(file_path, engine="openpyxl")
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    csv_content = buffer.getvalue()

    # -------- IMAGE PARSE (openpyxl) --------
    try:
        wb = load_workbook(file_path, data_only=True)

        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]

            # Images stored in ws._images (private API)
            for idx, image in enumerate(getattr(ws, "_images", []) or []):
                img_name = f"{os.path.basename(file_path)}_{sheet_name}_{idx+1}.png"
                img_path = os.path.join(images_dir, img_name)

                try:
                    image.image.save(img_path)
                    saved_images.append(img_path)
                except Exception:
                    # fallback for formats without .image
                    try:
                        with open(img_path, "wb") as fh:
                            fh.write(image.ref)
                        saved_images.append(img_path)
                    except:
                        pass

    except Exception:
        # many excel files have no images
        pass

    return csv_content, saved_images
