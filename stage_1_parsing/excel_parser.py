# stage_1_parsing/excel_parser.py

import os
from io import StringIO
import pandas as pd
from openpyxl import load_workbook
from typing import Tuple, List

def parse_excel(file_path: str, session_id: str) -> Tuple[str, List[str]]:
    saved_images = []

    images_dir = os.path.join("Outputs", "excel_images", session_id)
    os.makedirs(images_dir, exist_ok=True)

    # read CSV-like content
    df = pd.read_excel(file_path, engine="openpyxl")
    buf = StringIO()
    df.to_csv(buf, index=False)
    csv_content = buf.getvalue()

    # extract images
    try:
        wb = load_workbook(file_path, data_only=True)
        for sheet in wb.sheetnames:
            ws = wb[sheet]
            for idx, img in enumerate(getattr(ws, "_images", []) or []):
                img_name = f"{os.path.basename(file_path)}_{sheet}_{idx+1}.png"
                img_path = os.path.join(images_dir, img_name)

                try:
                    img.image.save(img_path)
                    saved_images.append(img_path)
                except:
                    pass
    except:
        pass

    return csv_content, saved_images
