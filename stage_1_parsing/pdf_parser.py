# stage_1_parsing/pdf_parser.py

import pymupdf as fitz
import os
from typing import Tuple, List

def parse_pdf(file_path: str, session_id: str) -> Tuple[str, List[str]]:
    text_parts = []
    saved_images = []

    images_dir = os.path.join("Outputs", "pdf_images", session_id)
    os.makedirs(images_dir, exist_ok=True)

    with fitz.open(file_path) as pdf:
        for page_index, page in enumerate(pdf):
            # extract text blocks safely
            try:
                blocks = page.get_text("blocks")
                for block in blocks:
                    if len(block) >= 5 and block[4].strip():
                        text_parts.append(block[4].strip())
            except:
                text_parts.append(page.get_text("text"))

            # extract images
            for img_index, img in enumerate(page.get_images(full=True)):
                try:
                    xref = img[0]
                    base_image = pdf.extract_image(xref)
                    img_bytes = base_image["image"]
                    ext = base_image.get("ext", "png")

                    img_name = f"{os.path.basename(file_path)}_p{page_index+1}_{img_index+1}.{ext}"
                    img_path = os.path.join(images_dir, img_name)

                    with open(img_path, "wb") as fh:
                        fh.write(img_bytes)

                    saved_images.append(img_path)
                except:
                    continue

    return "\n".join(text_parts).strip(), saved_images
