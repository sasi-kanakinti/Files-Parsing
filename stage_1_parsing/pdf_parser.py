# stage_1_parsing/pdf_parser.py
import fitz  # PyMuPDF
import os
from typing import Tuple

def parse_pdf(file_path: str, extract_images: bool = False) -> str:
    """
    Extract text (fast) from PDF using 'blocks' and optionally save images.
    Returns the extracted text.
    Params:
      - extract_images: only write images to disk if True
    """
    text_parts = []
    images_dir = os.path.join("outputs", "pdf_images")
    if extract_images:
        os.makedirs(images_dir, exist_ok=True)

    with fitz.open(file_path) as pdf:
        for page_index, page in enumerate(pdf):
            # 'blocks' tends to be faster and more structured than "text"
            try:
                blocks = page.get_text("blocks")  # list of tuples (x0,y0,x1,y1,"text", block_no)
                # join block texts preserving some whitespace
                for b in blocks:
                    if len(b) >= 5 and b[4].strip():
                        text_parts.append(b[4].strip())
            except Exception:
                # fallback
                text_parts.append(page.get_text("text"))

            if extract_images:
                for img_index, img in enumerate(page.get_images(full=True)):
                    try:
                        xref = img[0]
                        base_image = pdf.extract_image(xref)
                        image_bytes = base_image.get("image")
                        image_ext = base_image.get("ext", "png")
                        image_filename = f"{os.path.splitext(os.path.basename(file_path))[0]}_p{page_index+1}_{img_index+1}.{image_ext}"
                        image_path = os.path.join(images_dir, image_filename)
                        with open(image_path, "wb") as img_file:
                            img_file.write(image_bytes)
                    except Exception:
                        continue

    return "\n".join(text_parts).strip()
