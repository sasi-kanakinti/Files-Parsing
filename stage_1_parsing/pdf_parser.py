import fitz
import os

def parse_pdf(file_path):
    """Extract text and images from a PDF file."""
    text = ""
    images_dir = "outputs/pdf_images"
    os.makedirs(images_dir, exist_ok=True)

    with fitz.open(file_path) as pdf:
        for page_index, page in enumerate(pdf):
            text += page.get_text("text")

            for img_index, img in enumerate(page.get_images(full=True)):
                xref = img[0]
                base_image = pdf.extract_image(xref)
                image_bytes = base_image["image"]
                image_ext = base_image["ext"]

                image_filename = f"{os.path.splitext(os.path.basename(file_path))[0]}_p{page_index+1}_{img_index+1}.{image_ext}"
                image_path = os.path.join(images_dir, image_filename)
                with open(image_path, "wb") as img_file:
                    img_file.write(image_bytes)

    return text.strip()
