from docx import Document
import os

def parse_word(file_path):
    """Extract text and images from a Word (.docx) file."""
    doc = Document(file_path)
    text = "\n".join([p.text for p in doc.paragraphs])

    images_dir = "outputs/word_images"
    os.makedirs(images_dir, exist_ok=True)

    for i, shape in enumerate(doc.inline_shapes):
        if shape.type == 3:  # Picture
            image_part = shape._inline.graphic.graphicData.pic.blipFill.blip.embed
            image = doc.part.related_parts[image_part]
            image_data = image.blob
            image_filename = f"{os.path.splitext(os.path.basename(file_path))[0]}_{i+1}.png"
            with open(os.path.join(images_dir, image_filename), "wb") as f:
                f.write(image_data)

    return text.strip()
