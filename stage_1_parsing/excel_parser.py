import pandas as pd
from openpyxl import load_workbook
import os

def parse_excel(file_path):
    """Extract text data (as CSV) and any embedded images from an Excel file."""
    df = pd.read_excel(file_path)
    csv_content = df.to_csv(index=False)

    images_dir = "outputs/excel_images"
    os.makedirs(images_dir, exist_ok=True)

    try:
        wb = load_workbook(file_path)
        for sheet_name in wb.sheetnames:
            sheet = wb[sheet_name]
            for i, image in enumerate(getattr(sheet, "_images", [])):
                image_filename = f"{os.path.splitext(os.path.basename(file_path))[0]}_{sheet_name}_{i+1}.png"
                image_path = os.path.join(images_dir, image_filename)
                image.image.save(image_path)
    except Exception as e:
        print(f"⚠️ Could not extract images from Excel: {e}")

    return csv_content
