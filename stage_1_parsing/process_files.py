import os
import pandas as pd
from .pdf_parser import parse_pdf
from .word_parser import parse_word
from .excel_parser import parse_excel


def process_folder(folder_path):
    """Process all supported files in a folder and return a DataFrame of extracted data."""
    records = []
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        ext = os.path.splitext(file_path)[1].lower()

        print(f"📄 Processing: {file}")
        try:
            if ext == ".pdf":
                content = parse_pdf(file_path)
            elif ext == ".docx":
                content = parse_word(file_path)
            elif ext in [".xls", ".xlsx"]:
                content = parse_excel(file_path)
            else:
                print(f"⚠️ Skipping unsupported file: {file}")
                continue

            records.append({
                "file_name": file,
                "file_type": ext,
                "content": content
            })
            print(f"✅ Parsed {file} successfully!")

        except Exception as e:
            print(f"❌ Error parsing {file}: {e}")

    return pd.DataFrame(records)


def save_parsed_data(parsed_data, output_file=None):
    """Save parsed text data into outputs/ directory with user-defined file name."""
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)

    if not output_file:
        output_file = input("\n💾 Enter output file name (without extension): ").strip() or "parsed_output"
        output_file += ".txt"

    output_path = os.path.join(output_dir, output_file)

    with open(output_path, "w", encoding="utf-8") as f:
        for _, row in parsed_data.iterrows():
            f.write(f"=== {row['file_name']} ({row['file_type']}) ===\n")
            f.write(str(row['content']))
            f.write("\n\n")

    print(f"\n📁 All parsed data saved at: {output_path}")
