# import fitz
# from docx import Document
# import pandas as pd
# import os


# def parse_pdf(file_path):
#     text = ""
#     with fitz.open(file_path) as pdf:
#         for page in pdf:
#             text += page.get_text("text")
#     return text.strip()


# def parse_word(file_path):
#     doc = Document(file_path)
#     text = "\n".join([p.text for p in doc.paragraphs])
#     return text.strip()


# def parse_excel(file_path):
#     df = pd.read_excel(file_path)
#     return df.to_csv(index=False)


# def process_folder(folder_path):
#     records = []
#     for file in os.listdir(folder_path):
#         file_path = os.path.join(folder_path, file)
#         ext = os.path.splitext(file_path)[1].lower()

#         print(f"📄 Processing: {file}")
#         try:
#             if ext == ".pdf":
#                 content = parse_pdf(file_path)
#             elif ext == ".docx":
#                 content = parse_word(file_path)
#             elif ext in [".xls", ".xlsx"]:
#                 content = parse_excel(file_path)
#             else:
#                 print(f"⚠️ Skipping unsupported file: {file}")
#                 continue

#             records.append({
#                 "file_name": file,
#                 "file_type": ext,
#                 "content": content
#             })
#             print(f"✅ Parsed {file} successfully!")

#         except Exception as e:
#             print(f"❌ Error parsing {file}: {e}")

#     return pd.DataFrame(records)


# def save_parsed_data(parsed_data, output_file=None):
#     """Saves parsed results to outputs/ folder, with user-defined filename."""
#     output_dir = "outputs"
#     os.makedirs(output_dir, exist_ok=True)

#     # Ask the user for an output file name if not provided
#     if not output_file:
#         output_file = input("\n💾 Enter a name for the output file (without extension): ").strip()
#         if not output_file:
#             output_file = "parsed_output"
#         output_file += ".txt"

#     output_path = os.path.join(output_dir, output_file)

#     with open(output_path, "w", encoding="utf-8") as f:
#         for _, row in parsed_data.iterrows():
#             f.write(f"=== {row['file_name']} ({row['file_type']}) ===\n")
#             f.write(str(row['content']))
#             f.write("\n\n")

#     print(f"\n📁 All parsed data saved locally at: {output_path}")