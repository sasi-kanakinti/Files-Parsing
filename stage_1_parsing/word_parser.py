# stage_1_parsing/word_parser.py
from docx import Document
import os
from typing import List

def parse_word(file_path: str) -> str:
    """
    Extract text from a .docx file. Returns plain text.
    NOTE: .doc (older binary) is not supported by python-docx.
    """
    paragraphs: List[str] = []
    doc = Document(file_path)
    for p in doc.paragraphs:
        text = p.text.strip()
        if text:
            paragraphs.append(text)
    # Also try to extract tables as CSV-ish text
    for table in doc.tables:
        rows = []
        for r in table.rows:
            rows.append(",".join([c.text.replace("\n", " ").strip() for c in r.cells]))
        if rows:
            paragraphs.append("\n".join(rows))

    return "\n\n".join(paragraphs).strip()
