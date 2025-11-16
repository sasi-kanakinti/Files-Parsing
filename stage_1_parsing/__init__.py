# stage_1_parsing/__init__.py
"""
Stage 1 Parsing Package
Exposes the two main functions:
  - process_folder
  - save_parsed_data
"""

from .process_files import process_folder, save_parsed_data

__all__ = ["process_folder", "save_parsed_data"]
