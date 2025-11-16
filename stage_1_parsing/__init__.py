# stage_1_parsing/__init__.py
"""
Optimized Stage 1 parsing package entrypoints.
Exposes process_folder and save_parsed_data.
"""
from .process_files import process_folder, save_parsed_data

__all__ = ["process_folder", "save_parsed_data"]
