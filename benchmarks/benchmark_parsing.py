import sys
import time
from stage_1_parsing.process_files import process_folder
import pandas as pd
import os

def time_run(folder, workers=None):
    start = time.time()
    df = process_folder(folder, max_workers=workers)
    end = time.time()
    print(f"workers={workers}, files={len(df)}, time={end-start:.2f}s")
    return df

if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "uploads/sample"
    print("Running benchmark on", folder)
    _ = time_run(folder, workers=1)
    _ = time_run(folder, workers=None)