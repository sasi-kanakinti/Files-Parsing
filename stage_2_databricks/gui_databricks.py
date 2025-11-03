import tkinter as tk
from tkinter import filedialog, messagebox
import os
from stage_2_databricks.db_utils import upload_parsed_records

# ---------------------------------------------------------------------------
# GUI FUNCTIONS
# ---------------------------------------------------------------------------

def browse_file():
    """Open file picker and set path in entry field."""
    file_path = filedialog.askopenfilename(
        title="Select a file to upload",
        filetypes=[("All Files", "*.*")]
    )
    if file_path:
        entry_file_path.delete(0, tk.END)
        entry_file_path.insert(0, file_path)


def upload_file():
    """Handle file upload to Databricks."""
    file_path = entry_file_path.get()
    table_name = entry_table_name.get().strip()

    if not file_path:
        messagebox.showerror("Error", "Please select a file first.")
        return

    if not table_name:
        messagebox.showerror("Error", "Please enter a table name.")
        return

    try:
        # Read file contents
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        # Prepare record
        record = [{
            "file_name": os.path.basename(file_path),
            "file_type": os.path.splitext(file_path)[1].lstrip('.'),
            "content": content
        }]

        # Upload using the db_utils function
        upload_parsed_records(record, table_name)

        messagebox.showinfo("Success", f"✅ File uploaded to table '{table_name}' successfully!")

    except Exception as e:
        messagebox.showerror("Upload Failed", f"❌ {e}")


# ---------------------------------------------------------------------------
# GUI LAYOUT
# ---------------------------------------------------------------------------

root = tk.Tk()
root.title("Databricks File Uploader")
root.geometry("500x300")
root.resizable(False, False)

# File selection
tk.Label(root, text="Select File:", font=("Segoe UI", 11)).pack(pady=5)
entry_file_path = tk.Entry(root, width=55)
entry_file_path.pack(pady=2)
tk.Button(root, text="Browse", command=browse_file).pack(pady=4)

# Table name input
tk.Label(root, text="Target Table Name:", font=("Segoe UI", 11)).pack(pady=5)
entry_table_name = tk.Entry(root, width=40)
entry_table_name.pack(pady=2)

# Upload button
tk.Button(
    root,
    text="Upload to Databricks",
    command=upload_file,
    bg="#0078D7",
    fg="white",
    font=("Segoe UI", 11, "bold"),
    padx=10,
    pady=5
).pack(pady=20)

root.mainloop()