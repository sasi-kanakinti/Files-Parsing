import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
# import pandas as pd
import os

from stage_1_parsing.process_files import process_folder
from stage_1_parsing.process_files import save_parsed_data


def run_stage1_gui():
    # === GUI Actions ===
    def browse_folder():
        folder_selected = filedialog.askdirectory(title="Select Folder to Parse")
        if folder_selected:
            folder_path_var.set(folder_selected)

    def run_parsing():
        folder_path = folder_path_var.get().strip()
        output_name = output_name_var.get().strip()

        if not folder_path or not os.path.exists(folder_path):
            messagebox.showerror("Error", "❌ Please select a valid folder.")
            return

        if not output_name:
            output_name = "parsed_output"

        try:
            parsed_df = process_folder(folder_path)
            if parsed_df.empty:
                messagebox.showwarning("No Data", "⚠️ No supported files found in this folder.")
                return

            save_parsed_data(parsed_df, output_name)

            # Display preview
            preview_box.delete(1.0, tk.END)
            preview_text = parsed_df.head().to_string(index=False)
            preview_box.insert(tk.END, preview_text)

            messagebox.showinfo("Success", f"✅ Parsing completed!\nSaved in 'Outputs/{output_name}.txt'")

        except Exception as e:
            messagebox.showerror("Error", f"❌ Something went wrong:\n{str(e)}")

    # === Window Setup ===
    root = tk.Tk()
    root.title("Stage 1 - File Parsing Tool")
    root.geometry("700x500")
    root.config(bg="#f8f9fa")
    root.resizable(False, False)

    # Variables
    folder_path_var = tk.StringVar()
    output_name_var = tk.StringVar()

    # === UI Components ===
    tk.Label(root, text="📂 Select Folder to Parse", bg="#f8f9fa", font=("Arial", 12, "bold")).pack(pady=(10, 5))
    tk.Entry(root, textvariable=folder_path_var, width=70).pack(pady=5)
    tk.Button(root, text="Browse", command=browse_folder, bg="#007bff", fg="white", width=12).pack(pady=(0, 15))

    tk.Label(root, text="💾 Output File Name (without extension)", bg="#f8f9fa", font=("Arial", 11)).pack(pady=5)
    tk.Entry(root, textvariable=output_name_var, width=40).pack(pady=5)

    tk.Button(
        root,
        text="🚀 Parse Files",
        command=run_parsing,
        bg="#28a745",
        fg="white",
        font=("Arial", 12, "bold"),
        width=20
    ).pack(pady=15)

    tk.Label(root, text="🔍 Preview of Parsed Data", bg="#f8f9fa", font=("Arial", 12, "underline")).pack(pady=(10, 5))

    preview_box = scrolledtext.ScrolledText(root, width=80, height=12, wrap=tk.WORD, font=("Consolas", 10))
    preview_box.pack(padx=10, pady=5)

    tk.Label(root, text="Made with ❤️ | Stage 1 - File Parsing", bg="#f8f9fa", fg="gray").pack(pady=10)

    root.mainloop()


if __name__ == "__main__":
    run_stage1_gui()
