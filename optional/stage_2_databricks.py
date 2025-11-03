# import os
# import tkinter as tk
# from tkinter import filedialog, messagebox
# from pyspark.sql import SparkSession


# def upload_to_databricks(df, table_name):
#     """Uploads parsed file data to a Databricks (or local Spark) table."""
#     try:
#         spark = SparkSession.builder.appName("Stage2_DatabricksUpload").getOrCreate()
#         spark_df = spark.createDataFrame(df)
#         spark_df.write.mode("overwrite").saveAsTable(table_name)
#         messagebox.showinfo("Success", f"✅ Data uploaded successfully to table: '{table_name}'")
#     except Exception as e:
#         messagebox.showerror("Upload Error", f"❌ Failed to upload data to Databricks: {e}")
#     finally:
#         spark.stop()


# def run_stage_2():
#     """Main logic that connects GUI input with parsing and Databricks upload."""
#     folder_path = folder_path_var.get()
#     table_name = table_name_var.get()
#     output_name = output_name_var.get()

#     if not folder_path or not os.path.exists(folder_path):
#         messagebox.showerror("Error", "❌ Please select a valid folder path.")
#         return

#     if not table_name:
#         table_name = "parsed_files"

#     if not output_name:
#         output_name = "parsed_output"

#     try:
#         # Step 1: Process folder (uses stage 1 parser)
#         parsed_data = process_folder(folder_path)
#         if parsed_data.empty:
#             messagebox.showwarning("No Data", "⚠️ No files were parsed. Check file types.")
#             return

#         # Step 2: Save parsed data locally
#         save_parsed_data(parsed_data, output_name)

#         # Step 3: Upload parsed data to Databricks / DBeaver
#         upload_to_databricks(parsed_data, table_name)

#         messagebox.showinfo("Completed", "🎯 Stage 2 completed successfully!")

#     except Exception as e:
#         messagebox.showerror("Unexpected Error", f"❌ An unexpected error occurred:\n{e}")


# # --- Tkinter GUI Setup ---
# root = tk.Tk()
# root.title("Stage 2 - Databricks Integration")
# root.geometry("500x350")
# root.resizable(False, False)

# # Variables
# folder_path_var = tk.StringVar()
# table_name_var = tk.StringVar()
# output_name_var = tk.StringVar()


# def browse_folder():
#     folder_selected = filedialog.askdirectory(title="Select Folder Containing Files")
#     if folder_selected:
#         folder_path_var.set(folder_selected)


# # Labels and Inputs
# tk.Label(root, text="📂 Folder Path:", font=("Arial", 11)).pack(pady=5)
# tk.Entry(root, textvariable=folder_path_var, width=50).pack()
# tk.Button(root, text="Browse", command=browse_folder).pack(pady=3)

# tk.Label(root, text="🗄️ Table Name (Databricks/DBeaver):", font=("Arial", 11)).pack(pady=5)
# tk.Entry(root, textvariable=table_name_var, width=40).pack()

# tk.Label(root, text="💾 Output File Name (without .txt):", font=("Arial", 11)).pack(pady=5)
# tk.Entry(root, textvariable=output_name_var, width=40).pack()

# tk.Button(
#     root,
#     text="🚀 Run Stage 2",
#     command=run_stage_2,
#     bg="#4CAF50",
#     fg="white",
#     font=("Arial", 12, "bold"),
#     width=20,
#     pady=5
# ).pack(pady=20)

# tk.Label(root, text="Made with ❤️ | Databricks + DBeaver Integration", fg="gray").pack(pady=5)

# root.mainloop()
