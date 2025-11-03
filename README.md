# 🧠 Document Parser + Databricks Integration

![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)
![Databricks](https://img.shields.io/badge/Databricks-Integration-orange.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![DBeaver](https://img.shields.io/badge/Compatible-DBeaver-blue.svg)

This project is a Python-based **document processing and analytics pipeline** that extracts text and structured data from **PDF**, **Word**, and **Excel** files, then uploads it into a **Databricks Delta table** for querying and analysis.  
It also saves a local backup of the parsed data in an `outputs/` folder.  
Using **DBeaver**, you can visually explore, validate, and run SQL queries on the Databricks tables without writing any code.

---

## 🚀 Features

- 🔍 **Automatic File Detection** – Handles PDF, DOCX, and XLSX files seamlessly
- 🧾 **Unified Data Format** – Converts all file contents into a structured table
- ☁️ **Databricks Integration** – Uploads parsed results directly to Delta tables
- 💾 **Local Backup** – Saves outputs to an `outputs/` folder for reference
- 🧠 **DBeaver Ready** – Connect and query Databricks data visually

---

## 🧩 Folder Structure

project_root/
│
├── files/ # Input documents (PDF, DOCX, XLSX)
├── outputs/ # Local parsed outputs
├── parse_to_databricks.py # Main parsing + Databricks upload script
├── requirements.txt # Dependencies
└── README.md # Project documentation

---

## ⚙️ Setup & Installation

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/sasi-kanakinti/Chat-bot.git
cd Chat-bot


```

2️⃣ Install Dependencies

pip install -r requirements.txt

```

3️⃣ (Optional) Configure Databricks Connect

If you’re running locally, link your Python environment to Databricks:

databricks-connect configure

Provide:

    Databricks Workspace URL

    Personal Access Token

    Cluster or SQL Warehouse ID

```

4️⃣ Run the Parser
python parse_to_databricks.py

Parsed results will appear both:

    In your Databricks Delta table (parsed_files)

    In your local folder: outputs/parsed_output.txt

```


🧱 Databricks Output Example

After upload, query your table in Databricks or DBeaver:
sql:

SELECT file_name, LENGTH(content) AS text_length
FROM parsed_files
ORDER BY text_length DESC;

| file_name  | file_type | text_length |
| ---------- | --------- | ----------- |
| report.pdf | .pdf      | 1423        |
| notes.docx | .docx     | 986         |
| data.xlsx  | .xlsx     | 251         |

```

🖥️ DBeaver Integration

DBeaver lets you visually browse your Databricks tables.

1. Open DBeaver → click New Connection

2. Select Databricks

3. Fill in:

   Workspace URL

   HTTP Path (SQL Warehouse or Cluster)

   Access Token

4. Click Test Connection → then Finish

Now you can explore and query your parsed_files table directly in DBeaver’s SQL editor ✅

```

📦 Requirements

| Library                | Purpose                                        |
| ---------------------- | ---------------------------------------------- |
| **PyMuPDF**            | PDF parsing and text extraction                |
| **python-docx**        | Word (.docx) document reading                  |
| **pandas**             | Data organization and transformation           |
| **openpyxl**           | Excel file support for pandas                  |
| **pyspark**            | Databricks connection and Delta table creation |
| **databricks-connect** | Local Spark–Databricks integration             |
| **pyarrow**            | Optimized data transfer (optional)             |

```

📜 License

This project is licensed under the MIT License — you are free to use, modify, and distribute it with attribution.

```

👨‍💻 Author

Sasi Kanakinti
💼 [GitHub Profile](https://github.com/sasi-kanakinti)

```

```

```
