# Files Parsing & Databricks Upload Tool

A web-based application that allows users to upload **PDF, DOCX, and text files**, parse their contents, preview results, and upload parsed data directly to **Databricks SQL Warehouse**.

🚀 **Live Demo:**  
https://web-production-cc19c.up.railway.app/

---

## ✨ Features

- Upload PDF / DOCX / TXT files
- Parse and preview extracted content
- Download parsed output
- Upload parsed data to Databricks as Base64-safe storage
- Browse Databricks tables
- Preview table contents with automatic Base64 decoding
- Delete Databricks tables
- Clean UI and responsive design

---

## 🧰 Tech Stack

- **Python** (Flask)
- **Databricks SQL Connector**
- **HTML, CSS, JavaScript**
- **Railway.app** (Deployment)
- **Base64-safe data encoding**
- **FFmpeg (optional for media parsing)**

---

## 🚀 Deployment

The project is deployed using **Railway** with automatic builds from GitHub.

To deploy manually:

```bash
railway login
railway init
railway up
```

---

## ⚙️ Environment Variables

The following environment variables are required:

```
DATABRICKS_SERVER=
DATABRICKS_HTTP_PATH=
DATABRICKS_TOKEN=
```

---

## 📦 Installation (Local Development)

```bash
git clone <repo-url>
cd Files-Parsing
pip install -r requirements.txt
python app.py
```

Visit:

```
http://127.0.0.1:5000/
```

---

## 📁 Project Structure

```
/static
/templates
/user_uploads
/parsed_outputs
app.py
databricks_utils.py
requirements.txt
README.md
```

---

## 🧪 Databricks Table Format

Each parsed upload is stored with this schema:

| column          | type      |
|----------------|----------|
| file_name      | STRING   |
| file_type      | STRING   |
| content_base64 | STRING   |
| parsed_at      | TIMESTAMP |

---

## 👨‍💻 Author

Made by **KS**

---

## 🌟 License

MIT License — free to use and modify.
