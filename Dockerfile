# Use official lightweight Python image
FROM python:3.10-slim

# Set work dir
WORKDIR /app

# Install system dependencies for pandas, numpy, openpyxl, fitz, etc.
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libxml2-dev \
    libxslt1-dev \
    libjpeg-dev \
    zlib1g-dev \
    libfreetype6-dev \
    liblcms2-dev \
    libwebp-dev \
    tcl8.6-dev \
    tk8.6-dev \
    ghostscript \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Copy files
COPY . .

# Install dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Expose port
ENV PORT=8000
EXPOSE 8000

# Run application
CMD gunicorn app:app --bind 0.0.0.0:$PORT
