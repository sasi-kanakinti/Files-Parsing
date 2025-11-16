# ---------------------------
# Production Dockerfile
# ---------------------------

FROM python:3.10-slim

# Create working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Expose port
EXPOSE 5000

# Use gunicorn in production
CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app"]
