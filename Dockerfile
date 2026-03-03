FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY *.py ./
COPY config.example.py config.py

# Create directories
RUN mkdir -p logs data reports

# Non-root user for security
RUN useradd -m -r trader && chown -R trader:trader /app
USER trader

# Default command (overridden by docker-compose)
CMD ["python", "fetch_to_db.py"]
