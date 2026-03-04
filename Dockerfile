FROM python:3.11-slim

WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY *.py ./
COPY schema.sql ./
COPY config.example.py config.py

# Create directories
RUN mkdir -p logs data reports

# Non-root user for security
RUN useradd -m -r trader && chown -R trader:trader /app
USER trader

# Default command (overridden by docker-compose)
CMD ["python", "fetch_to_db.py"]
