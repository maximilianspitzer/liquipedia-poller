FROM python:3.12-slim

WORKDIR /app

# Install system dependencies and PostgreSQL client
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Ensure correct line endings for shell scripts
RUN sed -i 's/\r$//' entrypoint.sh && \
    chmod +x entrypoint.sh

# Create non-root user for security but give access to PostgreSQL client
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Use the entrypoint script
ENTRYPOINT ["./entrypoint.sh"]