# Use stable Python version (avoid bleeding-edge issues)
FROM python:3.11-slim

# Prevent Python from writing .pyc files & enable logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies (Java required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre \
        procps \
        gcc \
        python3-dev \
        libpq-dev \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip first (important for dependency resolver stability)
RUN pip install --upgrade pip

# Install all dependencies with timeout + retries
RUN pip install \
    --default-timeout=1000 \
    --retries=10 \
    --no-cache-dir \
    "dlt[postgres]" \
    requests \
    kestra \
    faker \
    pyspark==3.5.1 \
    psycopg2-binary \
    great_expectations \
    sqlalchemy

# Set working directory (important for Kestra execution)
WORKDIR /app