FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre \
        procps \
        gcc \
        python3-dev \
        libpq-dev \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

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
    great_expectations==0.18.19 \
    sqlalchemy

WORKDIR /app

#Bake the Python scripts permanently into the image!
COPY . /app