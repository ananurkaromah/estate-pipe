# UK Real Estate Data Pipeline (ELT)

An automated, containerized ELT pipeline that processes 90,000+ monthly property transaction records from the UK Land Registry. 

This project was built to practice robust data engineering principles, focusing on modularity, memory efficiency, and strict data quality checks.

## Workflow
```jsx
[🌐 UK Land Registry (Amazon S3)] --> CSV File (Tens of thousands of rows)
       │
       ▼
[🛠️ Kestra Task 1: Ingestion]  --> runs `ingestion/extract_estate.py`
       │                           - Streams CSV to avoid OOM crashes
       │                           - Uses `dlt` (Data Load Tool)
       ▼
[🗄️ PostgreSQL (Raw)]          --> Table: raw_estate_schema.uk_land_registry_raw
       │                           - Data type: All strings (raw extraction)
       ▼
[🛠️ Kestra Task 2: Transform]  --> runs `transform/process_estate.py`
       │                           - Apache PySpark reads raw table
       │                           - Casts dates to Timestamps, prices to Double
       │                           - Deduplicates by `transaction_id`
       │                           - Batches JDBC writes
       ▼
[🗄️ PostgreSQL (Curated)]      --> Table: curated_estate_schema.clean_uk_properties
       │                           - Data type: Analytical/Cleaned
       ▼
[🛠️ Kestra Task 3: Quality]    --> runs `quality/validate_estate.py`
                                   - Great Expectations connects to Curated table
                                   - Asserts prices > £1, no null IDs, unique IDs
                                   - Logs Success/Failure
```

## Folder Tree
```jsx
estate-pipe/
│
├── docker-compose.yaml        # Infrastructure: Kestra, PostgreSQL, pgAdmin
├── Dockerfile                 # Custom estate-worker image (PySpark 3.5.1, GX 0.18.19)
│
├── orchestration/
│   └── estate_flow.yml        # The Kestra DAG YAML (Triggers, Tasks, Env Vars)
│
├── ingestion/
│   └── extract_estate.py      # The `dlt` pipeline script for UK Land Registry
│
├── transform/
│   └── process_estate.py      # The PySpark cleaning and loading script
│
└── quality/
    └── validate_estate.py     # The Great Expectations data quality test suite
```


## Architecture
* **Orchestration:** Kestra (Daily Cron Schedule)
* **Ingestion:** Python / `dlt` (Data Load Tool) 
* **Transformation:** Apache PySpark
* **Data Quality:** Great Expectations
* **Storage:** PostgreSQL
* **Infrastructure:** Docker Compose

## Key Engineering Features
* **Memory-Efficient Streaming:** The ingestion script streams the massive upstream CSV row-by-row rather than loading it entirely into memory, preventing OOM errors.
* **Idempotent Incremental Loading:** PySpark queries the database for the latest `transfer_date` watermark, ensuring only net-new records are processed on subsequent runs.
* **Day-0 Guardrails:** Both PySpark and Great Expectations are wrapped in defensive `try/except` blocks. If upstream data is missing or the database is unseeded, the pipeline exits gracefully instead of crashing.
* **Optimized Writes:** PySpark writes to PostgreSQL using `.cache()`, JDBC batching, and `rewriteBatchedStatements=true` to drastically reduce database I/O time.

## How to Run Locally
1. Clone this repository.
2. Build the worker image: `docker build -t estate-worker:latest .`
3. Spin up the infrastructure: `docker compose up -d`
4. Access Kestra at `http://localhost:8080` and execute the `estate_flow.yml` DAG.