# UK Real Estate ELT Pipeline & Analytics Platform

## Project Overview
This project is an end-to-end, containerized ELT (Extract, Load, Transform) data pipeline designed to process monthly property transaction data from the UK Land Registry. Driven by a curiosity to understand large-scale data orchestration and memory-efficient processing, this project transforms raw, unstructured CSV data into an analytics-ready PostgreSQL data warehouse, culminating in a business intelligence dashboard. 

The architecture is built with a focus on idempotency, automated data quality testing, and scalable transformations.

---

## Dataset Description
**Source:** [UK Land Registry - Price Paid Data (PPD)](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads)
* **Format:** Monthly updated CSV (No headers, ~90,000+ rows per month).
* **Content:** Record of every property sale in England and Wales. This is an open government dataset containing actual transaction prices, property types, and geographic locations.

### Data Dictionary (Curated Layer)
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `transaction_id` | `VARCHAR` | Unique identifier for the transaction (Primary Key). |
| `price_numeric` | `DOUBLE` | Sale price of the property in GBP (£). |
| `transfer_date_ts` | `TIMESTAMP` | The exact date the transaction was completed. |
| `property_type` | `VARCHAR` | D (Detached), S (Semi-Detached), T (Terraced), F (Flat/Maisonette). |
| `old_new` | `VARCHAR` | Y (Newly built property), N (Established residential building). |
| `town_city` | `VARCHAR` | The city where the property is located (e.g., LONDON, BIRMINGHAM). |
| `ingested_at` | `TIMESTAMP` | Pipeline metadata recording when the row was processed. |

---

## Problem Statement & Key Research Questions
**The Engineering Challenge:** Extracting large volumes of data from a public endpoint can easily cause Out-Of-Memory (OOM) errors in local environments. The pipeline needed to stream data safely, handle "Day 0" (empty database) scenarios gracefully, and perform incremental loads without duplicating records.

**The Business Questions:**
1. What is the current market composition regarding property types?
2. How are property prices trending over time?
3. Which geographic regions (cities) have the highest transaction volume?
4. What is the ratio of newly built properties to established homes on the market?

---

## Pipeline Architecture & Data Lineage
This diagram illustrates a fully containerized, end-to-end ELT pipeline built to process large-scale UK real estate data safely and efficiently. It follows a structured four-stage workflow:

- Extract & Load: Unstructured CSV data is streamed from the external UK Land Registry and safely loaded into a raw PostgreSQL landing zone using dlt to prevent memory overload.
- Transform: Apache PySpark handles memory-efficient type casting and incremental filtering, writing the processed data into a curated PostgreSQL schema using optimized JDBC batching.
- Validate: Great Expectations runs automatically against the curated schema to enforce strict data quality rules and ensure pipeline integrity.
- Serve: Metabase connects directly to the clean PostgreSQL schema to deliver dynamic, real-time business intelligence dashboards.
<br>
The entire infrastructure—from the extraction scripts to the visualization layer—operates within an isolated Docker environment, ensuring high reproducibility and clean dependency management.

<br>

<img width="2830" height="1472" alt="estate-wf" src="https://github.com/user-attachments/assets/366e672d-4f71-4468-b55d-7a03004b00b5" />
<br>

### Modern data stack:

1. **Extraction:** Python `requests` (streaming) & `dlt` (Data Load Tool)
2. **Orchestration:** Kestra
3. **Data Warehouse:** PostgreSQL
4. **Transformation:** Apache PySpark
5. **Data Quality:** Great Expectations
6. **Visualization:** Metabase


### 1. Data Ingestion

- Uses `requests.get(stream=True)` to process the large CSV file row-by-row, ensuring minimal memory footprint.
- Implements `dlt` to dynamically map raw strings and load them into a `raw_estate_schema`.

### 2. Workflow Orchestration

- **Kestra** manages the workflow via a YAML DAG (`estate_flow.yml`).
- Scheduled to run daily via a Cron trigger (`0 6 * * *`), automating the entire ELT process from extraction to validation.

### 3. Data Warehouse

- **PostgreSQL** serves as the central data store, logically separated into `raw_estate_schema` (landing zone) and `curated_estate_schema` (analytics zone).

### 4. Transformation & Data Enrichment

- **PySpark** handles the heavy lifting:
    - **Type Casting:** Converts raw strings into `Double` and `Timestamp` formats.
    - **Incremental Logic:** Queries the target table for the maximum `transfer_date_ts` (watermark) to only process net-new rows.
    - **Optimization:** Utilizes `.cache()`, JDBC batching, and `rewriteBatchedStatements=true` for lightning-fast database inserts.

### 5. Documentation & Data Quality

- **Great Expectations** runs an automated test suite immediately after transformation.
- Asserts that `transaction_id` is unique and non-null, and that `price_numeric` is greater than £1, ensuring no corrupt data reaches the BI layer.

---

## Strategic Business Insights & Data Visualization

![alt text](<UK real estate executive summary-1.png>)

By connecting Metabase directly to the `curated_estate_schema`, the pipeline translates over 93,000 raw transaction records into actionable market intelligence:

* **Geographic Concentration:** London remains the undisputed epicenter of the UK property market, driving nearly five times the transaction volume of the next most active city (Manchester), followed closely by Bristol and Birmingham.
* **Property Type Distribution:** The market shows a balanced but strong preference for traditional housing over flats. Terraced (27%), Semi-Detached (26%), and Detached (21%) homes make up the vast majority of all transactions.
* **New Build Deficit:** Established properties absolutely dominate the transaction landscape, representing nearly 94% of all sales. Newly built properties make up only a small fraction (6.15%) of the market turnover.
* **Price Volatility & Outlier Detection:** The time-series price trend establishes the baseline market averages over the last two decades while clearly exposing extreme high-value transaction spikes. From a data engineering perspective, these spikes highlight a future opportunity to implement statistical outlier filtering (e.g., separating commercial/bulk sales from standard residential pricing) in the PySpark transformation layer.

---

## Project Folder Structure
```
estate-pipe/
│
├── docker-compose.yaml        # Infrastructure: Kestra, Postgres, pgAdmin, Metabase
├── Dockerfile                 # Custom worker image with PySpark & Great Expectations baked in
│
├── orchestration/
│   └── estate_flow.yml        # Kestra DAG definitions
│
├── ingestion/
│   └── extract_estate.py      # dlt extraction script (handles streams & raw loading)
│
├── transform/
│   └── process_estate.py      # PySpark transformation and incremental loading logic
│
└── quality/
    └── validate_estate.py     # Great Expectations data quality suite
```

---

## Execution Steps & Final Configuration

### Prerequisites

- Docker & Docker Compose
- Minimum 4GB RAM allocated to Docker Engine

### Running the Pipeline Locally

1. **Clone the repository:**Bash
    
    ```jsx
    git clone https://github.com/ananurkaromah/estate-pipe
    cd estate-pipe
    ```
    
2. **Build the Custom Worker Image:**
This bakes the Python code directly into the container for production-grade immutability.Bash
    
    ```jsx
    docker build -t estate-worker:latest .
    ```
    
3. **Deploy the Infrastructure:**
Bash
    
    ```jsx
    docker compose up -d
    ```
    
4. **Trigger the Orchestrator:**
    - Navigate to `http://localhost:8080` (Kestra UI).
    - Create a new flow, paste the contents of `orchestration/estate_flow.yml`, and click **Execute**.

### Verification & Troubleshooting

- **Kestra Logs (`localhost:8080`):** Verify that all three tasks (Ingestion, Transformation, Quality) return a green success status.
- **pgAdmin (`localhost:8085`):** Login with `admin@admin.com` / `root`. Check `estate_db` -> `curated_estate_schema` -> `clean_uk_properties` for populated data.
- **Metabase (`localhost:3000`):** Connect to Postgres and verify the dashboards sync correctly.

---

## Future Work & Scalability

While this architecture is robust for local deployment, a natural next step in the learning journey would be migrating to the cloud:

- **Cloud Storage:** Transitioning the raw landing zone from PostgreSQL to an AWS S3 bucket (Data Lake).
- **Serverless Compute:** Moving the PySpark transformations to AWS EMR or AWS Glue for massive parallel processing.
- **dbt Integration:** Replacing PySpark with `dbt` for SQL-based, version-controlled modeling within a cloud warehouse like BigQuery or Snowflake.

---

## Acknowledgements

- Data provided by **HM Land Registry** (Crown copyright 2024).
- Open-source tools: Kestra, dlt, Apache Spark, Great Expectations, and Metabase.
