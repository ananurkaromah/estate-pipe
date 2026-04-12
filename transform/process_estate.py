import os
import psycopg2
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, current_timestamp
from kestra import Kestra

logging.basicConfig(level=logging.INFO, format='%(message)s')

try:
    logging.info(json.dumps({"event": "spark_init"}))
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    db_host = os.environ['DB_HOST']
    db_port = os.environ['DB_PORT']
    db_name = os.environ['DB_NAME']
    db_url = os.environ['DB_URL']

    pg_conn_str = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    conn = psycopg2.connect(pg_conn_str)
    conn.autocommit = True
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS curated_estate_schema;")
    conn.close()

    spark = SparkSession.builder \
        .appName("EstateTx") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    db_props = {"user": db_user, "password": db_password, "driver": "org.postgresql.Driver"}

    raw_df = spark.read.jdbc(url=db_url, table="raw_estate_schema.real_estate_listings", properties=db_props)

    # --- High Watermark / Incremental Filtering in Spark ---
    try:
        # Query the curated table to find the highest ID we have already processed
        watermark_query = "(SELECT MAX(id) as max_id FROM curated_estate_schema.clean_properties) as t"
        watermark_df = spark.read.jdbc(url=db_url, table=watermark_query, properties=db_props)
        last_processed_id = watermark_df.collect()[0]["max_id"]
        
        if last_processed_id:
            # Filter the raw dataframe to only process genuinely new records
            raw_df = raw_df.filter(col("id") > last_processed_id)
            logging.info(json.dumps({"event": "watermark_applied", "last_processed_id": last_processed_id}))
    except Exception as e:
        # The table likely doesn't exist yet (first run)
        logging.info(json.dumps({"event": "no_watermark_found", "message": "Proceeding with full load"}))

    cols = raw_df.columns
    price_col = next((c for c in ["price__amount", "price"] if c in cols), None)
    type_col = next((c for c in ["property_sub_type", "propertySubType", "type"] if c in cols), None)

    if not price_col or not type_col:
        raise Exception("Required columns not found! Schema drift detected.")

    clean_df = raw_df.filter(col(price_col).isNotNull()) \
                     .withColumn("price_numeric", col(price_col).cast("double")) \
                     .withColumn("property_type", col(type_col)) \
                     .withColumn("ingested_at", current_timestamp())
                     
    # --- Deduplication Protection ---
    clean_df = clean_df.dropDuplicates(["id"])

    clean_df.cache()
    row_count = clean_df.count()

    if row_count == 0:
        logging.info(json.dumps({"event": "transformation_skipped", "message": "No new data to process"}))
    else:
        logging.info(json.dumps({"event": "data_cleaned", "rows": row_count}))
        Kestra.counter("spark_rows_cleaned", row_count)
        
        clean_df.write.jdbc(url=db_url, table="curated_estate_schema.clean_properties", mode="append", properties=db_props)
    
    clean_df.unpersist()
    spark.stop()
    logging.info(json.dumps({"event": "spark_complete", "status": "success"}))

except Exception as e:
    logging.error(json.dumps({"event": "spark_failed", "error": str(e)}))
    raise