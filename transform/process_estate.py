import os
import psycopg2
import logging
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr
from kestra import Kestra

logging.basicConfig(level=logging.INFO, format='%(message)s')

try:
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

    # Added partition config
    spark = SparkSession.builder \
        .appName("EstateTx") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    db_props = {"user": db_user, "password": db_password, "driver": "org.postgresql.Driver"}

    # Day 0 handling
    try:
        raw_df = spark.read.jdbc(url=db_url, table="raw_estate_schema.real_estate_listings", properties=db_props)
    except Exception as e:
        if "relation" in str(e) and "does not exist" in str(e):
            logging.warning(json.dumps({
                "event": "day_0_handling", 
                "message": "Source table does not exist yet. Assuming 0 rows extracted. Exiting cleanly."
            }))
            spark.stop()
            sys.exit(0)
        else:
            raise e

    # Optimized logging without triggering a full .count() compute
    logging.info(json.dumps({
        "event": "raw_data_loaded",
        "sample": raw_df.limit(5).toPandas().to_dict()
    }))

    # Watermark with explicit fallback
    try:
        watermark_query = "(SELECT MAX(id) as max_id FROM curated_estate_schema.clean_properties) as t"
        watermark_df = spark.read.jdbc(url=db_url, table=watermark_query, properties=db_props)
        last_id = watermark_df.collect()[0]["max_id"]
        
        if last_id:
            raw_df = raw_df.filter(col("id") > last_id)
            
    except Exception:
        logging.warning(json.dumps({
            "event": "no_watermark",
            "message": "No previous data found, doing full load"
        }))

    # Transform using expr() for nested columns
    clean_df = raw_df \
        .filter(col("price").isNotNull()) \
        .withColumn("price_numeric", expr("price.amount").cast("double")) \
        .withColumn("property_type", col("propertySubType")) \
        .withColumn("ingested_at", current_timestamp())
                     
    # Safe Drop Duplicates
    if "id" in raw_df.columns:
        clean_df = clean_df.dropDuplicates(["id"])
    
    row_count = clean_df.count()

    if row_count > 0:
        # Optimized JDBC Write with batching
        clean_df.write \
            .mode("append") \
            .option("batchsize", 1000) \
            .jdbc(url=db_url, table="curated_estate_schema.clean_properties", properties=db_props)
            
        Kestra.counter("spark_rows_cleaned", row_count)
        logging.info(json.dumps({"event": "transformation_success", "rows_written": row_count}))
    else:
        logging.info(json.dumps({"event": "transformation_skipped", "message": "No new rows to process"}))

    spark.stop()

except Exception as e:
    logging.error(json.dumps({"event": "spark_failed", "error": str(e)}))
    raise