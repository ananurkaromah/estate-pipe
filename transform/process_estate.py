import os
import psycopg2
import logging
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp
from kestra import Kestra

logging.basicConfig(level=logging.INFO, format='%(message)s')

try:
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    db_host = os.environ['DB_HOST']
    db_port = os.environ['DB_PORT']
    db_name = os.environ['DB_NAME']
    db_url = os.environ['DB_URL']

    #Ensure schema exists
    pg_conn_str = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    conn = psycopg2.connect(pg_conn_str)
    conn.autocommit = True
    conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS curated_estate_schema;")
    conn.close()

    spark = SparkSession.builder \
        .appName("EstateTx") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    db_props = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    #Day-0 handling
    try:
        raw_df = spark.read.jdbc(
            url=db_url,
            table="raw_estate_schema.uk_land_registry_transactions_raw",
            properties=db_props
        )
    except Exception as e:
        if "relation" in str(e) and "does not exist" in str(e):
            logging.warning(json.dumps({
                "event": "day_0_handling",
                "message": "Source table missing. Exiting cleanly."
            }))
            spark.stop()
            sys.exit(0)
        else:
            raise e

    #Convert date early for safe comparison
    raw_df = raw_df.withColumn(
        "transfer_date_ts",
        to_timestamp(col("transfer_date"), "yyyy-MM-dd")
    )

    #Watermark (incremental load)
    try:
        watermark_query = "(SELECT MAX(transfer_date_ts) as max_date FROM curated_estate_schema.clean_uk_properties) as t"
        watermark_df = spark.read.jdbc(url=db_url, table=watermark_query, properties=db_props)
        last_date = watermark_df.collect()[0]["max_date"]

        if last_date:
            raw_df = raw_df.filter(col("transfer_date_ts") > last_date)

    except Exception:
        logging.warning(json.dumps({
            "event": "no_watermark",
            "message": "No previous data found, doing full load"
        }))

    #Transform
    clean_df = raw_df \
        .filter(col("price").isNotNull()) \
        .withColumn("price_numeric", col("price").cast("double")) \
        .withColumn("year", col("transfer_date").substr(1, 4)) \
        .withColumn("ingested_at", current_timestamp())

    #Deduplicate
    if "transaction_id" in raw_df.columns:
        clean_df = clean_df.dropDuplicates(["transaction_id"])

    #Cache to avoid recomputation
    clean_df = clean_df.cache()
    row_count = clean_df.count()

    logging.info(json.dumps({
        "event": "pipeline_metrics",
        "rows_clean": row_count
    }))

    if row_count > 0:
        clean_df.write \
            .mode("append") \
            .option("batchsize", 5000) \
            .option("numPartitions", 2) \
            .option("rewriteBatchedStatements", "true") \
            .jdbc(
                url=db_url,
                table="curated_estate_schema.clean_uk_properties",
                properties=db_props
            )

        Kestra.counter("spark_rows_cleaned", row_count)

        logging.info(json.dumps({
            "event": "transformation_success",
            "rows_written": row_count
        }))
    else:
        logging.info(json.dumps({
            "event": "transformation_skipped",
            "message": "No new rows to process"
        }))

    spark.stop()

except Exception as e:
    logging.error(json.dumps({
        "event": "spark_failed",
        "error": str(e)
    }))
    raise