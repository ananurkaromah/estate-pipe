import dlt
import requests
import csv
import logging
import json
import os
from kestra import Kestra

logging.basicConfig(level=logging.INFO, format='%(message)s')

PPD_HEADERS = [
    "transaction_id", "price", "transfer_date", "postcode", "property_type",
    "old_new", "duration", "paon", "saon", "street", "locality",
    "town_city", "district", "county", "ppd_category", "record_status"
]

@dlt.resource(name="uk_land_registry_transactions_raw", write_disposition="append")
def fetch_land_registry_data():
    url = os.environ["CSV_URL"]

    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            lines = (line.decode('utf-8') for line in r.iter_lines())

            #Safe header skip
            try:
                next(lines)
            except StopIteration:
                logging.warning(json.dumps({
                    "event": "empty_file",
                    "message": "CSV file is empty"
                }))
                return

            reader = csv.DictReader(lines, fieldnames=PPD_HEADERS)

            count = 0
            for row in reader:
                row["transaction_id"] = row["transaction_id"].replace("{", "").replace("}", "")
                yield row
                count += 1

            logging.info(json.dumps({
                "event": "ingestion_success",
                "rows_yielded": count
            }))

    except Exception as e:
        logging.error(json.dumps({
            "event": "ingestion_failed",
            "error": str(e)
        }))
        raise


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="land_registry_ext",
        destination="postgres",
        dataset_name="raw_estate_schema"
    )

    load_info = pipeline.run(fetch_land_registry_data())
    Kestra.counter("dlt_packages_loaded", len(load_info.loads_ids))