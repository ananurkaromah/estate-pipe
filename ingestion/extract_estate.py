import dlt
import requests
import os
import logging
import json
from kestra import Kestra

logging.basicConfig(level=logging.INFO, format='%(message)s')

@dlt.resource(name="real_estate_listings", write_disposition="append")
def fetch_properties():
    url = os.environ["API_URL"]
    headers = {
        "X-RapidAPI-Key": os.environ["RAPIDAPI_KEY"],
        "X-RapidAPI-Host": os.environ["API_HOST"]
    }
    querystring = {
        "identifier": os.environ["REGION_ID"],
        "sort_by": "HighestPrice",
        "search_radius": "0.0"
    }

    offset = 0
    while True:
        querystring["offset"] = str(offset)
        
        try:
            # Added timeout as requested
            response = requests.get(url, headers=headers, params=querystring, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            properties = data.get("properties", [])
            
            # Break the loop and handle empty results
            if not properties:
                if offset == 0:  # Only log warning if the very first page is empty
                    logging.warning(json.dumps({
                        "event": "no_data",
                        "message": "API returned empty result"
                    }))
                    yield from []
                break
            
            logging.info(json.dumps({
                "event": "api_fetch_success",
                "region": querystring["identifier"],
                "offset": offset,
                "properties_found": len(properties)
            }))
            
            # Python-side JSON flattening and yielding
            for prop in properties:
                prop["price_amount"] = prop.get("price", {}).get("amount")
                yield prop
            
            # Advance pagination
            offset += len(properties)

        except requests.exceptions.RequestException as e:
            logging.error(json.dumps({"event": "api_fetch_failed", "error": str(e)}))
            raise

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="estate_ext", 
        destination="postgres", 
        dataset_name="raw_estate_schema"
    )
    load_info = pipeline.run(fetch_properties())
    Kestra.counter("dlt_packages_loaded", len(load_info.loads_ids))