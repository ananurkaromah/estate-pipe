import dlt
import requests
import os
import time
import logging
import json
from kestra import Kestra

# Set up logging (we use format='%(message)s' so the raw JSON prints cleanly)
logging.basicConfig(level=logging.INFO, format='%(message)s')

@dlt.resource(name="real_estate_listings", write_disposition="merge", primary_key="id")
def fetch_properties(last_id=dlt.sources.incremental("id")):
    try:
        url = os.environ["API_URL"]
        host = os.environ["API_HOST"]
        region_id = os.environ["REGION_ID"]
              
        headers = {
            "x-rapidapi-key": os.environ["RAPIDAPI_KEY"], 
            "x-rapidapi-host": host,
            "Content-Type": "application/json"
        }
              
        offset = 0          
        max_pages = 3       
        current_page = 0
        
        last_val = last_id.last_value if hasattr(last_id, 'last_value') else None
              
        while current_page < max_pages:
            querystring = {
                "identifier": region_id,
                "sort_by": "HighestPrice",
                "search_radius": "0.0",
                "offset": str(offset) 
            }
            
            success = False
            for attempt in range(3):
                try:
                    response = requests.get(url, headers=headers, params=querystring, timeout=10)
                    
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", 5))
                        logging.warning(json.dumps({"event": "rate_limited", "status": 429, "sleep_s": retry_after}))
                        time.sleep(retry_after)
                        continue 
                        
                    # --- Explicit 5xx Handling ---
                    if response.status_code >= 500:
                        sleep_time = 2 ** attempt
                        logging.warning(json.dumps({"event": "server_error", "status": response.status_code, "attempt": attempt + 1, "sleep_s": sleep_time}))
                        time.sleep(sleep_time)
                        continue
                          
                    response.raise_for_status() 
                    success = True
                    break
                except requests.exceptions.RequestException as e:
                    sleep_time = 2 ** attempt
                    logging.warning(json.dumps({"event": "request_failed", "error": str(e), "sleep_s": sleep_time}))
                    time.sleep(sleep_time)
            
            if not success:
                logging.error(json.dumps({"event": "fatal_api_error", "message": "Max retries exceeded"}))
                break
                  
            properties = response.json().get("properties", [])
            if not properties:
                break
                  
            for prop in properties:
                prop_id = prop.get("id")
                # --- Robust Incremental Fallback ---
                try:
                    if last_val is None or int(prop_id) > int(last_val):
                        yield prop
                except (ValueError, TypeError):
                    yield prop 
                  
            offset += len(properties)
            current_page += 1
            
            # --- Structured JSON Logging ---
            logging.info(json.dumps({
                "event": "page_loaded",
                "page": current_page,
                "rows_yielded": len(properties)
            }))
            
    except Exception as e:
        logging.error(json.dumps({"event": "extraction_failed", "error": str(e)}))
        raise

if __name__ == "__main__":
    try:
        logging.info(json.dumps({"event": "pipeline_start", "pipeline": "dlt_ingestion"}))
        pipeline = dlt.pipeline(pipeline_name="estate_ext", destination="postgres", dataset_name="raw_estate_schema")
        load_info = pipeline.run(fetch_properties())
              
        Kestra.counter("dlt_packages_loaded", len(load_info.loads_ids))
        logging.info(json.dumps({"event": "pipeline_complete", "status": "success"}))
    except Exception as e:
        logging.error(json.dumps({"event": "pipeline_failed", "error": str(e)}))
        raise