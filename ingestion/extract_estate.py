import dlt
import requests
import os
import time
import json

@dlt.resource(name="real_estate_listings", write_disposition="merge", primary_key="id")
def fetch_properties(api_key: str = dlt.secrets.value):
    url = "https://uk-real-estate-rightmove.p.rapidapi.com/buy/property-for-sale"
    host = "uk-real-estate-rightmove.p.rapidapi.com"
    
    headers = {
        "x-rapidapi-key": api_key, 
        "x-rapidapi-host": host,
        "Content-Type": "application/json"
    }
    
    offset = 0          
    max_pages = 3       
    current_page = 0
    total_rows = 0
    
    print(f"[METADATA] {json.dumps({'status': 'Init', 'max_pages_limit': max_pages})}")
    
    while current_page < max_pages:
        querystring = {
            "identifier": "REGION^1036",
            "sort_by": "HighestPrice",
            "search_radius": "0.0",
            "offset": str(offset) 
        }
        
        # --- API RESILIENCY & RETRY LOGIC ---
        max_retries = 3
        success = False
        
        for attempt in range(max_retries):
            try:
                # Added explicit 10-second timeout
                response = requests.get(url, headers=headers, params=querystring, timeout=10)
                
                # Smart 429 Handling: Trust the server's requested wait time, default to 5s
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    print(f"[METADATA] {json.dumps({'status': '429 Rate Limited', 'action': f'sleeping {retry_after}s'})}")
                    time.sleep(retry_after)
                    continue 
                
                response.raise_for_status() 
                data = response.json()
                success = True
                break # Success! Break out of the retry loop
                
            except requests.exceptions.RequestException as e:
                # Exponential Backoff Strategy for timeouts or 500 server errors
                sleep_time = min(2 ** attempt, 30)
                print(f"[METADATA] {json.dumps({'status': 'Request Failed', 'attempt': attempt + 1, 'error': str(e), 'action': f'sleeping {sleep_time}s'})}")
                time.sleep(sleep_time)
                
        if not success:
            print(f"[METADATA] {json.dumps({'status': 'Fatal Error', 'reason': 'Max retries exceeded for page. Stopping pipeline.'})}")
            break # Stop pagination entirely if the API is completely dead

        # --- PAGINATION & EXTRACTION LOGIC ---
        properties = data.get("properties", [])
        page_size = len(properties)
        
        if page_size == 0:
            print(f"[METADATA] {json.dumps({'status': 'Ended Early', 'reason': f'No properties at offset {offset}'})}")
            break
            
        yield properties
        
        total_rows += page_size
        current_page += 1
        
        log_data = {
            "status": "Success",
            "page": current_page,
            "rows_this_page": page_size,
            "total_rows_extracted": total_rows,
            "next_offset": offset + page_size
        }
        print(f"[METADATA] {json.dumps(log_data)}")
        
        offset += page_size 

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="estate_extraction",
        destination="postgres",
        dataset_name="raw_estate_schema" 
    )
    
    print("Starting the resilient dlt pipeline...")
    load_info = pipeline.run(fetch_properties())
    
    print(f"[METADATA] {json.dumps({'status': 'Pipeline Complete', 'failed_jobs': load_info.has_failed_jobs})}")