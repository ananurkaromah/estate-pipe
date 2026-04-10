import dlt
import requests

# 1. dlt securely fetches the API key from .dlt/secrets.toml
@dlt.resource(name="real_estate_listings", write_disposition="replace")
def fetch_properties(api_key: str = dlt.secrets.value):
    print("Fetching real estate data from RapidAPI...")
    
    # URL and Host for the UK Rightmove API
    url = "https://uk-real-estate-rightmove.p.rapidapi.com/buy/property-for-sale"
    host = "uk-real-estate-rightmove.p.rapidapi.com"
    
    # The correct query for the UK API
    querystring = {"identifier":"REGION^1036","sort_by":"HighestPrice","search_radius":"0.0"}
    
    # Headers handle your secret authentication
    headers = {
        "x-rapidapi-key": api_key, 
        "x-rapidapi-host": host,
        "Content-Type": "application/json"
    }
    
    # We pass headers to headers=, and querystring to params=
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status() 
    
    data = response.json()
    
    # Safely yield the data (Rightmove usually houses listings under 'properties')
    yield data.get("properties", data)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="estate_extraction",
        destination="postgres",
        dataset_name="raw_estate_schema" 
    )
    
    print("⏳ Starting the dlt pipeline...")
    load_info = pipeline.run(fetch_properties())
    
    print("Pipeline finished successfully!")
    print(load_info)