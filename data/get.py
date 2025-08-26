import kaggle
import os
import requests
import pandas as pd
import json

# Download latest version to 'data' dir
# kaggle.api.authenticate()
# kaggle.api.dataset_download_files("yelp-dataset/yelp-dataset", path=os.getcwd(), unzip=True)

# Example HTTP request for Place data (will be fetched lazily to minimize costs)
API_KEY = os.getenv('GOOGLE_API_KEY')
base_url = "https://places.googleapis.com/v1/places:searchNearby"
headers = {
    "X-Goog-Api-Key": API_KEY,
    "X-Goog-FieldMask": "places.displayName,places.types,places.rating"
}
payload = {
    "maxResultCount": 10,
    "locationRestriction": {
        "circle": {
            "center": {
                "latitude": 37.7937,
                "longitude": -122.3965
            },
            "radius": 500.0
        }
    }
}
# Commented out actual request to conserve requests quota during dev phase
# r = requests.post(url=base_url, json=payload, headers=headers)
# print(r.text)

# Example: Reading first 5 rows of business dataset
def read_business_dataset(file_object):
    targeted_keys = {'business_id', 'name', 'address', 'city', 'state', 'postal_code', 'latitude', 'longitude', 'stars'}
    while True:
        line = file_object.readline()

        # break if EOF reached
        if not line:
            break

        full_json = json.loads(line.strip())
        filtered_json = {key: full_json[key] for key in full_json if key in targeted_keys}

        yield filtered_json
    
with open('yelp_academic_dataset_business.json', 'r', encoding='utf-8') as file:
    first_5_rows = [next(read_business_dataset(file)) for i in range(5)]
    df = pd.DataFrame(first_5_rows)
    print(df.head())
