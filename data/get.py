import kaggle
import os
import requests
import pandas as pd
import json

# Download latest version to 'data' dir
kaggle.api.authenticate()
kaggle.api.dataset_download_files("yelp-dataset/yelp-dataset", path=os.getcwd(), unzip=True)

def get_place_data(latitude_decimal, longitude_decimal, radius_meters):
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
                    "latitude": latitude_decimal,
                    "longitude": longitude_decimal
                },
                "radius": radius_meters
            }
        }
    }
    r = requests.post(url=base_url, json=payload, headers=headers)
    return r.text

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
        
        # Can't make this call for every business due to request quota restrictions
        filtered_json['nearby_places_raw'] = get_place_data(filtered_json['latitude'], filtered_json['longitude'], 500)
        yield filtered_json
    
with open('yelp_academic_dataset_business.json', 'r', encoding='utf-8') as file:
    first_3_rows = [next(read_business_dataset(file)) for i in range(3)]
    df = pd.DataFrame(first_3_rows)
    print(df.head())
