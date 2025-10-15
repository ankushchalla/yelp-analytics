import requests
import logging
from utils.api_utils import retry_n_times
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('osm.get')

url = "https://overpass-api.de/api/interpreter"

payload = """
[out:json][timeout:300];

// Define administrative areas (city boundaries)
area["name"="San Francisco"]["boundary"="administrative"]["admin_level"="8"]->.sf;
area["name"="Los Angeles"]["boundary"="administrative"]["admin_level"="8"]->.la;
area["name"="New York City"]["boundary"="administrative"]["admin_level"="8"]->.nyc;

// Combine the three city areas
(.sf;.la;.nyc;)->.searchAreas;

// Query for Points of Interest (POI nodes only)
(
  node["amenity"](area.searchAreas);
  node["shop"](area.searchAreas);
  node["leisure"](area.searchAreas);
  node["tourism"](area.searchAreas);
);
out body;
>;
out skel qt;
"""

headers = {
    "Content-Type": "text/plain"  # Specify that the payload is plain text
}

@retry_n_times(3, "GET_NODES_JSON", lambda: {})
def get_nodes_json():
    r = requests.post(url, data=payload, headers=headers)
    if (r.status_code is 200):
        try: 
            nodes = r.json()
            logging.info(f"RESPONSE_PARSING_SUCCESS numNodes={len(nodes)}")
        except requests.JSONDecodeError as e:
            logger.error(f"RESPONSE_PARSING_FAILURE ex={e}")
    else:
        raise requests.HTTPError(f"{r.status_code} {r.reason}")

