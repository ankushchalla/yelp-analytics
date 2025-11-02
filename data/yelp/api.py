from typing import Iterable, List
from collections.abc import Generator
from ..types import BusinessRecord, to_address, to_attribute_list, to_category_list
import kaggle
import os
import pandas as pd
import json
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data.yelp.api")

# Download latest version to working directory
# kaggle.api.authenticate()
# kaggle.api.dataset_download_files("yelp-dataset/yelp-dataset", path=os.getcwd(), unzip=True)

class YelpService():
    def __init__(self) -> None:
        data_dir = os.path.join(os.getcwd(), 'data', 'yelp')
        self.reviews_file_path = os.path.join(data_dir, 'yelp_academic_dataset_review.json')
        self.businesses_file_path = os.path.join(data_dir, 'yelp_academic_dataset_business.json')

    def download_yelp_data(self):
        if not (os.path.exists(self.reviews_file_path) and os.path.exists(self.businesses_file_path)):
            logger.info("YELP_DATA_DOWNLOAD_INITIATED")
            kaggle.api.authenticate()
            kaggle.api.dataset_download_files("yelp-dataset/yelp-dataset", path=os.getcwd(), unzip=True)
            logger.info(f"YELP_DATA_DOWNLOAD_COMPLETE reviews_file_path={self.reviews_file_path} businesses_file_path={self.businesses_file_path}")
        else:
            logger.info("YELP_DATA_DOWNLOAD_SKIP")


    def read_business_dataset(self, file_object) -> Generator[BusinessRecord, None, None]:
        while True:
            line = file_object.readline()

            # break if EOF reached
            if not line:
                break

            json_line = json.loads(line.strip())
            yield BusinessRecord(to_address(json_line), to_attribute_list(json_line), to_category_list(json_line))

            
