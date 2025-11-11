from itertools import islice
from data.yelp.api import YelpService
from data.big_query.api import BigQueryClient
import os

class Pipeline:
    def __init__(self) -> None:
        self.yelp_service = YelpService()
        self.big_query_client = BigQueryClient()

    def extract(self):
        self.yelp_service.download_yelp_data()

    def load_slice(self, num):
        yelp_dataset_gen = self.yelp_service.read_review_dateset()     
        first_n_rows = list(islice(yelp_dataset_gen, num))
        self.big_query_client.load_review_records(first_n_rows)
        if self.big_query_client.errors != []:
            print(self.big_query_client.errors)

pipeline = Pipeline()
pipeline.extract()
pipeline.load_slice(10)

