from data.yelp.api import YelpService
from data.big_query.api import BigQueryClient
import os

class Pipeline:
    def __init__(self) -> None:
        self.yelp_service = YelpService()
        self.big_query_client = BigQueryClient()
        self.yelp_download_path = file_path = os.path.join(os.getcwd(), 'data', 'yelp', 'yelp_academic_dataset_business.json')

    def extract(self):
        self.yelp_service.download_yelp_data()

    def load_slice(self, num):
        with open(self.yelp_download_path, 'r', encoding='utf-8') as file:            
            first_n_rows = [next(self.yelp_service.read_business_dataset(file)) for i in range(num)]
            address_data = [tuple(x.address) for x in first_n_rows]
            self.big_query_client.load_businesses(address_data)
            if self.big_query_client.errors != []:
                print(self.big_query_client.errors)

pipeline = Pipeline()
pipeline.extract()
pipeline.load_slice(10)


