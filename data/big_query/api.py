from typing import Iterable, Sequence
from google.cloud import bigquery
from google.cloud.bigquery.table import Table
import json
import logging

from data.types import Address, Attribute, BusinessRecord, Category
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data.big_query.api")

# Load data in batches
# Check for nulls/clean data
# Load into BigQuery using CSV loader

class BigQueryClient:
    def __init__(self) -> None:
        self.client = bigquery.Client(project="curious-sandbox-470103-c4")
        self.address_table = self.client.get_table("curious-sandbox-470103-c4.Yelp_Insights_DB.business")
        self.attribute_table = self.client.get_table("curious-sandbox-470103-c4.Yelp_Insights_DB.attribute")
        self.category_table = self.client.get_table("curious-sandbox-470103-c4.Yelp_Insights_DB.category")
        self.errors = []

    def load(self, table: Table, rows_to_insert: Sequence[Address | Attribute | Category]) -> None:
        errors = self.client.insert_rows(table, rows_to_insert)
        if (errors == []):
            logger.info(f"LOAD_SUCCESSFUL table={table} records_loaded={len(rows_to_insert)}")
        else:
            logger.warning(f"LOAD_FAILURE table={table} errors_encountered={len(errors)}")
            self.errors.append(errors)

    def load_business_records(self, business_records: Iterable[BusinessRecord]) -> None:
        addresses = []
        attributes = []
        categories = []
        for business_record in business_records:
            addresses.append(business_record.address)
            if business_record.attributes is not None:
                attributes.extend(business_record.attributes)
            if business_record.categories is not None:
                categories.extend(business_record.categories)
        
        self.load(self.address_table, addresses)
        self.load(self.attribute_table, attributes)
        self.load(self.category_table, categories)
