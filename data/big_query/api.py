from typing import Sequence, Dict, Any
from google.cloud import bigquery
import json
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data.big_query.api")

# Load data in batches
# Check for nulls/clean data
# Load into BigQuery using CSV loader

class BigQueryClient:
    def __init__(self) -> None:
        self.client = bigquery.Client(project="curious-sandbox-470103-c4")
        self.table = self.client.get_table("curious-sandbox-470103-c4.Yelp_Insights_DB.business")
        self.errors = []

    def load_businesses(self, rows_to_insert) -> None:
        errors = self.client.insert_rows(self.table, rows_to_insert)
        if (errors == []):
            logger.info(f"BUSINESS_LOAD_SUCCESSFUL records_loaded={len(rows_to_insert)}")
        else:
            logger.warning(f"BUSINESS_LOAD_FAILURE errors_encountered={len(errors)}")
            self.errors.append(errors)


