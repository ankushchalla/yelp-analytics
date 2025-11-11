from typing import Iterable, Sequence
from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.api_core.exceptions import GoogleAPICallError
import json
import logging

from data.types import Address, Attribute, BusinessRecord, ReviewRecord, Category, Review, Date
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
        self.review_table = self.client.get_table("curious-sandbox-470103-c4.Yelp_Insights_DB.review")
        self.date_table = self.client.get_table("curious-sandbox-470103-c4.Yelp_Insights_DB.date")
        self.errors = []

    def load(self, table: Table, rows_to_insert: Sequence[Address | Attribute | Category| Review | Date]) -> None:
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

    def load_review_records(self, review_records: Iterable[ReviewRecord]) -> None:
        reviews = [record.review for record in review_records]
        dates = [record.date for record in review_records]

        # self.load(self.review_table, reviews)
        self.load(self.date_table, dates)

    def create_poi_table(self) -> None:
        sql = """
            CREATE TABLE IF NOT EXISTS `curious-sandbox-470103-c4.Yelp_Insights_DB.poi` AS
            SELECT
            osm_id,
            (SELECT value FROM UNNEST(all_tags) WHERE key = 'name') AS name,
            COALESCE(
                (SELECT key FROM UNNEST(all_tags) WHERE key = 'amenity'),
                (SELECT key FROM UNNEST(all_tags) WHERE key = 'shop'),
                (SELECT key FROM UNNEST(all_tags) WHERE key = 'tourism'),
                (SELECT key FROM UNNEST(all_tags) WHERE key = 'leisure')
            ) AS category_type,
            COALESCE(
                (SELECT value FROM UNNEST(all_tags) WHERE key = 'amenity'),
                (SELECT value FROM UNNEST(all_tags) WHERE key = 'shop'),
                (SELECT value FROM UNNEST(all_tags) WHERE key = 'tourism'),
                (SELECT value FROM UNNEST(all_tags) WHERE key = 'leisure')
            ) AS category,
            geometry AS geom
            FROM `bigquery-public-data.geo_openstreetmap.planet_features_points`
            WHERE
            EXISTS (SELECT 1 FROM UNNEST(all_tags) WHERE key IN ('amenity','shop','tourism', 'leisure'))
            AND geometry IS NOT NULL
            AND ST_X(geometry) BETWEEN -170 AND -50
            AND ST_Y(geometry) BETWEEN 15 AND 75;
        """
        query_job = self.client.query(sql)
        try:
            query_job.result()
        except GoogleAPICallError as api_error:
            if api_error.errors != []:
                logger.warning(f"POI_TABLE_CREATION_FAILURE {api_error.errors}")
                self.errors.append(api_error.errors)
            elif api_error.message is not None:
                logger.warning(f"POI_TABLE_CREATION_FAILURE {api_error.message}")
                self.errors.append(api_error.message)

        
