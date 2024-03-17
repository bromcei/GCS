from repositories.bigquery import BigqueryRepo
from repositories.gcs import GCSRepo
from services.json_parser_functions import parse_json_record
import json
import logging
from datetime import datetime

current_time = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')


class ETL():
    def __init__(self, project_id, bucket_name, credentials):
        self.bigquery_repo = BigqueryRepo(project_id, credentials)
        self.gcs_repo = GCSRepo(project_id, bucket_name, credentials)
        self.retries_count = 5

    def create_bigquery_table(self, table_name, schema):
        try:
            self.bigquery_repo.create_table(table_name, schema)
            logging.info(f'Table {table_name} was successfully created')

        except Exception as e:
            logging.error(f"Failed to create table {table_name}, error: {e}")

    def delete_table(self, table_name):
        try:
            self.bigquery_repo.delete_table(table_name)
            logging.info(f'Table {table_name} was successfully deleted')
        except Exception as e:
            logging.error(f"Failed to delete table {table_name}, error: {e}")

    def load_json_to_table(self, table_name, entities_array, try_no=1):
        try:
            if try_no <= self.retries_count:
                self.bigquery_repo.load_json_to_table(table_name, entities_array)
                return try_no
            else:
                return None
        except Exception as e:
            try_no += 1
            if try_no == self.retries_count:
                return -1
            else:
                logging.warning(f"Failed to insert records, retry. Try no {try_no}")
                self.load_json_to_table(table_name, entities_array, try_no)

    def load_gcs_data_to_bigquery(self, prefix_name, table_name):
        for blob in self.gcs_repo.get_blobs(prefix_name):
            try:
                json_p = json.loads(blob.download_as_string())
                results_array = json_p.get('results')
                entities_array = []
                for result in results_array:
                    json_string = parse_json_record(result)
                    entities_array.append(json_string)
                load_result = self.load_json_to_table(table_name, entities_array)
                if load_result is not None:
                    logging.info(f'Blob {blob.name} was uploaded successfully')
                elif load_result == -1:
                    logging.error(f'Failed to insert {blob.name} data, retry')

            except Exception as e:
                logging.error(f'Failed to upload {blob.name} data to {table_name}')

    def delete_duplicates(self, table_name):
        try:
            self.bigquery_repo.delete_duplicated_id_rows(table_name)
        except Exception as e:
            logging.error(f'Failed to delete duplicates from {table_name}. Error: {e}')

    def print_blobs(self, prefix_name):
        blobs = self.gcs_repo.get_blobs(prefix_name)
        for blob in blobs:
            print(blob.name)
