import google.auth
from services.etl import ETL
from configs.job_params import project_id, bucket_name, folder_name, table_name, table_schema
import os
import time
import argparse
import logging

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'application_default_credentials.json'
credentials, project = google.auth.default()

parser = argparse.ArgumentParser(description='Description of your program')
parser.add_argument('-ie', '--if_exists', help='-', required=False)
args = vars(parser.parse_args())

logger = logging.getLogger('services')
def main():
    etl = ETL(project_id, bucket_name, credentials)
    if args['if_exists'] == 'replace':
        etl.delete_table(table_name)
        etl.create_bigquery_table(table_name, table_schema)
        time.sleep(5)
        etl.load_gcs_data_to_bigquery(folder_name, table_name)
        etl.delete_duplicates(table_name)

    if args['if_exists'] == 'append':
        etl.load_gcs_data_to_bigquery(folder_name, table_name)


if __name__ == '__main__':
    main()
