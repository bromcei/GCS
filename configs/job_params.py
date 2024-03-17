from google.cloud import bigquery

project_id = 'project_id'

# GCS params
bucket_name = 'bucket_name'
folder_name = 'folder_name'

# BigQuery params
table_name = 'tbl_name'
table_schema = [
            bigquery.SchemaField(name='id', field_type='STRING', mode='REQUIRED', ),
            bigquery.SchemaField('project_name', 'STRING', mode='Nullable'),
            bigquery.SchemaField('schema_name', 'STRING', mode='Nullable'),
            bigquery.SchemaField('table_name', 'STRING', mode='Nullable'),
            bigquery.SchemaField('latest_loaded_at', 'TIMESTAMP', mode='Nullable'),
            bigquery.SchemaField('queried_at', 'TIMESTAMP', mode='Nullable'),
            bigquery.SchemaField('time_since_last_row_arrived_in_s', 'NUMERIC', mode='Nullable'),
            bigquery.SchemaField('status', 'STRING', mode='Nullable'),
            bigquery.SchemaField('filter_field', 'STRING', mode='Nullable'),
            bigquery.SchemaField('filter_type', 'STRING', mode='Nullable'),
            bigquery.SchemaField('filter_value', 'STRING', mode='Nullable'),
            bigquery.SchemaField('warn_after_period', 'STRING', mode='Nullable'),
            bigquery.SchemaField('warn_after_value', 'INTEGER', mode='Nullable'),
            bigquery.SchemaField('error_after_period', 'STRING', mode='Nullable'),
            bigquery.SchemaField('error_after_value', 'INTEGER', mode='Nullable'),
            bigquery.SchemaField('bytes_processed', 'INTEGER', mode='Nullable'),
            bigquery.SchemaField('bytes_billed', 'INTEGER', mode='Nullable'),
            bigquery.SchemaField('job_location', 'STRING', mode='Nullable'),
            bigquery.SchemaField('job_project_id', 'STRING', mode='Nullable'),
            bigquery.SchemaField('slot_ms', 'INTEGER', mode='Nullable'),
            bigquery.SchemaField('price', 'BIGNUMERIC', mode='Nullable'),
            bigquery.SchemaField('started_at', 'TIMESTAMP', mode='Nullable'),
            bigquery.SchemaField('completed_at', 'TIMESTAMP', mode='Nullable')
        ]
