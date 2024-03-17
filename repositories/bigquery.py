from google.cloud import bigquery


class BigqueryRepo():
    def __init__(self, project_id, credentials):
        self.client = bigquery.Client(project=project_id, credentials=credentials)

    def exec_query(self, query):
        """
        Exec custom query
        :param query:
        :return:
        """
        query_job = self.client.query(query)
        for row in query_job:
            print(row[0])

    def create_table(self, table_name, schema):
        """
        Creates table in BigQuery
        :param table_name:
        :param schema:
        :return:
        """
        table = bigquery.Table(table_name, schema=schema)
        self.client.create_table(table)

    def delete_table(self, table_name):
        """
        Deletes table in BigQuery
        :param table_name:
        :return:
        """
        self.client.delete_table(table_name, not_found_ok=True)

    def get_table(self, table_name):
        """
        Return BigQuery table
        :param table_name:
        :return: Bigquery Table object
        """
        return self.client.get_table(table_name)

    def get_table_schema(self, table_name):
        """
        Returns required table schema
        :param table_name:
        :return: BigQuery Schema
        """
        return self.client.get_table(table_name).schema

    def load_json_to_table(self, table_name, json_object):
        """
        Method loads json data, if insert fails it will rerun insert
        :param table_name: table name in BigQuery
        :param json_object: json object to load to table
        :return: 1 if insert was successful
        """
        table = self.get_table(table_name)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = self.get_table_schema(table_name)
        self.client.insert_rows_json(table, json_object)

    def delete_duplicated_id_rows(self, table_name):
        """
        Function deletes duplicated id records in table
        :param table_name:
        :return:
        """
        query = f"""
            DELETE FROM {table_name}` 
            WHERE id IN (
              SELECT 
              id
            FROM `{table_name}`
            GROUP BY id
            HAVING COUNT(*) > 1
            )
        """
        self.exec_query(query)
