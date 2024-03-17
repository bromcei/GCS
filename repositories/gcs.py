# This is a sample Python script.
from google.cloud import storage


class GCSRepo():
    def __init__(self, project_id, bucket_name, credentials):
        self.client = storage.Client(project=project_id, credentials=credentials)
        self.bucket = self.client.bucket(bucket_name)

    def blobs_count(self, prefix_name):
        blobs = self.bucket.list_blobs(prefix=prefix_name)
        return sum([1 for blob in blobs])

    def get_newest_blob_date(self, prefix_name):
        blobs = self.bucket.list_blobs(prefix=prefix_name)
        return max([blob.time_created.date() for blob in blobs])

    def get_blobs(self, prefix_name):
        return self.bucket.list_blobs(prefix=prefix_name)

    def newest_blob_date(self, prefix_name):
        max_blob_date = max([blob.time_created.date() for blob in self.get_blobs(prefix_name)])
        print(max_blob_date)
