import os 
from google.cloud import storage

class GoogleCloud:
    def __init__(self, gc_credential_file):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gc_credential_file
        self.gcs_client = storage.Client()

    def ls_objects(self, bucket_name, obj_prefix):
        bucket = self.gcs_client.bucket(bucket_name)

        for obj in list(bucket.list_blobs(prefix=obj_prefix)):
            print(obj.name)

    def write_to_bq(self, dataframe, temp_bucket_name, dataset_name, table_name):
        dataframe.write \
            .format('bigquery') \
            .option('table', f'{dataset_name}.{table_name}') \
            .option("temporaryGcsBucket", temp_bucket_name) \
            .mode('overwrite') \
            .save()