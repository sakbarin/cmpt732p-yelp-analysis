from abc import abstractmethod

class BaseClass:
    def __init__(self, sparkSession):
        self.spark = sparkSession

    @abstractmethod
    def process(self, bucket_name, dataset_filename):
        """
        This function reads the dataset JSON file from Google Cloud Storage (GCS), process it and return it as a data frame
            :param bucket_name: the name of bucket in GCS
            :param dataset_filename: the name of JSON file to be processed in GCS
            :return: processed dataframe
        """
        pass

    def write_to_bq(self, df, temp_bucket_name, ds_name, tbl_name):
        """
        This function writes a dataframe into BigQuery
            :param df: dataframe to be written into BigQuery
            :param temp_bucket_name: temporary bucket name to be used for staging
            :param ds_name: target dataset name in BigQuery
            :param tbl_name: target table name in BigQuery
            :return: None
        """
        df.write.format('bigquery') \
                .option('table', f'{ds_name}.{tbl_name}') \
                .option("temporaryGcsBucket", temp_bucket_name) \
                .mode('overwrite') \
                .save()