class Province:
    def __init__(self, sparkSession):
        self.spark = sparkSession
    
    def read_dataset_file(self, bucket_path, dataset_filename):
        df_provinces = self.spark.read.json(f'{bucket_path}/{dataset_filename}')
        df_provinces.createOrReplaceTempView('VW_Province')

        return df_provinces

    