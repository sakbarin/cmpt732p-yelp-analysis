from baseclass import BaseClass

class Province(BaseClass):
    def __init__(self, sparkSession):
        super().__init__(sparkSession)
    
    def process(self, bucket_name, dataset_filename):
        print(f"Started processing gs://{bucket_name}/{dataset_filename}")
        
        df_provinces = self.spark.read.json(f'gs://{bucket_name}/{dataset_filename}')
        df_provinces.createOrReplaceTempView('VW_Province')
        #df_provinces.show(10)

        return df_provinces