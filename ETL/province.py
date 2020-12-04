from datetime import datetime

class Province:
    def __init__(self, sparkSession):
        self.spark = sparkSession
    
    def process_file(self, bucket_path, dataset_filename):
        print("Provice.process_file() started at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        df_provinces = self.spark.read.json(f'{bucket_path}/{dataset_filename}')
        df_provinces.createOrReplaceTempView('VW_Province')

        print("Provice.process_file() finished at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        return df_provinces

    