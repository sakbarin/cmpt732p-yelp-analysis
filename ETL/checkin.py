from datetime import datetime
from pyspark.sql import functions, types

class Checkin:
    def __init__(self, sparkSession):
        self.spark = sparkSession

    def process_file(self, bucket_path, dataset_filename):
        print("Checkin.process_file() started at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        df_checkin = self.spark.read.json(f'{bucket_path}/{dataset_filename}')
        df_checkin.createOrReplaceTempView("VW_Checkin")

        df_checkin_valid = self.spark.sql('''
            SELECT 
                VC.*
                FROM VW_CHECKIN VC
                    INNER JOIN VW_BUSINESS_CA_FINAL VB ON VB.BUSINESS_ID = VC.BUSINESS_ID ''')
        df_checkin_valid.createOrReplaceTempView('VW_CHECKIN_VALID')

        df_checkin_exp = self.spark.sql('''
            SELECT 
                BUSINESS_ID,
                FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(DATE), 'yyyy-MM-dd HH:mm:ss')) AS DATE
            FROM (
                SELECT
                    BUSINESS_ID,
                    EXPLODE(SPLIT(DATE, ',')) AS DATE
                FROM 
                    VW_CHECKIN_VALID)
            WHERE
                    DATE IS NOT NULL
                AND DATE != ''  ''')

        print("Checkin.process_file() finished at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        return df_checkin_exp