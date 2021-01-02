from pyspark.sql import functions, types
from baseclass import BaseClass

class Checkin(BaseClass):
    def __init__(self, sparkSession):
        super().__init__(sparkSession)
    
    def process(self, bucket_name, dataset_filename):
        print(f"Started processing gs://{bucket_name}/{dataset_filename}")
        
        df_checkin = self.spark.read.json(f'gs://{bucket_name}/{dataset_filename}')
        df_checkin.createOrReplaceTempView("VW_Checkin")
        #df_checkin.printSchema()
        #df_checkin.show(5)

        df_checkin_valid = self.spark.sql('''
            SELECT CHK.*
            FROM VW_CHECKIN CHK
                INNER JOIN VW_BUSINESS_CA_FINAL BSN ON BSN.BUSINESS_ID = CHK.BUSINESS_ID ''')
        df_checkin_valid.createOrReplaceTempView('VW_CHECKIN_VALID')


        df_checkin_exp = self.spark.sql('''
            SELECT  BUSINESS_ID,
                    FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(DATE), "yyyy-MM-dd HH:mm:ss")) AS DATE
            FROM (
                SELECT BUSINESS_ID, EXPLODE(SPLIT(DATE, ",")) AS DATE
                FROM   VW_CHECKIN_VALID )
            WHERE  
                    DATE IS NOT NULL
                AND DATE != "" ''')
        #df_checkin_exp.show(10)

        return df_checkin_exp