from pyspark.sql import functions, types
from baseclass import BaseClass

class Review(BaseClass):
    def __init__(self, sparkSession):
        super().__init__(sparkSession)
    
    def process(self, bucket_name, dataset_filename):
        print(f"Started processing gs://{bucket_name}/{dataset_filename}")
        
        df_review = self.spark.read.json(f'gs://{bucket_name}/{dataset_filename}')
        df_review.createOrReplaceTempView("VW_Review")
        #df_review.printSchema()


        df_review_valid = self.spark.sql('''
            SELECT 
                RVW.BUSINESS_ID, USER_ID,
                REGEXP_REPLACE(TEXT, '\n', ' ') AS TEXT, 
                TO_DATE(DATE) AS DATE,
                STARS, COOL, FUNNY, USEFUL
            FROM VW_REVIEW RVW
                INNER JOIN VW_BUSINESS_CA_FINAL BSN ON BSN.BUSINESS_ID = RVW.BUSINESS_ID
            WHERE
                    TEXT IS NOT NULL
                AND USEFUL IS NOT NULL
                AND COOL IS NOT NULL
                AND FUNNY IS NOT NULL
                AND STARS IS NOT NULL AND STARS >= 0 AND STARS <= 5 ''')
        df_review_valid.createOrReplaceTempView('VW_REVIEW_VALID')
        #df_review_valid.show(5)


        return df_review_valid