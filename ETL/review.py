from datetime import datetime
from pyspark.sql import functions, types

class Review:
    def __init__(self, sparkSession):
        self.spark = sparkSession

    def process_file(self, bucket_path, dataset_filename):
        print("Review.process_file() started at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        df_review = self.spark.read.json(f'{bucket_path}/{dataset_filename}')
        df_review.createOrReplaceTempView("VW_Review")

        df_review_valid = self.spark.sql('''
            SELECT 
                VW_REVIEW.BUSINESS_ID,
                USER_ID,
                REGEXP_REPLACE(TEXT, '\n', ' ') AS TEXT, 
                TO_DATE(DATE) AS DATE,
                STARS,
                COOL,
                FUNNY,
                USEFUL
            FROM VW_REVIEW 
                INNER JOIN VW_BUSINESS_CA_FINAL ON VW_BUSINESS_CA_FINAL.BUSINESS_ID = VW_REVIEW.BUSINESS_ID
            WHERE
                    TEXT IS NOT NULL
                AND USEFUL IS NOT NULL
                AND COOL IS NOT NULL
                AND FUNNY IS NOT NULL
                AND STARS IS NOT NULL AND STARS >= 0 AND STARS <= 5 ''')
        df_review_valid.createOrReplaceTempView('VW_REVIEW_VALID')

        print("Review.process_file() finished at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        return df_review_valid