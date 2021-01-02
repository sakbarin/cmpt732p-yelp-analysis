from pyspark.sql import functions, types
from baseclass import BaseClass

class User(BaseClass):
    def __init__(self, sparkSession):
        super().__init__(sparkSession)
    
    def process(self, bucket_name, dataset_filename):
        print(f"Started processing gs://{bucket_name}/{dataset_filename}")
        
        df_user = self.spark.read.json(f'gs://{bucket_name}/{dataset_filename}')
        df_user.createOrReplaceTempView("VW_User")
        #df_user.printSchema()

        df_user_valid = self.spark.sql('''
            SELECT
                USR.USER_ID, NAME,
                COMPLIMENT_WRITER, COMPLIMENT_PROFILE, COMPLIMENT_PLAIN,
                COMPLIMENT_PHOTOS, COMPLIMENT_NOTE, COMPLIMENT_MORE,
                COMPLIMENT_LIST, COMPLIMENT_HOT, COMPLIMENT_FUNNY,
                COMPLIMENT_CUTE, COMPLIMENT_COOL,
                USR.USEFUL, USR.FUNNY,
                FRIENDS, FANS, ELITE,
                USR.COOL, REVIEW_COUNT, AVERAGE_STARS, YELPING_SINCE
            FROM VW_USER USR
                INNER JOIN VW_REVIEW_VALID RVW ON RVW.USER_ID = USR.USER_ID ''').cache()
        df_user_valid.createOrReplaceTempView('VW_USER_VALID')
        #df_user_valid.show(5)


        df_user_friends = self.spark.sql('''
            SELECT DISTINCT USER_ID, TRIM(FRIEND_USER_ID) AS FRIEND_USER_ID
            FROM (
                SELECT  USER_ID, EXPLODE(SPLIT(FRIENDS, ",")) AS FRIEND_USER_ID
                FROM    VW_USER_VALID
            )
            WHERE   FRIEND_USER_ID IS NOT NULL
                AND FRIEND_USER_ID <> "None" ''')
        #df_user_friends.show(10)


        df_user_elites = self.spark.sql('''
            SELECT DISTINCT USER_ID, TRIM(ELITE) AS ELITE
            FROM (
                SELECT  USER_ID,
                        EXPLODE(SPLIT(ELITE, ",")) AS ELITE
                FROM    VW_USER_VALID
            )
            WHERE   ELITE IS NOT NULL
                AND ELITE <> "None"
                AND ELITE <> "" ''')
        #df_user_elites.show(10)

        df_user_valid_final = df_user_valid.drop('ELITE', 'FRIENDS')
        #df_user_valid_final.show(10)

        return df_user, df_user_friends, df_user_elites