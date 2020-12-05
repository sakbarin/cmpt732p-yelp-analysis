from datetime import datetime
from pyspark.sql import functions, types

class User:
    def __init__(self, sparkSession):
        self.spark = sparkSession

    def __process_user_friends(self):
        print("User.__process_user_friends() started at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        df_user_friends = self.spark.sql('''
            SELECT DISTINCT
                USER_ID,
                TRIM(FRIEND_USER_ID) AS FRIEND_USER_ID
            FROM (SELECT 
                    USER_ID,
                    EXPLODE(SPLIT(FRIENDS, ',')) AS FRIEND_USER_ID
                FROM 
                    VW_USER_VALID)
            WHERE
                    FRIEND_USER_ID IS NOT NULL
                AND FRIEND_USER_ID <> 'None'  ''')

        print("User.__process_user_friends() finished at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        return df_user_friends

    def __process_user_elite(self):
        print("User.__process_user_elite() started at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
        
        df_user_elite = self.spark.sql('''
            SELECT DISTINCT
                USER_ID,
                TRIM(ELITE) AS ELITE
            FROM (SELECT 
                    USER_ID,
                    EXPLODE(SPLIT(ELITE, ',')) AS ELITE
                FROM 
                    VW_USER_VALID)
            WHERE
                    ELITE IS NOT NULL
                AND ELITE <> 'None'
                AND ELITE <> ''   ''')

        print("User.__process_user_elite() finished at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        return df_user_elite

    def process_file(self, bucket_path, dataset_filename):
        print("User.process_file() started at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        df_user = self.spark.read.json(f'{bucket_path}/{dataset_filename}')
        df_user.createOrReplaceTempView("VW_User")

        df_user_valid = self.spark.sql('''
            SELECT
                VW_USER.USER_ID,
                NAME,
                COMPLIMENT_WRITER,
                COMPLIMENT_PROFILE,
                COMPLIMENT_PLAIN,
                COMPLIMENT_PHOTOS,
                COMPLIMENT_NOTE,
                COMPLIMENT_MORE,
                COMPLIMENT_LIST,
                COMPLIMENT_HOT,
                COMPLIMENT_FUNNY,
                COMPLIMENT_CUTE,
                COMPLIMENT_COOL,
                VW_USER.USEFUL,
                VW_USER.FUNNY,
                FRIENDS,
                FANS,
                ELITE,
                VW_USER.COOL,
                REVIEW_COUNT,
                AVERAGE_STARS,
                YELPING_SINCE
            FROM VW_USER 
                INNER JOIN VW_REVIEW_VALID ON VW_REVIEW_VALID.USER_ID = VW_USER.USER_ID ''').cache()
        df_user_valid.createOrReplaceTempView('VW_USER_VALID')

        df_user_friends = self.__process_user_friends()

        df_user_elite = self.__process_user_elite()

        df_user_valid_final = df_user_valid.drop('ELITE', 'FRIENDS')

        print("User.process_file() finished at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        return df_user_valid_final, df_user_friends, df_user_elite