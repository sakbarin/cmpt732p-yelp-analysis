import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import ast 
import json
import re

from pyspark.sql import SparkSession, functions, types
from google.cloud import storage


# define global variables
spark = SparkSession.builder.appName('YELP_ETL') \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta') \
    .getOrCreate()


def start_ETL(bucket_name, bucket_path, dataset_name):

    def get_attribute_keys(df):
        """
        This function retrieves keys that are available in Attributes column
        :param df: input dataframe
        :return: a list of keys
        """
        fields = json.loads(df.schema.json())['fields']

        attributes = []

        for field in fields:
            if (field['name'] == 'ATTRIBUTES'):

                sub_fields = field['type']['fields']

                for sub_field in sub_fields:
                    attributes += [sub_field['name']]
        
        return attributes

    @functions.udf(returnType=types.StringType())
    def flatten_attributes(col_data):
        """
        This function flattens "attributes" column JSON value into a list
        :param col_data: input column to be processed
        :return: a list of flattened attributes
        """
        output = {}

        for attribute in attributes:
            if (col_data[attribute] == None):
                output[attribute] = None
            elif str(col_data[attribute]).startswith('{'):
                col_sub_data = str(col_data[attribute]).split(',')
                
                for sub_data in col_sub_data:
                    if (len(sub_data.split(':')) == 2):
                        sub_attr_key = sub_data.split(':')[0].replace('{', '').replace('\'', '').strip()
                        sub_attr_val = sub_data.split(':')[1].replace('}', '').replace('\'', '').strip()
                        output[attribute + "_" + sub_attr_key] = sub_attr_val
            else:
                output[attribute] = col_data[attribute]

        return str(output).replace('{', '').replace('}','').replace('\'', '')

    def write_to_bq(df, temp_bucket_name, ds_name, tbl_name):
        """
        This function writes a dataframe into BigQuery
        :param df: dataframe to be written into BigQuery
        :param temp_bucket_name: temporary bucket name to be used for staging
        :param ds_name: target dataset name in BigQuery
        :param tbl_name: target table name in BigQuery
        :return: None
        """
        df.write.format('bigquery')\
                .option('table', f'{ds_name}.{tbl_name}')\
                .option("temporaryGcsBucket", temp_bucket_name)\
                .mode('overwrite').save()
    

    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)

    for obj in list(bucket.list_blobs(prefix='yelp_academic')):
        print(obj)


    dataset_files = {
        'provinces': 'yelp_academic_ca_provinces.json',
        'businesses': 'yelp_academic_dataset_business.json',
        'checkins': 'yelp_academic_dataset_checkin.json',
        'reviews': 'yelp_academic_dataset_review.json',
        'users': 'yelp_academic_dataset_user.json'
    }


    df_provinces = spark.read.json(f'{bucket_path}/{dataset_files["provinces"]}')
    df_provinces.createOrReplaceTempView('VW_Province')

    write_to_bq(df_provinces, bucket_name, dataset_name, 'provinces')

    #df_provinces.show(10)

    df_business = spark.read.json(f'{bucket_path}/{dataset_files["businesses"]}')
    df_business.createOrReplaceTempView("VW_Business")
    
    #df_business.show(10)


    #spark.sql('''SELECT STATE, COUNT(*) AS STATE_COUNT FROM VW_Business GROUP BY STATE ORDER BY 2 DESC''').show(10)


    df_business_CA = spark.sql('''SELECT
                                    BUSINESS_ID, NAME, STATE, CITY, POSTAL_CODE, LATITUDE, LONGITUDE, CATEGORIES, ATTRIBUTES
                                FROM
                                    VW_Business VB INNER JOIN VW_Province VP ON VB.STATE = VP.CODE
                                WHERE
                                        IS_OPEN = '1'
                                    AND POSTAL_CODE IS NOT NULL
                                    AND LENGTH(POSTAL_CODE) = 7
                                    AND LATITUDE IS NOT NULL
                                    AND LONGITUDE IS NOT NULL
                                    AND STARS IS NOT NULL
                                    AND REVIEW_COUNT IS NOT NULL
                                    AND CATEGORIES IS NOT NULL
                                    AND ATTRIBUTES IS NOT NULL ''').cache()

    df_business_CA.createOrReplaceTempView('VW_BUSINESS_CA')
    
    #df_business_CA.show(10)

    #spark.sql('''SELECT STATE, COUNT(*) AS STATE_COUNT FROM VW_BUSINESS_CA GROUP BY STATE ORDER BY 2 DESC''').show(10)

    #spark.sql('''SELECT STATE, CITY, COUNT(*) AS CITY_COUNT FROM VW_BUSINESS_CA GROUP BY STATE, CITY ORDER BY 3 DESC''').show(10)


    # prepare row wise category table
    df_category_rw = spark.sql('''SELECT  BUSINESS_ID AS B_ID, UPPER(TRIM(CATEGORY)) as CATEGORY, True AS SERVED
                                  FROM (
                                        SELECT  BUSINESS_ID, EXPLODE(SPLIT(CATEGORIES, ",")) as CATEGORY
                                        FROM    VW_BUSINESS_CA ) ''')

    df_category_rw.createOrReplaceTempView('VW_CATEGORY_RW')
    
    #df_category_rw.show(10)


    #spark.sql('SELECT CATEGORY, COUNT(B_ID) AS BUSINESS_COUNT FROM VW_CATEGORY_RW GROUP BY CATEGORY ORDER BY 2 DESC').show(10)


    # prepare column wise category table
    df_category_cw = df_category_rw.groupby('B_ID').pivot('CATEGORY').agg(functions.max('SERVED'))
    df_category_cw = df_category_cw.select([functions.col(col).alias("CAT_" + re.sub("[^0-9a-zA-Z$]+","",col)) for col in df_category_cw.columns])
    df_category_cw.createOrReplaceTempView('VW_CATEGORY_CW')
    
    #df_category_cw.show(10)


    #spark.sql('''SELECT RESTAURANTS, COUNT(B_ID) BUSINESS_COUNT FROM VW_CATEGORY_CW GROUP BY RESTAURANTS''').show(10)


    # Stg1 => First Stage: Including business categories as extra columns to business dataframe
    df_business_CA_Stg1 = spark.sql('''SELECT 
                                            BUSINESS_ID, NAME, STATE, CITY, POSTAL_CODE, LATITUDE, LONGITUDE, CATEGORIES, ATTRIBUTES, CAT.*
                                        FROM 
                                            VW_BUSINESS_CA BSN 
                                                INNER JOIN VW_CATEGORY_CW CAT ON BSN.BUSINESS_ID = CAT.CAT_BID ''')


    df_business_CA_Stg1 = df_business_CA_Stg1.drop("CATEGORIES", "CAT_BID")
    df_business_CA_Stg1.createOrReplaceTempView('VW_BUSINESS_CA_Stg1')


    #df_business_CA_Stg1.show(10)


    attributes = get_attribute_keys(df_business_CA)


    spark.udf.register("FLATTEN", flatten_attributes);


    # row wise attribute dataframe
    df_attribute_rw = spark.sql('''
                                SELECT BUSINESS_ID AS B_ID,
                                    REPLACE(SPLIT(TRIM(ATTRIBUTE), ':')[0], "'", "") AS ATTR_KEY,
                                    UPPER(TRIM(SPLIT(ATTRIBUTE, ':')[1])) AS ATTR_VAL
                                FROM
                                (
                                    SELECT BUSINESS_ID,
                                        EXPLODE(SPLIT(FLATTEN(ATTRIBUTES), ',')) AS ATTRIBUTE
                                    FROM   VW_BUSINESS_CA )''')

    df_attribute_rw.createOrReplaceTempView('VW_ATTRIBUTE_RW')


    #spark.sql("SELECT * FROM VW_ATTRIBUTE_RW").show(5, False)


    # column wise attribute dataframe
    df_attribute_cw = df_attribute_rw.groupby('B_ID').pivot('ATTR_KEY').agg(functions.max('ATTR_VAL'))
    df_attribute_cw = df_attribute_cw.select([functions.col(col).alias("ATTR_" + re.sub("[^0-9a-zA-Z$]+","",col)) for col in df_attribute_cw.columns])
    df_attribute_cw.createOrReplaceTempView('VW_ATTRIBUTE_CW')

    #df_attribute_cw.show(10)


    # Stg2 => Second Stage: Including attribute values as extra columns into business dataframe
    df_business_CA_Stg2 = spark.sql('''SELECT BSN.*, ATR.* 
                                    FROM 
                                        VW_BUSINESS_CA_Stg1 BSN 
                                            INNER JOIN VW_ATTRIBUTE_CW ATR ON BSN.BUSINESS_ID = ATR.ATTR_BID''')


    df_business_CA_final = df_business_CA_Stg2.drop('ATTRIBUTES', 'ATTR_BID')


    df_business_CA_final.createOrReplaceTempView('VW_BUSINESS_CA_FINAL')
    df_business_CA_final.cache();


    #df_business_CA_final.show(5)


    # ## Process yelp_academic_dataset_checkin.csv file


    df_checkin = spark.read.json(f'{bucket_path}/{dataset_files["checkins"]}')
    df_checkin.createOrReplaceTempView("VW_Checkin")


    #df_checkin.printSchema()


    #df_checkin.show(5)


    df_checkin_valid = spark.sql('''SELECT CHK.*
                                    FROM VW_CHECKIN CHK
                                        INNER JOIN VW_BUSINESS_CA_FINAL BSN
                                            ON BSN.BUSINESS_ID = CHK.BUSINESS_ID ''')

    df_checkin_valid.createOrReplaceTempView('VW_CHECKIN_VALID')


    df_checkin_exp = spark.sql('''
                                SELECT  BUSINESS_ID,
                                        FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(DATE), "yyyy-MM-dd HH:mm:ss")) AS DATE
                                FROM (
                                    SELECT BUSINESS_ID, EXPLODE(SPLIT(DATE, ",")) AS DATE
                                    FROM   VW_CHECKIN_VALID )
                                WHERE  
                                        DATE IS NOT NULL
                                    AND DATE != "" ''')


    #df_checkin_exp.show(10)


    # ## Process yelp_academic_dataset_review.csv file


    df_review = spark.read.json(f'{bucket_path}/{dataset_files["reviews"]}')
    df_review.createOrReplaceTempView("VW_Review")


    #df_review.printSchema()


    df_review_valid = spark.sql('''SELECT 
                                        RVW.BUSINESS_ID,
                                        USER_ID,
                                        REGEXP_REPLACE(TEXT, '\n', ' ') AS TEXT, 
                                        TO_DATE(DATE) AS DATE,
                                        STARS,
                                        COOL,
                                        FUNNY,
                                        USEFUL
                                    FROM VW_REVIEW RVW
                                        INNER JOIN VW_BUSINESS_CA_FINAL BSN ON BSN.BUSINESS_ID = RVW.BUSINESS_ID
                                    WHERE
                                            TEXT IS NOT NULL
                                        AND USEFUL IS NOT NULL
                                        AND COOL IS NOT NULL
                                        AND FUNNY IS NOT NULL
                                        AND STARS IS NOT NULL AND STARS >= 0 AND STARS <= 5
                                ''')

    df_review_valid.createOrReplaceTempView('VW_REVIEW_VALID')


    #df_review_valid.show(5)


    # ## Process yelp_academic_dataset_user.csv file


    df_user = spark.read.json(f'{bucket_path}/{dataset_files["users"]}')
    df_user.createOrReplaceTempView("VW_User")


    #df_user.printSchema()


    df_user_valid = spark.sql('''
                                SELECT
                                    USR.USER_ID,
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
                                    USR.USEFUL,
                                    USR.FUNNY,
                                    FRIENDS,
                                    FANS,
                                    ELITE,
                                    USR.COOL,
                                    REVIEW_COUNT,
                                    AVERAGE_STARS,
                                    YELPING_SINCE
                                FROM VW_USER USR
                                    INNER JOIN VW_REVIEW_VALID RVW ON RVW.USER_ID = USR.USER_ID
                            ''').cache()

    df_user_valid.createOrReplaceTempView('VW_USER_VALID')


    #df_user_valid.show(5)


    df_user_friends = spark.sql('''
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
                                    AND FRIEND_USER_ID <> 'None'
                    ''')


    #df_user_friends.show(10)


    df_user_elites = spark.sql('''
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
                                    AND ELITE <> ''
                    ''')


    #df_user_elites.show(10)


    df_user_valid_final = df_user_valid.drop('ELITE', 'FRIENDS')


    #df_user_valid_final.show(10)

    write_to_bq(df_business_CA_final, bucket_name, dataset_name, 'businesses')
    write_to_bq(df_category_rw, bucket_name, dataset_name, 'categories')
    write_to_bq(df_attribute_rw, bucket_name, dataset_name, 'attributes')
    write_to_bq(df_checkin_exp, bucket_name, dataset_name, 'checkins')
    write_to_bq(df_review_valid, bucket_name, dataset_name, 'reviews')
    write_to_bq(df_user_valid_final, bucket_name, dataset_name, 'users')
    write_to_bq(df_user_friends, bucket_name, dataset_name, 'user_friends')
    write_to_bq(df_user_elites, bucket_name, dataset_name, 'user_elites')


# main section of the code
if __name__ == '__main__':
    bucket_name = sys.argv[1] #'cmpt732-project-bucket'
    bucket_path = 'gs://' + bucket_name
    dataset_name = sys.argv[2] #"yelp_dataset"

    print(f"Bucket: {bucket_path}")
    print(f"Dataset: {dataset_name}")

    #gc_credential_file = sys.argv[1]
    #bucket_name = sys.argv[2]
    #dataset_name = sys.argv[3]    
    #bucket_path = 'gs://' + bucket_name

    #spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    #spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'ture')
    #spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', gc_credential_file)

    start_ETL(bucket_name, bucket_path, dataset_name)