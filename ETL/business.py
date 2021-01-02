from pyspark.sql import functions, types
from baseclass import BaseClass
import json
import re

class Business(BaseClass):
    def __init__(self, sparkSession):
        super().__init__(sparkSession)
    
    def process(self, bucket_name, dataset_filename):
        print(f"Started processing gs://{bucket_name}/{dataset_filename}")
        
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
        

        df_business = self.spark.read.json(f'gs://{bucket_name}/{dataset_filename}')
        df_business.createOrReplaceTempView("VW_Business")
        #df_business.show(10)

        #self.spark.sql('''SELECT STATE, COUNT(*) AS STATE_COUNT FROM VW_Business GROUP BY STATE ORDER BY 2 DESC''').show(10)

        df_business_CA = self.spark.sql('''
            SELECT  BUSINESS_ID, NAME, STATE, CITY, POSTAL_CODE, LATITUDE, LONGITUDE, CATEGORIES, ATTRIBUTES
            FROM    VW_Business VB INNER JOIN VW_Province VP ON VB.STATE = VP.CODE
            WHERE   IS_OPEN = '1'
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

        #self.spark.sql('''SELECT STATE, COUNT(*) AS STATE_COUNT FROM VW_BUSINESS_CA GROUP BY STATE ORDER BY 2 DESC''').show(10)

        #self.spark.sql('''SELECT STATE, CITY, COUNT(*) AS CITY_COUNT FROM VW_BUSINESS_CA GROUP BY STATE, CITY ORDER BY 3 DESC''').show(10)

        # prepare row wise category table
        df_category_rw = self.spark.sql('''
            SELECT  BUSINESS_ID AS B_ID, UPPER(TRIM(CATEGORY)) as CATEGORY, True AS SERVED
            FROM (  SELECT  BUSINESS_ID, EXPLODE(SPLIT(CATEGORIES, ",")) as CATEGORY
                    FROM    VW_BUSINESS_CA ) ''')
        df_category_rw.createOrReplaceTempView('VW_CATEGORY_RW')
        #df_category_rw.show(10)

        #self.spark.sql('SELECT CATEGORY, COUNT(B_ID) AS BUSINESS_COUNT FROM VW_CATEGORY_RW GROUP BY CATEGORY ORDER BY 2 DESC').show(10)

        # prepare column wise category table
        df_category_cw = df_category_rw.groupby('B_ID').pivot('CATEGORY').agg(functions.max('SERVED'))
        df_category_cw = df_category_cw.select([functions.col(col).alias("CAT_" + re.sub("[^0-9a-zA-Z$]+","",col)) for col in df_category_cw.columns])
        df_category_cw.createOrReplaceTempView('VW_CATEGORY_CW')
        #df_category_cw.show(10)

        #self.spark.sql('''SELECT RESTAURANTS, COUNT(B_ID) BUSINESS_COUNT FROM VW_CATEGORY_CW GROUP BY RESTAURANTS''').show(10)

        # Stg1 => First Stage: Including business categories as extra columns to business dataframe
        df_business_CA_Stg1 = self.spark.sql('''
            SELECT BUSINESS_ID, NAME, STATE, CITY, POSTAL_CODE, LATITUDE, LONGITUDE, CATEGORIES, ATTRIBUTES, CAT.*
            FROM VW_BUSINESS_CA BSN 
                INNER JOIN VW_CATEGORY_CW CAT ON BSN.BUSINESS_ID = CAT.CAT_BID ''')

        df_business_CA_Stg1 = df_business_CA_Stg1.drop("CATEGORIES", "CAT_BID")
        df_business_CA_Stg1.createOrReplaceTempView('VW_BUSINESS_CA_Stg1')
        #df_business_CA_Stg1.show(10)


        attributes = get_attribute_keys(df_business_CA)

        self.spark.udf.register("FLATTEN", flatten_attributes)

        # row wise attribute dataframe
        df_attribute_rw = self.spark.sql('''
            SELECT  BUSINESS_ID AS B_ID,
                    REPLACE(SPLIT(TRIM(ATTRIBUTE), ':')[0], "'", "") AS ATTR_KEY,
                    UPPER(TRIM(SPLIT(ATTRIBUTE, ':')[1])) AS ATTR_VAL
            FROM (
                SELECT  BUSINESS_ID,
                        EXPLODE(SPLIT(FLATTEN(ATTRIBUTES), ',')) AS ATTRIBUTE
                FROM    VW_BUSINESS_CA )''')
        df_attribute_rw.createOrReplaceTempView('VW_ATTRIBUTE_RW')

        #spark.sql("SELECT * FROM VW_ATTRIBUTE_RW").show(5, False)

        # column wise attribute dataframe
        df_attribute_cw = df_attribute_rw.groupby('B_ID').pivot('ATTR_KEY').agg(functions.max('ATTR_VAL'))
        df_attribute_cw = df_attribute_cw.select([functions.col(col).alias("ATTR_" + re.sub("[^0-9a-zA-Z$]+","",col)) for col in df_attribute_cw.columns])
        df_attribute_cw.createOrReplaceTempView('VW_ATTRIBUTE_CW')
        #df_attribute_cw.show(10)

        # Stg2 => Second Stage: Including attribute values as extra columns into business dataframe
        df_business_CA_Stg2 = self.spark.sql('''
            SELECT  BSN.*, ATR.* 
            FROM    VW_BUSINESS_CA_Stg1 BSN 
                        INNER JOIN VW_ATTRIBUTE_CW ATR ON BSN.BUSINESS_ID = ATR.ATTR_BID ''')

        df_business_CA_final = df_business_CA_Stg2.drop('ATTRIBUTES', 'ATTR_BID')

        df_business_CA_final.createOrReplaceTempView('VW_BUSINESS_CA_FINAL')
        df_business_CA_final.cache()
        #df_business_CA_final.show(5)

        return df_business_CA_final, df_category_rw, df_attribute_rw