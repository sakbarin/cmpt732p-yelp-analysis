import json
import ast
from datetime import datetime
from pyspark.sql import functions, types

class Business:
    def __init__(self, sparkSession):
        self.spark = sparkSession
        self.attributes = []
        self.spark.udf.register("FLATTEN", self.__flatten_attributes)

    def __set_attribute_keys(self, df_business):
        fields = json.loads(df_business.schema.json())['fields']

        for field in fields:
            if (field['name'] == 'ATTRIBUTES'):

                sub_fields = field['type']['fields']

                for sub_field in sub_fields:
                    self.attributes += [sub_field['name']]

    @functions.udf(returnType=types.StringType())
    def __flatten_attributes(col_data, xxx):
        output = {}

        for attribute in ast.literal_eval(xxx):
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

    def __process_categories(self):
        print("Business.__process_categories() started at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        df_business_CA_cat = self.spark.sql('''
            SELECT 
                BUSINESS_ID,
                TRIM(CATEGORY) as CATEGORY
            FROM
                (
                SELECT 
                    BUSINESS_ID, 
                    EXPLODE(SPLIT(CATEGORIES, ',')) as CATEGORY
                FROM
                    VW_BUSINESS_CA) ''')
        
        print("Business.__process_categories() finished at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        return df_business_CA_cat

    def __process_attributes(self):
        print("Business.__process_attributes() started at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        df_business_CA_attr = self.spark.sql('''
            SELECT
                BUSINESS_ID,
                REPLACE(SPLIT(TRIM(ATTRIBUTE), ':')[0], "'", "") AS ATTR_KEY,
                TRIM(SPLIT(ATTRIBUTE, ':')[1]) AS ATTR_VAL
            FROM
            (
                SELECT
                    BUSINESS_ID,
                    EXPLODE(SPLIT(FLATTEN(ATTRIBUTES,"''' + str(self.attributes) + '''"), ',')) AS ATTRIBUTE
                FROM 
                    VW_BUSINESS_CA )''')

        print("Business.__process_attributes() finished at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        return df_business_CA_attr

    def process_file(self, bucket_path, dataset_filename):
        print("Business.process_file() started at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        df_business = self.spark.read.json(f'{bucket_path}/{dataset_filename}')
        df_business.createOrReplaceTempView("VW_Business")
        
        df_business_CA = self.spark.sql('''
            SELECT
                BUSINESS_ID,
                NAME,
                STATE,
                CITY,
                POSTAL_CODE,
                LATITUDE,
                LONGITUDE,
                CATEGORIES,
                ATTRIBUTES
            FROM
                VW_Business VB 
                    INNER JOIN VW_Province VP ON VB.STATE = VP.CODE
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
        
        self.__set_attribute_keys(df_business_CA)

        df_business_CA_attr = self.__process_attributes()

        df_business_CA_cat = self.__process_categories()

        df_business_CA_final = df_business_CA.drop('CATEGORIES', 'ATTRIBUTES')
        df_business_CA_final.createOrReplaceTempView('VW_BUSINESS_CA_FINAL')

        print("Business.process_file() finished at " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

        return df_business_CA_final, df_business_CA_cat, df_business_CA_attr