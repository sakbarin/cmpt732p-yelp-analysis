import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import ast 
import json

from pyspark.sql import SparkSession, functions, types
from google.cloud import storage

from province import Province
from business import Business
from checkin import Checkin
from review import Review
from user import User

# define global variables
spark = SparkSession.builder.appName('YELP_ETL') \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta') \
    .getOrCreate()


def start_ETL(bucket_name, bucket_path, dataset_name):
    # retrieve and print objects in the provided bucket
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)

    for obj in list(bucket.list_blobs(prefix='yelp_academic')):
        print(obj)


    # dataset file names
    dataset_files = {
        'provinces': 'yelp_academic_ca_provinces.json',
        'businesses': 'yelp_academic_dataset_business.json',
        'checkins': 'yelp_academic_dataset_checkin.json',
        'reviews': 'yelp_academic_dataset_review.json',
        'users': 'yelp_academic_dataset_user.json'
    }


    # read and process province dataset
    province = Province(spark)
    df_provinces = province.process(bucket_name, dataset_files["provinces"])
    province.write_to_bq(df_provinces, bucket_name, dataset_name, "provinces")


    # read and process business dataset
    business = Business(spark)
    df_business_CA_final, df_category_rw, df_attribute_rw = business.process(bucket_name, dataset_files["businesses"])
    business.write_to_bq(df_business_CA_final, bucket_name, dataset_name, "businesses")
    business.write_to_bq(df_category_rw, bucket_name, dataset_name, "categories")
    business.write_to_bq(df_attribute_rw, bucket_name, dataset_name, "attributes")


    # read and process checkin dataset
    checkin = Checkin(spark)
    df_checkins = checkin.process(bucket_name, dataset_files["checkins"])
    checkin.write_to_bq(df_checkins, bucket_name, dataset_name, "checkins")


    # read and process review dataset
    review = Review(spark)
    df_reviews = review.process(bucket_name, dataset_files["reviews"])
    review.write_to_bq(df_reviews, bucket_name, dataset_name, "reviews")


    # read and process user dataset
    user = User(spark)
    df_users, df_user_friends, df_user_elites = user.process(bucket_name, dataset_files["users"])
    user.write_to_bq(df_users, bucket_name, dataset_name, "users")
    user.write_to_bq(df_user_friends, bucket_name, dataset_name, "user_friends")
    user.write_to_bq(df_user_elites, bucket_name, dataset_name, "user_elites")


# main section of the code
if __name__ == '__main__':
    bucket_name = sys.argv[1] #"cmpt732-project-bucket"
    bucket_path = 'gs://' + bucket_name #"gs://cmpt732-project-bucket"
    dataset_name = sys.argv[2] #"yelp_dataset"


    print(f"Bucket Name: {bucket_name}")
    print(f"Bucket Path: {bucket_path}")
    print(f"Dataset Name: {dataset_name}")


    #spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    #spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'ture')
    #spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', gc_credential_file)


    start_ETL(bucket_name, bucket_path, dataset_name)