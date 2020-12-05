import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

#from pyspark.sql import SparkSession, functions, types

from pyspark.sql import SparkSession, functions, types
from google_cloud import GoogleCloud


from business import Business
from province import Province
from checkin import Checkin
from review import Review
from user import User


spark = SparkSession.builder.appName('YELP_ETL').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')


# define global variables
spark = SparkSession.builder.appName('YELP_ETL') \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta') \
    .config('spark.jars', 'gcs-connector-hadoop2-latest.jar') \
    .getOrCreate()

# start ETL pipeline
def start_ETL(gc_credential_file, bucket_name, bucket_path, dataset_name):
    gc = GoogleCloud(gc_credential_file)

    print('\nListing objects: ')
    gc.ls_objects(bucket_name, 'yelp_academic')


    province_file = 'yelp_academic_ca_provinces.json'
    province = Province(spark)
    df_provinces = province.process_file(bucket_path, province_file)
    df_provinces.show(10)

    business_file = 'yelp_academic_dataset_business.json'
    business = Business(spark)
    df_business_CA, df_business_CA_cat, df_business_CA_attr = business.process_file(bucket_path, business_file)

    checkin_file = 'yelp_academic_dataset_checkin.json'
    checkin = Checkin(spark)
    df_checkin = checkin.process_file(bucket_path, checkin_file)

    review_file = 'yelp_academic_dataset_review.json'
    review = Review(spark)
    df_review = review.process_file(bucket_path, review_file)

    user_file = 'yelp_academic_dataset_user.json'
    user = User(spark)
    df_user, df_user_friends, df_user_elite = user.process_file(bucket_path, user_file)


    #gc.write_to_bq(df_provinces, bucket_name, dataset_name, 'provinces')


# main section of the code
if __name__ == '__main__':
    gc_credential_file = sys.argv[1]
    bucket_name = sys.argv[2]
    dataset_name = sys.argv[3]

    bucket_path = 'gs://' + bucket_name

    bucket_path = 'C:\\Users\\Shahram\\Downloads\\Compressed\\yelp_dataset\\'

    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'ture')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', gc_credential_file)

    start_ETL(gc_credential_file, bucket_name, bucket_path, dataset_name)