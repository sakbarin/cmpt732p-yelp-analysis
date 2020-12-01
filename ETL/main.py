import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


from pyspark.sql import SparkSession, functions, types
from google.cloud import storage


spark = SparkSession.builder.appName('YelpAnalysis_ETL').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')


def start_ETL(bucket_name, bucket_path):
    pass

if __name__ == '__main__':
    bucket_name = sys.argv[1]
    bucket_path = 'gs://' + bucket_name

    start_ETL(bucket_name, bucket_path)