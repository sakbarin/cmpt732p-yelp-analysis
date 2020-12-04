# An Insightful Analysis of YELP Dataset

# Requirements:

## Create virtual environment
conda create --name yelp_analysis_env

conda activate yelp_analysis_env

## Install libraries
pip install google-cloud-storage

## Download Google Cloud Storage (GCS) connector 
Download Cloud Storage connector for Hadoop from the following link to /path/spark/jars: <br/>
https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage <br/>

# Run:
spark-submit main.py gc-credential-file bucket-name bigquery-dataset-name <br/>
spark-submit main.py yelp-data-analysis-fcca66c851f1.json sa-yelp-dataset yelp_dataset



pip install matplotlib --user <br/>
pip install wordcloud --user <br/>
pip install nltk --user <br/>
pip install wikipedia --user <br/>

## Instructions to run

### Convert yelp json files to parquet
spark-submit business.py ../dataset/json/business.json ../dataset/json/province.json ../dataset/parquet/Business <br/>
spark-submit review.py ../dataset/parquet/Business ../dataset/json/review.json ../dataset/parquet/Review <br/>
spark-submit checkin.py ../dataset/parquet/Business ../dataset/json/checkin.json ../dataset/parquet/Checkin <br/>
spark-submit user.py ../dataset/parquet/Review ../dataset/json/user.json ../dataset/parquet/User <br/>

### Codes for data analysis
spark-submit business_analysis.py ../dataset/parquet/Business ../dataset/output/ <br/>
spark-submit checkin_analysis.py ../dataset/parquet/Checkin ../dataset/output/ <br/>
spark-submit user_analysis.py ../dataset/parquet/User ../dataset/output/ <br/>
spark-submit review_analysis.py ../dataset/parquet/Review ../dataset/output/ <br/>
spark-submit ethnicitydistribution_analysis.py ../dataset/parquet/Business ../dataset/postcode_toronto.csv ../dataset/wellbeing_ethnicity.csv/ <br/>
spark-submit labour_v_review_analysis.py ../dataset/parquet/Business ../dataset/wellbeing_labour.csv ../dataset/postcode_toronto.csv/ <br/>

### Codes for topic modelling
spark-submit topic_modelling.py ../dataset/parquet/Review ../dataset/parquet/Business ../dataset/output <br/>
spark-submit topicDistribution_explode.py ../dataset/output/Topic_low_business_topic_dist ../dataset/output/Topic_high_business_topic_dist ../dataset/output/ <br/>

### Codes for recommender part
spark-submit recommender_train.py ../dataset/output/BusinessSentiment ../dataset/parquet/User ../dataset/parquet/Review ../dataset/output/ <br/>
spark-submit recommender_test.py ../dataset/output/BusinessSentiment ../dataset/parquet/User ../dataset/parquet/Review ../dataset/output/TrainedModel ../dataset/output/ <br/>
