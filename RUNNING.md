# An Insightful Analysis of YELP Dataset

# Requirements:

## Google Cloud Section
### Create a project
Initially, you need to create a project in Google Cloud Platform (GCP). <br/>
In command line, you will enter the following command to create "CMPT732 Project":<br/>
```
gcloud projects create cmpt732-project --name "CMPT732 Project" 
```
Next, you need to link this project with your billing account.<br/>
You can use the following command to retrieve your billing account ID:<br/>
```
gcloud alpha billing accounts list
```
Note your billing account ID somewhere safe.<br/>
Use the following command to link your project with your billing account ID:<br/>
```
gcloud alpha billing accounts projects link cmpt732-project --billing-account <billing-account-id>
```

### Create a Google Cloud Storage bucket 
In this project, we have used Google Cloud Storage (GCS) to store YELP dataset.<br/>
Bucket "" is created for this purpose using the command that comes next:<br/>
```
gsutil mb -c standard -l us -p cmpt732-project gs://cmpt732-project-bucket
```
Copy YELP dataset from our bucket to your project bucket:
```
gsutil cp gs://sa-yelp-dataset/*.json gs://cmpt732-project-bucket
```

### Create a Google Cloud DataProc cluster
Next, we need to create DataProc cluster in order to process the dataset using PySpark.<br/>
To do so, we will use the following command:<br/>
```
gcloud dataproc clusters create cmpt732-project-cluster --region us-central1 --zone us-central1-f --image-version 1.5 --optional-components=Anaconda,Jupyter --bucket cmpt732-project-bucket --temp-bucket cmpt732-project-bucket --master-machine-type n1-standard-2 --master-boot-disk-size 50 --worker-machine-type n1-standard-4 --worker-boot-disk-size 50 --num-workers 2 --enable-component-gateway --project cmpt732-project
```

### Create a Google Cloud BigQuery dataset
In this part we will create a BigQuery dataset to store our cleaned dataset inside for further EDA purposes.<br/>
```
bq mk cmpt732-project:yelp_dataset
```

## Create virtual environment
We first need to create a virtual environment for the project: <br/>
```
conda create --name yelp_analysis_env
```

Then we will activate the environment using: <br/>
```
conda activate yelp_analysis_env
```

## Install libraries
This is the time to install libraries required to use Google Cloud Platform. <br/>
We have used YELP dataset in this project which is stored on Google Cloud Storage (GCS). <br/>
So, we need to install google-cloud-storage library using pip to import data. <br/>
```
pip install google-cloud-storage
```

## Download Google Cloud Storage (GCS) connector 
Additionaly, Spark need Google CLoud Storage connector to read data stored on Google Cloud Storage paths starting with gs:// <br/>
Download Cloud Storage connector for Hadoop from the following link to /path/spark/jars: <br/><br/>
[Download from this link](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) <br/>

# Run:
The pipeline extracts data from Google Cloud Storage (GCS), transforms and loads it into Google BigQuery for further analysis.<br/>
To start the process, run the following command: <br/>
```
spark-submit main.py <<gc-credential-file>> <<bucket-name>> <<bigquery-dataset-name>>
```
There are some parameters you need to set in this commands:
- gc-credential-file: This is the JSON credential file required to connect too Google Cloud.
- bucket-name: This is the bucket name where the dataset is stored (without gs://).
- bigquery-dataset-name: This is the name of big query dataset where you want to load data into.

An example of this command is shown here:
```
spark-submit main.py yelp-data-analysis-fcca66c851f1.json sa-yelp-dataset yelp_dataset
```


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
