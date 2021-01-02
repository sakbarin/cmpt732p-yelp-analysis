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
gcloud dataproc clusters create cmpt732-project-cluster --region us-central1 --zone us-central1-f --image-version 1.5 --optional-components=Anaconda,Jupyter --bucket cmpt732-project-bucket --temp-bucket cmpt732-project-bucket --master-machine-type n1-standard-4 --master-boot-disk-size 50 --worker-machine-type n1-standard-8 --worker-boot-disk-size 50 --num-workers 2 --max-idle 1h --enable-component-gateway --project cmpt732-project
```

### Create a Google Cloud BigQuery dataset
In this part we will create a BigQuery dataset to store our cleaned dataset inside for further EDA purposes.<br/>
```
bq mk cmpt732-project:yelp_dataset
```

### Submit PySpark job to DataProc
Now this is the time to submit the PySpark job to DataProc to extract, transform, and load (ETL) dataset into BigQuery.
```
gcloud dataproc jobs submit pyspark main.py --project=cmpt732-project --cluster=cmpt732-project-cluster --region=us-central1 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar --py-files=baseclass.py,province.py,business.py,checkin.py,review.py,user.py -- cmpt732-project-bucket yelp_dataset
```