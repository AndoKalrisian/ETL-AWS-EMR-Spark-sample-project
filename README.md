# ETL on AWS EMR Cluster running pySpark

## Purpose

To create an analytics data lake for the analytics team at Sparkify so they can find insights into what songs their users are listening to.

## Database schema

![alt text](/star_schema.png "Star Schema")

This schema was used to optimize queries about what songs are being played.  All the infomation needed for these queries can be found by querying the fact table.  For other queries, all other tables can be accessed by using only one JOIN.

## ETL Pipeline

In this project, I am using pySpark running on an EMR cluster with 3 instances.  The EMR cluster running Spark allows for faster transformation of the data. The pipeline used in this project is as follows:
0. Spin up EMR cluster and run the following steps as one step of a Spark job.
1. Extract songs JSON data stored in a S3 Bucket, store in memory in a Spark dataframe running on an EMR cluster
2. Extract and transform, from the songs data, the songs and artists tables according to the database schema defined above.
3. Store songs and artist tables in S3 as individual parquet files. Also keep a local cluster copy to use in a later step.
4. Extract logs JSON data stored in a S3 Bucket, store in memory in a Spark dataframe running on an EMR cluster.
5. Extract and transform, from the logs data, the users and time tables according to the database schema defined above.
6. Store users and time tables in S3 as individual parquet files.
7. Extract and transform, from the logs data and the stored songs and artists tables, the songplays table according to the database schema defined above.
8. Shutdown EMR cluster.

## How to run this ETL process on AWS

1. Create an S3 bucket and add a folder for 'src', 'output', and 'logs'.
2. Upload 'etl.py' into the 'src' folder on S3 bucket
3. Use the AWS CLI to spin up cluster, run Spark job and then terminate when completed. Following is a sample of AWS CLI code I used:
```
aws emr create-cluster --name "Spark cluster with step" --release-label emr-5.29.0 --applications Name=Spark Name=Hive Name=Livy --log-uri s3://de-nano-project-4/logs --ec2-attributes KeyName=emr-from-cli --instance-type m5.xlarge --instance-count 3 --steps Type=Spark,Name="Spark job",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,s3://de-nano-project-4/src/etl.py] --use-default-roles --auto-terminate
```

A nicer way to view this is as follows:
```aws emr create-cluster --name "Spark cluster with step" \
       --release-label emr-5.29.0 \
       --applications Name=Spark \
       --log-uri s3://de-nano-project-4/logs \
       --ec2-attributes KeyName=emr-from-cli \
       --instance-type m5.xlarge \
       --instance-count 3 \
       --steps Type=Spark,Name="Spark job",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,/
                                                                          --master,yarn,s3://de-nano-project-4/src/etl.py] /
       --use-default-roles --auto-terminate
```
## Testing

I included a jupyter notebook (data-validation.ipynb) with simple data validation on each of the tables saved in my S3 bucket.  This should be run on EMR using their notebook feature.

## Other useful notes

### How to connect to EMR Cluster via SSH

1. Create and download '.pem' credentials.
2. Change file permissions on '.pem' file:
```$ chmod 0400 .ssh/my_private_key.pem```
3. Log in to cluster (example):
``` $ ssh -i my_private_key.pem hadoop@ec2-54-245-27-75.us-west-2.compute.amazonaws.com```

#### Copy files to emr cluster:

scp -i maclinux-ec2.pem etl.py hadoop@ec2-54-245-27-75.us-west-2.compute.amazonaws.com:src/

### Use AWS CLI to spin up cluster, run spark job and then terminate when complete:

```

View cluster data:

```aws emr describe-cluster --cluster-id j-id-here```

This was adapted from the following article:
[Production Data Processing with Apache Spark](https://towardsdatascience.com/production-data-processing-with-apache-spark-96a58dfd3fe7)

Debugging - Where is the stack trace?

First I looked in the steps 'stderr' logs and found an error related to a specific application id.  Then I went into the 'container' log folder and click on the application with that id (in this case it was the only application).  Then I went in the first 'container' folder and found the stack traceback in the 'stdout' log file.
