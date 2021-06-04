import os
# from aws_secret_keys import secret_keys
import pandas as pd
from pyspark import SparkContext, SparkConf, SQLContext
import pyspark.sql.functions as F
import boto3
from pyspark.sql.types import *
from io import StringIO, BytesIO
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("ProjectX") \
    .getOrCreate()

# #You can ignore the below variables
# #Starts Here
# #API_ENDPOINT = "s3-ap-south-1.amazonaws.com"
# REGION_NAME = "ap-south-1" #Please mention the region to connect s3
# S3_BUCKET_FOR_LOG = "myfirstbucketqwerty" #Please mention the output bucket name for log files
# S3_BUCKET_LOG_PATH = "MyFirstProject" #Provide the folder name or path for log_file without bucket name
# S3_ENDPOINT = "ap-south-1.amazonaws.com" #Provide the endpoint
#
# #Ends Here
#
# os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
#
# # Creating S3 client from session.
# session = boto3.Session(
#             region_name = 'ap-south-1',
#             aws_access_key_id = 'AKIAWWJR22JASYNFCL7C',
#             aws_secret_access_key = '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',
#         )
# s3 = session.client('s3') #initiating the connectivity for AWS
#
# appName = "forian quality control" #Sets a name for application
# master = 'local[3]' #Sets the Spark master URL to connect to, such as "local" to run locally
# conf = SparkConf().setAppName(appName).setMaster(master) #the class which gives you the various option to provide configuration parameters
# sc = SparkContext(conf=conf).getOrCreate() #Sparkcontext is the entry point for spark environment
# sc.setLogLevel("WARN")
#
# hadoop_conf = sc._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3a.awsAccessKeyId", 'AKIAWWJR22JASYNFCL7C',)
# hadoop_conf.set("fs.s3a.awsSecretAccessKey", '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',)
# hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
# hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
# hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
# hadoop_conf.set("fs.s3a.endpoint", S3_ENDPOINT)
# sqlContext = SQLContext(sc)
# spark = sqlContext
#


# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id = 'AKIAWWJR22JASYNFCL7C',
    aws_secret_access_key = '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',
    region_name = 'ap-south-1'
)


# Create the S3 object
obj = client.get_object(
    Bucket='myfirstbucketqwerty',
    Key='MyFirstProject/medical_hospital_claim.csv'
    # Key='MyFirstProject/userdata1.parquet'
)
print(obj)
print(client)


df = pd.read_csv(obj['Body'])
print(df)

dfS = spark.createDataFrame(df)
print(dfS.show())
