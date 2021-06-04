import boto3
import pandas as pd
from pyspark.sql import SparkSession
import logging
# from logging.config import dictConfig
import pathlib
from pathlib import Path
import  os
import fnmatch
import urllib


spark = SparkSession \
    .builder \
    .appName("ProjectX") \
    .getOrCreate()


# logging.basicConfig(filename="Test1.log", level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s')
logging.basicConfig(filename="logfilename.log", level=logging.INFO)




# Creating the low level functional client
clients3 = boto3.client(
    's3',
    aws_access_key_id = 'AKIAWWJR22JASYNFCL7C',
    aws_secret_access_key = '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',
    region_name = 'ap-south-1',
)

# Creating the low level functional client
resources3 = boto3.resource(
    's3',
    aws_access_key_id = 'AKIAWWJR22JASYNFCL7C',
    aws_secret_access_key = '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',
    region_name = 'ap-south-1',
)

# Create the S3 object
obj = clients3.get_object(
    Bucket='myfirstbucketqwerty',
    Key='MyFirstProject/MOCK_DATA.csv'
    # Key='MyFirstProject/userdata1.parquet'
)

# creasting a variable in bucket and files
bucket='myfirstbucketqwerty'
File='MyFirstProject/'
filepath ="s3://myfirstbucketqwerty/MyFirstProject/movies.csv"
# # df = spark.read.csv("s3://myfirstbucketqwerty/MyFirstProject/movies.csv")
# df = spark.read.format('csv').options(header='true', inferSchema='true').load(filepath)
# df.show()
# df.printSchema()

dfP = pd.read_csv(obj['Body'])
print(dfP.head())

df = spark.createDataFrame(dfP.astype(str))
df.show()



