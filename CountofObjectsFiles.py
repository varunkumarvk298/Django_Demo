import boto3
import pandas as pd
from pyspark.sql import SparkSession
import logging
# from logging.config import dictConfig
import pathlib
from pathlib import Path
import  os
import fnmatch



spark = SparkSession \
    .builder \
    .appName("ProjectX") \
    .getOrCreate()


# logging.basicConfig(filename="Test1.log", level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s')
logging.basicConfig(filename="logfilename.log", level=logging.INFO)




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
    Key='MyFirstProject/MOCK_DATA.csv'
    # Key='MyFirstProject/userdata1.parquet'
)
print(obj)
print(client)

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
















# dfP = spark.read.csv(obj['Body'])
# dfP.show()

# sparkDF = spark.read.csv("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ',')\
#     .load("s3://myfirstbucketqwerty/MyFirstProject/movies.csv", header=True)
#
# sparkDF.show()
#
# spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "y0r2/4QpH4IjF9izEgVoTHPqk+y4P7IPDqhsGMyw")
# spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

# sparkDF = spark.read.format('csv').options(header='true', inferSchema='true').load("s3://myfirstbucketqwerty/MyFirstProject/movies.csv")
# print(sparkDF.show())
# Loads RDD
# lines = spark.textFile("s3://myfirstbucketqwerty/MyFirstProject/movies.csv")
# # Split lines into columns; change split() argument depending on deliminiter e.g. '\t'
# parts = lines.map(lambda l: l.split(','))
# # Convert RDD into DataFrame
# df = spark.createDataFrame(parts)
# df.show()
# spark = SparkSession.builder \
#             .appName("Qwerty") \
#             .getOrCreate()


