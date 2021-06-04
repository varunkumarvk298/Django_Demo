import boto3
import pandas as pd
from pyspark.sql import SparkSession
import logging
from logging.config import dictConfig
import os
import pyspark.sql.functions as F


spark = SparkSession \
    .builder \
    .appName("df1_qc_6.1") \
    .getOrCreate()

access_key_id = 'AKIAWWJR22JASYNFCL7C',
secret_access_key = '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',
region = 'ap-south-1'
bucketname = 'myfirstbucketqwerty'
keyfile ='MyFirstProject/medical_hospital_claim.csv'

# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id = 'AKIAWWJR22JASYNFCL7C',
    aws_secret_access_key = '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',
    region_name = region
)
# Creating the high level functional client
resource_s3 = boto3.resource(
    's3',
    aws_access_key_id = access_key_id,
    aws_secret_access_key = secret_access_key,
    region_name = region,
)

# Create the S3 object
obj = client.get_object(
    Bucket= 'dev-mor-datalake-v2',
    Key=  'sliver_df1_/medical_hospital_claim_header/extract_date=2021-05-07/year=2021/month=6/day=1/pharmacy_transaction.csv', # medical_hospital_claim.csv
    #medical_hospital_claim_procedur.csv #	medical_hospital_remittance_claim_payment.csv #medical_hospital_remittance_service_payment.csv #pharmacy_transaction.csv
)

#connecting to the aws s3 ====testing
dfP = pd.read_csv(obj['Body'])
print(dfP.head())
ds = spark.createDataFrame(dfP)
ds.show()
ds.printSchema()

ds = ds.groupBy("date_of_service",'extract_date','create_ts').agg(
    F.countDistinct('claim_number').alias('claim_number'),
    F.countDistinct('total_charge').alias('total_charge'),
  )

ds.show()
print(ds.count())
print(ds.count())
totalrecords = ds.count()
ds = ds.withColumn("totalrecords", F.lit(totalrecords))
ds.show()