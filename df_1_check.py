import os
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import numpy as np
import boto3
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql.types import *
from io import StringIO
import sys
import logging

print("14 ===>")
logger = logging.getLogger()
_s = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_s.setFormatter(formatter)
logger.addHandler(_s)
logger.setLevel(logging.INFO)
logger.info(sys.argv)

#API_ENDPOINT = "s3-ap-south-1.amazonaws.com"
REGION_NAME = "ap-south-1" #Please mention the region to connect s3

S3_BUCKET_FOR_LOG = "myfirstbucketqwerty" #Please mention the output bucket name for log files
S3_BUCKET_LOG_PATH = "MyFirstProject/" #Provide the folder name or path for log_file without bucket name
S3_BUCKETS_IP_FILES = {"raw":"dev-mor-df1"} #Provide the Bucket Names for the layers
source_file_path = "s3://myfirstbucketqwerty/MyFirstProject/medical_hospital_claim.csv" #Provide the complete path for Source Input file for checking Source Codes

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
#Please provide the exact folder names here
dict_folders = {"raw":{"SUBMIT_HEADER":'vendor_source_code'}}

# Email alert for missing source codes for AWS SES service and aws access keys needs to be configured
print("varun==> 36")
# Creating S3 client from session.
session = boto3.Session()
s3 = boto3.client('s3') #initiating the connectivity for AWS
appName = "qualitycontrol" #Sets a name for application
#master = 'local[3]' #Sets the Spark master URL to connect to, such as "local" to run locally
conf = SparkConf().setAppName(appName) #the class which gives you the various option to provide configuration parameters
sc = SparkContext(conf=conf).getOrCreate() #Sparkcontext is the entry point for spark environment
sc.setLogLevel("WARN")
print("varun==> 45")
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
sqlContext = SQLContext(sc)

#The below function will return the DF having the input source code file for comparing the other files
def read_sourcefile():
    sdf = sqlContext.read.option("header", "true").csv(source_file_path)
    return sdf

#This is the main function to be called with Extract_Date and S3a://
def read_inputfiles(directory_, extract_date):
    #Loading the input source code csv file for comparing source codes
    sdf = read_sourcefile()
    #Creatign the Schema for output dataframe to write the information to the log_file.csv
    schema = StructType([StructField("source_code_expected", IntegerType(), True),
                         StructField("vendor_source_code", IntegerType(), True),
                         StructField("filename", StringType(), True),
                         StructField("date_of_reporting", StringType(), True),
                         StructField("layer",StringType(),True),
                         StructField("file_exists",StringType(),True)])
    output_df = sqlContext.createDataFrame([], schema)
    output_df_schema = output_df.columns
    #Looping all the layers from the dictionary such as raw, bronze, silver
    for dir, columns_ in dict_folders.items():
        #Looping each layer for the folder names such as pharmacy, submit_header, etc
        for folder, column_name in columns_.items():
            if dir == 'raw':
                # Framing the path with S3 bucket for ex: S3a://bucketname/
                directory = directory_+S3_BUCKETS_IP_FILES[dir]+'/'
                #Framing the folder path for ex: raw/drg/
                dir_ = dir+"/drg/"
                #Framing the final path for ex: S3a://bucketname/raw/drg/extract_date=20-05-21/
                path = directory + dir_ + folder + "/extract_date=" + extract_date + "/"
            elif dir == 'bronze':
                # Framing the path with S3 bucket for ex: S3a://bucketname/
                directory = directory_+S3_BUCKETS_IP_FILES[dir]+'/'
                # Framing the folder path for ex: bronze/drg/
                dir_ = dir + "/drg"
                # Framing the final path for ex: S3a://bucketname/bronze/drg/
                path = directory +dir_+"/"+folder +"/"
            elif dir == 'silver':
                #extract_date = '2020-04-16'
                # Framing the path with S3 bucket for ex: S3a://bucketname/
                directory = directory_+S3_BUCKETS_IP_FILES[dir]+'/'
                # Framing the final path for ex: S3a://bucketname/silver_df1_/pharmacy_transaction/extract_date=20-05-21/
                path = directory + "silver_df1_/" + folder + "/extract_date=" + extract_date + "/"
                s3_bucket = session.resource('s3')
                #Check all the files and folders available
                buck = s3_bucket.Bucket(S3_BUCKETS_IP_FILES[dir])
                bucket_files = list(buck.objects.all())
                #count = 0
                for rec in range(0, len(bucket_files)):
                    direct = bucket_files[rec].key
                    if (direct.find(path, 0, len(direct)) >= 0 and direct.endswith('.parquet')):
                        path = direct
            try:
                print(path)
                # Read the input  parquet file present in the path
                df = sqlContext.read.parquet(path)
                #If the df is having more than 0 records then compare or else say there is no data in the path
                if len(df.head(1)) > 0:
                    #Select column like vendor_id,etc and take distinct
                    df = df.select(column_name).distinct()
                    folder = folder.lower()
                    #Filter the data from Input Source Code File.
                    sdf_sb = sdf.filter(F.col('file_name') == folder).filter(F.col('layer') == dir ).filter(F.col('active') == 'Y')\
                    .select("source_code_expected")
                    #renaming the column names such as vendor_source_code,vendor_id to vendor_source_code
                    df = df.withColumnRenamed(column_name, 'vendor_source_code')
                    #Do a left join to take missing source code from the files
                    final_df = sdf_sb.join(df, sdf_sb.source_code_expected == df.vendor_source_code, 'left')
                    # renaming the column names such as vendor_source_code to vendor_source_code,vendor_id, data_source_code
                    final_df = final_df.withColumnRenamed('vendor_source_code',column_name)
                    #If the final DF is having more than zero records then do a union with output_Df to show it in Log_File
                    if len(final_df.head(1)) > 0:
                        final_df = final_df.withColumn('filename', F.lit(folder))\
                        .withColumn('date_of_reporting', F.lit(extract_date))\
                        .withColumn('layer',F.lit(dir))\
                        .withColumn('file_exists', F.lit('Y'))
                        output_df = output_df.union(final_df)
                else:
                    #If file exists and there is no data in it
                    empty_file_df = sqlContext.createDataFrame([(0, 0,folder, extract_date, dir, 'Y')], output_df_schema)
                    output_df = output_df.union(empty_file_df)
            except:
                # If file not exists in the given folder, Irrespective of any exception the control will be here
                print("File not exists in the folder "+path)
                no_files_df = sqlContext.createDataFrame([(0,0, folder, extract_date, dir, 'N')], output_df_schema)
                output_df = output_df.union(no_files_df)

    #Convert the Spark DF to Pandas DF to save the file in S3 bucket
    pandas_df = output_df.toPandas()
    print(pandas_df)
    # if len(pandas_df) > 0:
    #     #Send email Notification if the output log_file is having more than zero records
    #     send_email_notification_ses.send_email_notification_ses(sender, sender_name, recipient, subject, USERNAME_SMTP,
    #                                                             PASSWORD_SMTP, HOST, PORT, BODY_TEXT, BODY_HTML)
    #     csv_buffer = StringIO()
    #     pandas_df.to_csv(csv_buffer, header=True, index=False)
    #     csv_buffer.seek(0)
    #     logfile = S3_BUCKET_LOG_PATH+ '/log_file.csv'
    #     #Save the log_file.csv in Log S3 Bucket
    #     s3.put_object(Bucket=S3_BUCKET_FOR_LOG, Body=csv_buffer.getvalue(), Key=logfile)
    #     print(logfile)

if __name__ == '__main__':
    extract_date = '2021-01-14'
    #extract_date = sys.argv[1]
    s3_bucket_path = 's3a://'
    read_inputfiles(s3_bucket_path, extract_date)
    sc.stop()