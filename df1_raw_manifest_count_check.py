# This script checks the number of records for each file provided in the manifest file is matching with the raw file record count.
# Files will be available in S3 based on extract date like extract_date=2019-10-10 and under the extract date folder different domains folder (claims, header etc) will be available and file need to be check in those folders.
# Send an alert (email) if raw file counts are not matching with manifest.
# Stores file name, extract date, record count - file, record count - manifest , match indicator  for  reporting
# Code is parameterized to accept the file location from S3 as well as extract date as code will look at different extract date on a weekly basisThe scope of this script is to count records in a folder
#
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3
# import delta
import csv
import os
import logging
# from common.utilities.utils import *

APP_NAME = "DF1 Raw Manifest Count Check"
spark = get_pyspark_session(APP_NAME)
job = configure_glue_job(spark)
logger = configure_logger()
s3 = boto3.client('s3')

def record_count_and_compare(extract_date: str, dict_files: dict, df1_default_path):
    '''        Function to count records of csv file at given S3 location and compare it with manifest files    '''
    try:
        rows = list()  # List to get all the rows for all folders with filename, extract_date and record count of files
        logger.info("############# Inside Count #############\n")

        for key in dict_files:
            if key != 'manifest':
                logger.info("############# Inside for.if folder expect Manifet#############\n")
                # print(dict_files)
                df = read_parquet_file(spark, dict_files[key])
                count = df.count()
                # print(key)
                # print(count)
                rows.append([key, extract_date, str(count)])
            else:
                # df_manifest = read_csv_file(spark, dict_files[key],inferSchema=True, header=True, delimiter = "," , dict_files[key] )
                logger.info("############# Inside Manifet#############\n")
                # print(key)
                df_manifest = spark.read.csv(dict_files[key], header=True, inferSchema=True)
                df_manifest.show()

        """
        filename = 'record_count_all_files_' + extract_date + '.csv'
        fields = ['filename', 'extract_date', 'record_count_file'] 
        with open(filename, 'a', newline='\n') as csvfile: 
            csvwriter = csv.writer(csvfile) 
            csvwriter.writerow(fields) 
            csvwriter.writerows(rows)
        final_df = spark.read.csv(filename, header = True, inferSchema = True)
        final_df.show()
        join_df = final_df.join(df_manifest, final_df.filename == df_manifest.TABLE_NAME)
        join_df.show()
        join_df = join_df.select('filename', 'extract_date', 'record_count_file', col('RECORD_COUNT').alias('record_count_manifest'))
        join_df = join_df.withColumn('difference', (join_df['record_count_file'] - join_df['record_count_manifest']))
        join_df = join_df.withColumn('is_count_matching', when(col('difference') == 0, "Yes").otherwise("No"))
        join_df.show()

        write_parquet_file(join_df, "extract_date", df1_default_path,"append")
        """
        """
        df_to_pandas = join_df.toPandas()
        print(df_to_pandas)
        output_file = open('output_record_count_' + extract_date + '.csv', 'w')
        df_to_pandas.to_csv(output_file, index = False, header=True, line_terminator = '\n')
        # write_file_to_s3(output_file)
        os.remove(filename)
        sc.stop()
        """
    except Exception as e:
        print('error in record_count(): ', e)
        raise e


def list_folders_in_bucket(extract_date: str, bucket: str, prefix_name: str):
    ''' Function to get list of all folders at particular location in S3 bucket
        Input : extract date, bucket name and prefix
        Output : list of folders in s3 at the input location
    '''
    try:
        prefix = prefix_name + 'extract_date={0}/'.format(extract_date)
        paginator = s3.get_paginator('list_objects')
        folders = []
        iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/', PaginationConfig={'PageSize': None})
        for response_data in iterator:
            prefixes = response_data.get('CommonPrefixes', [])
            for prefix in prefixes:
                prefix_name = prefix['Prefix']
                if prefix_name.endswith('/'):
                    prefix_name = prefix_name.rstrip('/')
                    index = prefix_name.rfind('/')
                    prefix_name = prefix_name[index + 1:]
                    folders.append(prefix_name)
        return folders
    except Exception as e:
        print('error in list_folders_in_bucket(): ', e)
        raise e


def get_s3_log_file_name(extract_date: str, bucket: str, prefix_name: str, folders: set):
    """Get list of s3 file uri as value and folder name as key in dictionary for a given date
       Input: extract date, input bucket name, prefix and set of folders
       Output : Dictionary, key = name of folder and values = list of all files in that folder, eg. {SUBMIT_HEADER : [file1.csv, file2.csv]}
    """
    try:
        files_dict = dict()
        for folder in folders:
            files_dict[folder] = 's3a://{0}/{1}extract_date={2}/{3}/'.format(bucket, prefix_name, extract_date, folder)
        return files_dict
    except Exception as e:
        print('error in get_s3_log_file_name(): ', e)
        raise e


def has_key(obj: dict, key: str):
    try:
        if obj:
            return key in obj.keys()
        else:
            return False
    except Exception as e:
        print('error in has_key() with key: {0} :: {1}'.format(key, e))
        return False


"""
def write_file_to_s3(file):
    ''' Function to save csv file to S3 bucket at given location in FILE_PATH'''
    try:
        logging.info('Writing file to S3.')
        s3_out = boto3.resource('s3', 
                        region_name = REGION_NAME,
                        aws_access_key_id = ACCESS_KEY_ID,
                        aws_secret_access_key = SECRET_ACCESS_KEY)
        s3_out.meta.client.upload_file(file, S3_BUCKET_OUT, FILE_PATH)
    except Exception as e:
        print('error in write_file_to_s3() with key: {0} :: {1}'.format(key, e))
        return False
"""


def main():
    # Dictionary: prefix as key and bucket as values
    DF1_SOURCE_PATH_KEY = "df1_default_path"
    # DF1_GET_PATHS = "df1_paths"

    job_name = get_glue_job_name()
    config_file_path = get_config_file_path()
    job_config_dict = get_job_config(config_file_path, job_name)
    df1_default_path = job_config_dict[DF1_SOURCE_PATH_KEY]
    # dict_of_bucket_and_prefix = {'raw/drg/' : 'dev-mor-df1', 'bronze/drg/' : 'dev-mor-df1', 'silver_df1_/' : 'dev-mor-datalake'}
    # dict_of_bucket_and_prefix = job_config_dict[DF1_GET_PATHS]
    # extract_date = get_extract_date()

    # S3_BUCKET_OUT = 'mor-dl-prod' # Please mention the s3 output bucket name where you want to store logs
    # FILE_PATH = 'raw/drg/'    # Please mention the output file path to store logs while writing to S3
    input_bucket = 'dev-mor-drg-input'  # Please mention the input bucket name
    s3_prefix_name = 'DRG/data/raven/incremental_extract/'  # Please mention the prefix where your folders are, before extract_date only
    # raw_file_location = "s3://dev-mor-drg-input/DRG/data/raven/incremental_extract/"
    files_dict = {}
    logger.info("############# Begin Job #############\n")
    logger.info("############# App Name: " + APP_NAME + " #############\n")
    try:
        extract_date = '2021-04-22'  # Please change the extract date as per the requirement
        folders = set(list_folders_in_bucket(extract_date, input_bucket,
                                             s3_prefix_name))  # List of all folders at given S3 location
        dict_files = get_s3_log_file_name(extract_date, input_bucket, s3_prefix_name,
                                          folders)  # Dictionary of folder (key) and list of files in folder (values)
        record_count_and_compare(extract_date, dict_files,
                                 df1_default_path)  # Count the records of raw file and compare it with manifest file


    except Exception as ex:
        print('error in __main__ : {0}', ex)
        logger.info("############# Committing the Job ... #############\n")
        job.commit()
        logger.info("############# End Job #############\n")
        raise ex


if __name__ == "__main__":
    main()