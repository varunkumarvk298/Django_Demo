# The scope of this script is to count the number of records in the defined files for extract date
# Store file name, extract date, record count for  reporting
# Files will be available in S3 based on extract date like extract_date=2019-10-10 and
# under the extract date folder different domains folder (claims, header etc) will be available and
# file need to be check in those folders.
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, regexp_replace, expr, substring, when, isnull, count, lit, round, \
    countDistinct, input_file_name
import numpy as np
import boto3
from datetime import datetime, timedelta
import csv
import logging
from common.utilities.utils import *

import sys
print(sys.path)

APP_NAME = "DF1 Record Count - Raw Input, Raw, Bronze, Silver"
spark = get_pyspark_session(APP_NAME)
job = configure_glue_job(spark)
logger = configure_logger()

session = boto3.Session()
s3 = boto3.client('s3')  # initiating the connectivity for AWS

raw_file_names = set([
    "SUBMIT_HEADER",
    "SUBMIT_PROCEDURE",
    "SUBMIT_DIAGNOSIS",
    "SUBMIT_PAYER",
    "SUBMIT_PROVIDER",
    "SUBMIT_PATIENT",
    "PHARMACY",
    "REMITS_CLAIM_ADJUSTMENT",
    "REMITS_CLAIM_PAYMENT",
    "REMITS_COB",
    "REMITS_SERVICE_ADJUSTMENT",
    "REMITS_SERVICE_PAYMENT",
    "SUBMIT_REMIT_CONNECTION"
])

bronze_file_names = set([
    "submit_header",
    "submit_procedure",
    "submit_diagnosis",
    "submit_payer",
    "submit_provider",
    "submit_patient",
    "pharmacy",
    "remits_claim_adjustment",
    "remits_claim_payment",
    "remits_cob",
    "remits_service_adjustment",
    "remits_service_payment",
    "submit_remit_connection"
])

silver_file_names = set([
    "medical_hospital_claim_header",
    "medical_hospital_claim_procedure",
    "medical_hospital_claim_diagnosis",
    "medical_hospital_claim_payer",
    "medical_hospital_claim_provider",
    "medical_hospital_claim_patient",
    "pharmacy_transaction",
    "medical_hospital_remittance_claim_adjustment",
    "medical_hospital_remittance_claim_payment",
    "medical_hospital_remittance_cob",
    "medical_hospital_remittance_service_adjustment",
    "medical_hospital_remittance_service_payment",
    "medical_hospital_remittance_connection"
])


def get_record_count(extract_date: str, dict_files: dict, bucket: str, prefix: str, location: str,
                     df1_default_path: str):
    '''
        Function to count records of csv files at given S3 location. It also writes the csv in "s3_path, filename, extract_date, record_count" format
    '''
    try:

        rows = []
        for key in dict_files:
            # print(key)
            path = dict_files[key]
            logger.info("\n############# Inside get_record_count ... #############\n")
            if (location == 'raw' or location == 'raw_input'):
                logger.info("\n ############# Inside raw get_record_count ... #############\n")
                print(path)
                logger.info("\n ############# START READING PARQUET... #############\n")
                df = read_parquet_file(spark, path)
                logger.info("\n ############# COMPLETE READING PARQUET... #############\n")
                count = df.count()
                rows.append([bucket + '/' + prefix, key, extract_date, location, str(count)])
            else:
                logger.info("\n ############# Inside silver/bronze get_record_count ... #############\n")
                print(path)
                df = read_delta_file(spark, path, extract_date)
                count = df.count()
                rows.append([bucket + '/' + prefix, key, extract_date, location, str(count)])

        fields = ['s3_path', 'filename', 'extract_date', 'location', 'record_count']
        final_df = spark.createDataFrame(rows, fields)
        logger.info("\n############# Inside Writing Files ... #############\n")
        write_parquet_file(final_df, "extract_date", df1_default_path, "append")

    except Exception as e:
        raise e


def list_folders_in_bucket_raw_input(extract_date: str, bucket, prefix_name):
    try:
        prefix = prefix_name + 'extract_date={0}/'.format(extract_date)
        paginator = s3.get_paginator('list_objects')
        folders = []
        iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/', PaginationConfig={'PageSize': None})
        for response_data in iterator:
            logger.info("\n############# Inside list_folders_in_bucket_raw_input ... #############\n")
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
        print('error in list_folders_in_bucket_raw_input(): ', e)
        raise e


def list_folders_in_bucket_raw_bronze_silver(extract_date: str, bucket: str, prefix_name: str):
    try:
        paginator = s3.get_paginator('list_objects')
        folders = []
        iterator = paginator.paginate(Bucket=bucket, Prefix=prefix_name, Delimiter='/',
                                      PaginationConfig={'PageSize': None})
        for response_data in iterator:
            logger.info("\n############# Inside list_folders_in_bucket_raw_bronze_silver ... #############\n")
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
        print('error in list_folders_in_bucket_raw_bronze_silver(): ', e)
        raise e

def get_s3_files_raw_input(extract_date: str, bucket: str, prefix_name: str, folders: set, location: str):
    """Get list of s3 file uri as value and folder name as key in dictionary for a given date
       Input: extract date, input bucket name, prefix and set of folders
       Output : Dictionary, key = name of folder and values = list of all files in that folder, eg. {SUBMIT_HEADER : [file1.csv, file2.csv]}
    """
    try:
        files_dict = dict()
        logger.info("\n############# Inside get_s3_files_raw_input ... #############\n")
        for folder in folders:
            if location == "raw_input":
                files_dict[folder] = 's3a://{0}/{1}extract_date={2}/{3}/'.format(bucket, prefix_name, extract_date,
                                                                                 folder)
            elif location == "raw":
                files_dict[folder] = 's3a://{0}/{1}{2}/extract_date={3}/'.format(bucket, prefix_name, folder,
                                                                                 extract_date)
            else:
                files_dict[folder] = 's3a://{0}/{1}{2}'.format(bucket, prefix_name, folder)
        # print(files_dict)
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


def main():
    DF1_SOURCE_PATH_KEY = "df1_default_path"
    DF1_SOURCE_CODE_INPUT_PATH = "source_file_path"
    DF1_GET_PATHS = "df1_paths"
    job_name = get_glue_job_name()
    config_file_path = get_config_file_path()
    job_config_dict = get_job_config(config_file_path, job_name)
    df1_default_path = job_config_dict[DF1_SOURCE_PATH_KEY]
    # S3_BUCKET = job_config_dict[DF1_S3_BUCKET]
    # Dictionary: prefix as key and bucket as values
    dict_of_bucket_and_prefix = {'DRG/data/raven/incremental_extract/': 'dev-mor-drg-input', 'raw/drg/': 'dev-mor-df1',
                                 'bronze/drg/': 'dev-mor-df1', 'silver_df1_/': 'dev-mor-datalake'}
    extract_date = '2021-03-11'  # Extract date to be taken from the extract-date file stored on AWS
    logger.info("\n############# Inside Main ... #############\n")
    # dict_of_bucket_and_prefix = job_config_dict[DF1_GET_PATHS]
    # extract_date = get_extract_date()



try:

    for key in dict_of_bucket_and_prefix:
        if key == 'DRG/data/raven/incremental_extract/':
            location = "raw_input"
            logger.info("\n############# Inside Raw Input ... #############\n")
            raw_input_folders = set(list_folders_in_bucket_raw_input(extract_date, dict_of_bucket_and_prefix[key], key))
            folders_present = raw_input_folders.intersection(raw_file_names)  # Removing any dummy folder at S3 location
            dict_files = get_s3_files_raw_input(extract_date, dict_of_bucket_and_prefix[key], key, folders_present,
                                                location)
            get_record_count(extract_date, dict_files, dict_of_bucket_and_prefix[key], key, location, df1_default_path)
        elif key == 'raw/drg/':
            location = "raw"
            logger.info("\n############# Inside Raw  ... #############\n")
            raw_input_folders = set(
                list_folders_in_bucket_raw_bronze_silver(extract_date, dict_of_bucket_and_prefix[key], key))
            folders_present = raw_input_folders.intersection(raw_file_names)  # Removing any dummy folder at S3 location
            dict_files = get_s3_files_raw_input(extract_date, dict_of_bucket_and_prefix[key], key, folders_present,
                                                location)
            get_record_count(extract_date, dict_files, dict_of_bucket_and_prefix[key], key, location, df1_default_path)
        elif key == 'bronze/drg/':
            location = "bronze"
            logger.info("\n############# Inside Bronze ... #############\n")
            raw_input_folders = set(
                list_folders_in_bucket_raw_bronze_silver(extract_date, dict_of_bucket_and_prefix[key], key))
            print(raw_input_folders)
            folders_present = raw_input_folders.intersection(
                bronze_file_names)  # Removing any dummy folder at S3 location
            print(folders_present)
            dict_files = get_s3_files_raw_input(extract_date, dict_of_bucket_and_prefix[key], key, folders_present,
                                                location)
            print(dict_files)
            get_record_count(extract_date, dict_files, dict_of_bucket_and_prefix[key], key, location, df1_default_path)
        elif key == 'silver_df1_/':
            location = "silver"
            logger.info("\n############# Inside Silver ... #############\n")
            raw_input_folders = set(
                list_folders_in_bucket_raw_bronze_silver(extract_date, dict_of_bucket_and_prefix[key], key))
            folders_present = raw_input_folders.intersection(
                silver_file_names)  # Removing any dummy folder at S3 location
            dict_files = get_s3_files_raw_input(extract_date, dict_of_bucket_and_prefix[key], key, folders_present,
                                                location)
            get_record_count(extract_date, dict_files, dict_of_bucket_and_prefix[key], key, location, df1_default_path)

            logger.info("############# Committing the Job ... #############\n")
            job.commit()
            logger.info("############# End Job #############\n")

except Exception as ex:
    print('error in __main__ : {0}', ex)
    logger.info("############# Committing the Job ... #############\n")
    job.commit()
    logger.info("############# End Job #############\n")
    raise ex

if __name__ == "__main__":
    main()
