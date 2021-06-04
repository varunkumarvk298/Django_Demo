import boto3
import pandas as pd
import datetime


def check_expected_folder(client, bucket, prefix, expected_folders):
    # Below piece of code returns ONLY the 'subfolders' in a 'folder' from s3 bucket.

    result = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')

    folders_present = [x.get('Prefix')[:-1] for x in result.get('CommonPrefixes')]

    # print(folders_present)
    folders_not_present = set(folders_present) ^ set(expected_folders)
    return folders_present, folders_not_present


def update_folder_log(folders_not_present, log_df):
    new_row = {'timestamp': str(datetime.datetime.now()), 'extract_date': extract_date,
               'no_of_folders': len(folders_present), 'name_of_folders': expected_folders}

    log_df = log_df.append(new_row, ignore_index=True)
    log_df = log_df.assign(name_of_folders=log_df.name_of_folders).explode('name_of_folders')
    for folder_not_present in folders_not_present:
        log_df.loc[log_df.name_of_folders == folder_not_present, 'is_folder_missing'] = "Yes"
    log_df['is_folder_missing'] = log_df['is_folder_missing'].fillna("No")
    return log_df


def get_non_empty_folders(s3, bucket):
    my_bucket = s3.Bucket(bucket)

    folders_that_are_not_empty = []
    for s3_file in my_bucket.objects.all():
        if (s3_file.key.endswith('/') | s3_file.key.endswith('v')):  # v for csv
            # This is a folder
            pass
        else:
            non_empty_folder_name = s3_file.key.rsplit('/', 1)

            folders_that_are_not_empty.append(non_empty_folder_name[0])
    return folders_that_are_not_empty


def update_is_folder_empty_log(folders_present, folders_that_are_not_empty, log_df):
    folders_that_are_empty = list(
        set(folders_that_are_not_empty) ^ set(folders_present))  # how many folders are empty out of present folders

    for empty_folder in folders_that_are_empty:
        log_df.loc[log_df.name_of_folders == empty_folder, 'is_empty'] = "Yes"
    log_df['is_empty'] = log_df['is_empty'].fillna("No")
    return log_df


log_df = pd.DataFrame(columns=["timestamp", "extract_date", "no_of_folders", "name_of_folders", "is_folder_missing", "is_empty"])

ACCESS_KEY = 'AKIAR7CSWD2KUJEJLC4H '
SECRET_KEY = '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'
REGION_NAME = 'us-east-1'

default_path = r"C:\Users\abc\Desktop\albanero\data engineering\python-data-cleansing-main\data/"
bucket = 'albanero-mor-drg-input'
extract_date = '2021-04-08'
prefix = 'DRG/data/raven/incremental_extract/extract_date=' + extract_date + "/"

expected_folders = ['DRG/data/raven/incremental_extract/extract_date=2021-04-08/CLAIMS_REMITS_CLAIM_ADJUSTMENT',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/SUBMIT_PATIENT',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/manifest',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/SUBMIT_PROVIDER',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/CLAIMS_REMITS_CLAIM_PAYMENT',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/CLAIMS_REMITS_SERVICE_PAYMENT',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/SUBMIT_HEADER',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/CLAIMS_REMITS_COB',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/SUBMIT_PAYER',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/PHARMACY',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/SUBMIT_PROCEDURE',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/SUBMIT_REMIT_CONNECTION',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/REMITS_SERVICE_ADJUSTMENT',
                    'DRG/data/raven/incremental_extract/extract_date=2021-04-08/SUBMIT_DIAGNOSIS']

client = boto3.client(service_name='s3',
                      region_name=REGION_NAME,
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

s3 = boto3.resource(service_name='s3',
                    region_name=REGION_NAME,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY)

folders_present, folders_not_present = check_expected_folder(client, bucket, prefix, expected_folders)

log_df = update_folder_log(folders_not_present, log_df)

# Check if folders are empty

folders_that_are_not_empty = get_non_empty_folders(s3, bucket)
log_df = update_is_folder_empty_log(folders_present, folders_that_are_not_empty, log_df)
log_df.to_csv(default_path + "log_file.csv", index=False)

s3.Bucket(bucket).upload_file(default_path + '/log_file.csv',
                              '%s/%s' % ('DRG/data/raven/incremental_extract', 'log_file.csv'))