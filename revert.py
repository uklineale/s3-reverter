import boto3
from botocore.exceptions import ClientError
import datetime
import pytz

utc = pytz.UTC
s3 = boto3.client('s3')

BUCKET_NAME = ''
TARGET_DATE = utc.localize(datetime.datetime(2021,8,9))
NEW_FILE_STR = 'no_version_id_can_match_my_power'
SKIP_LIST = ['folder-to-skip', 'folder-to-skip-1']


def get_latest_version_before(versions, target_date):
    version_ids = [ version['VersionId'] for version in versions if version['LastModified'] < target_date]
    if len(version_ids) > 0:
        return version_ids[0]
    else:
        return NEW_FILE_STR # All versions of file are newer 
    

# Rollback object by deleting all earlier versions
def rollback_object(bucket_name, object_key, target_date):
    # Get versions for object
    versions = sorted(s3.list_object_versions(Bucket=bucket_name,Prefix=object_key)['Versions'], key=lambda v:v['LastModified'],reverse=True) 
    # Get target version to revert to
    version_id = get_latest_version_before(versions, target_date)

    if version_id == NEW_FILE_STR:
        print(f"Skipping object {object_key} from {bucket_name}")
    else:
        print(f"Rolling back to version {version_id} of object {object_key} from {bucket_name}")
        
    for version in versions:
        curr_version_id = version['VersionId']

        # Delete versions more recent than target version
        if  curr_version_id != version_id:
            s3.delete_object(Bucket=bucket_name, Key=object_key, VersionId=curr_version_id)
            print(f"Deleted version {curr_version_id} of object {object_key} from {bucket_name}")
        else: 
            break # Stop when you've gotten to the latest, don't delete later versions

def should_skip_item(object_key):
    for i in SKIP_LIST:
        if i in object_key:
            return True
    return False     


def rollback_bucket(bucket, target_date):
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME)
    for page in pages:
        for obj in page['Contents']:
            key = obj['Key']
            if not should_skip_item(key): 
                rollback_object(BUCKET_NAME, key, target_date)
    
rollback_bucket(BUCKET_NAME, TARGET_DATE)
