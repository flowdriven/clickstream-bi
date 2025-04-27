"""Module for managing AWS access."""

import os
import logging
from boto3.session import Session
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)

aws_access_key = os.getenv("AWS_ACCESS_KEY", "admin")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
aws_endpoint_url_s3 = os.getenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
bucket_name = os.getenv("BUCKET_NAME", "data")

session = Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_access_key,
)

def get_file_from_s3(key):
    try:
        s3 = session.resource('s3', endpoint_url=aws_endpoint_url_s3)
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        data = obj['Body'].read().decode('utf-8').splitlines()
        return data
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            print("(!) Key doesn't match. Please check the key value entered.")
    except NoCredentialsError as ex:
        print("(!) Unable to locate credentials. %s", ex)
        
def main():
    pass

if __name__ == "__main__":
    main()