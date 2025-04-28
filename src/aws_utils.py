"""Module for managing AWS access."""

import io
import os
import logging
import json
import pandas as pd
from dateutil.parser import parse
from boto3.session import Session
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)

aws_access_key = os.getenv("AWS_ACCESS_KEY", "admin")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
aws_endpoint_url_s3 = os.getenv("AWS_ENDPOINT_URL_S3", "http://minio:9000")
aws_region = os.getenv("AWS_REGION", "None")
bucket_name = os.getenv("BUCKET_NAME", "data")

session = Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region,
)

def get_file_from_s3(key):
    """Retrieving file from s3 bucket."""
    try:
        s3 = session.client('s3', endpoint_url=aws_endpoint_url_s3)
        data = s3.get_object(Bucket=bucket_name, Key=key)
        #data = obj['Body'].read().decode('utf-8')
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            print("(!) Key doesn't match. Please check the key value entered.")
            return None
    except NoCredentialsError as ex:
        print("(!) Unable to locate credentials. %s", ex)
        return None

    if data:
        # Returning the StreamingBody of the object from s3 bucket
        print(f"   (+) Returning <url>/<bucket>/<key>: '{aws_endpoint_url_s3}/{bucket_name}/{key}'")
        return data
    return None

def put_file_into_s3(data_buffer, key):
    """Writing file into s3 bucket."""
    try:
        s3 = session.client('s3', endpoint_url=aws_endpoint_url_s3)
        s3.put_object(
            Bucket=bucket_name, Key=key, Body=data_buffer.getvalue()
        )

    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            print("(!) Key doesn't match. Please check the key value entered.")
            return None
    except NoCredentialsError as ex:
        print("(!) Unable to locate credentials. %s", ex)
        return None

    return None

def write_event(record: str, offset: str) -> str:
    """Function for writing event to a file"""
    # Parse the JSON data into a Python dictionary
    record_dict = json.loads(record)

    df = pd.DataFrame(record_dict, index=[0])

    event_time = df['event_time'].values[:1][0]
    date_obj = parse(event_time[0:19])
    date_time = date_obj.strftime("%y-%m-%d_%H-%M-%S")
    event_type = df['event_type'].values[:1][0]
    key = '/raw/' + date_time + '_offset_' + offset + '_' + event_type + '.json'

    with io.StringIO() as json_buffer:
        df.to_json(json_buffer, orient='records')
        put_file_into_s3(json_buffer, key)

    return key