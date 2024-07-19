import boto3
from botocore.client import Config
import os

from storage_config import storage_conf, bucket_name

# S3 client init
s3_client = boto3.client(
    **storage_conf,
    config=Config(signature_version='s3v4')
)


def create_bucket(bucket):
    """
    :param bucket: bucket name (in lowercase, no underscores)
    """
    try:
        s3_client.create_bucket(Bucket=bucket)
        print(f'Bucket {bucket} successfully created')
    except Exception as e:
        print(f'Error while creating bucket: {e}')


def upload_file(file, bucket, object_name=None):
    """
    Upload file into S3 bucket

    :param file: path to file
    :param bucket: bucket's name
    :param object_name: Object`s name in storage (if None, uses file name)
    """
    if object_name is None:
        object_name = os.path.basename(file)

    try:
        s3_client.upload_file(file, bucket, object_name)
        print(f'File {file} was successfully uploaded {bucket}/{object_name}')
    except Exception as e:
        print(f'Error while uploading: {e}')


def delete_file(bucket, object_name):
    """
    Delete file from S3 bucket

    :param bucket: bucket's name
    :param object_name: Object`s name in storage
    """
    try:
        s3_client.delete_object(Bucket=bucket, Key=object_name)
        print(f'File {object_name} successfully deleted in {bucket}')
    except Exception as e:
        print(f'Error while deleting: {e}')


def get_object(bucket, object_name):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_name)
        content = response['Body'].read()
        return content
    except Exception as e:
        print(f"Error getting object {object_name}: {e}")
        return None


def head_object(bucket, object_name):
    try:
        response = s3_client.head_object(Bucket=bucket, Key=object_name)
        # print(response['ResponseMetadata']['HTTPStatusCode'])
        return response
    except Exception as e:
        print(f"Error getting metadata of {object_name}: {e}")
        return None

# create_bucket(bucket_name)
# file_path = '../kafka/producer.py'
# upload_file(file_path, bucket_name)
# delete_file(bucket_name, "producer.py")
# print(get_object(bucket_name, "load_gen.py"))
# print(head_object(bucket_name, "load_gen.py"))
