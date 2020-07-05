import ast
import boto3
import json
import pickle
from botocore.exceptions import ClientError


# Create client
def create_client():
    client = boto3.client(
        's3',
        aws_access_key_id='',
        aws_secret_access_key=''
    )
    return client


# Create Bucket
def create_bucket(bucket_name):
    client = create_client()
    try:
        client.create_bucket(Bucket=bucket_name)
        print('Bucket {} succesfully created'.format(bucket_name))
    except:
        print('ERROR in Creating new Bucket')
        return 0
    return 1


# List Bucket
def list_bucket():
    client = create_client()
    try:
        response = client.list_buckets()['Buckets']
        # print(response[0].get('Name'))
        print(response)
    except:
        print('ERROR in getting Bucket List from s3')
        return 0
    return 1


# Upload File
def upload_file(file_name, bucket, object_name=None):
    client = create_client()
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    try:
        client.upload_file(file_name, bucket, object_name)
        print('File {} uploaded succesfully'.format(file_name))
    except:
        print('ERROR in uploading File in s3')
        return 0
    return 1


# Read file
def read_file(bucket_name, file_name):
    client = create_client()
    try:
        file = client.get_object(
            Bucket=bucket_name, Key=file_name)['Body'].read()
        file_content = ast.literal_eval(file.decode('utf-8'))
        print(file_content)
    except:
        print('ERROR in Reading File from s3 Bucket')
        return 0
    return 1


# Download File from s3
def download_file(bucket_name, obj_file_name, file_name):
    client = create_client()
    try:
        client.download_file(bucket_name, file_name, file_name)
        print('File {} downloaded succesfully'.format(file_name))
    except:
        print('ERROR in Download File from s3 Bucket')
        return 0
    return 1


# Delete File from s3 bucket
def delete_file(bucket_name, file_name):
    client = create_client()
    try:
        client.delete_object(Bucket=bucket_name, Key=file_name)
        print('File {} deleted succesfully'.format(file_name))
    except:
        print('ERROR in Deleting File from s3 Bucket')
        return 0
    return 1


# Empty existing Bucket
def empty_s3_bucket(bucket_name):
    client = create_client()
    try:
        response = client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for item in response['Contents']:
                print('deleting file', item['Key'])
                client.delete_object(Bucket=bucket_name, Key=item['Key'])
                while response['KeyCount'] == 1000:
                    response = client.list_objects_v2(
                        Bucket=bucket_name,
                        StartAfter=response['Contents'][0]['Key'],)
                    for item in response['Contents']:
                        print('deleting file', item['Key'])
                        client.delete_object(Bucket=bucket_name, Key=item['Key'])

            print('Bucket {} now Empty'.format(bucket_name))
            return 0
        print('Bucket {} is Empty'.format(bucket_name))
    except:
        return 1


# Deleting s3 Bucket
def delete_bucket(bucket_name):
    client = create_client()
    try:
        empty_s3_bucket(bucket_name)
        client.delete_bucket(Bucket=bucket_name)
        print('Bucket {} got deleted succesfully'.format(bucket_name))
    except:
        print('ERROR in Deleting s3 Bucket')
        return 0
    return 1

