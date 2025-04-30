import boto3
import pandas as pd

AWS_ACCESS_KEY_ID = 'AKIAS74TMDCP3GJNPR6'
AWS_SECRET_ACCESS_KEY = 'YOUR_SECRET_KEY'
AWS_REGION = 'ap-southeast-2'
BUCKET_NAME = 'salesdatafaris22'
S3_KEY = 'sales_data_sample.csv'

def download_from_s3():
    s3 = boto3.client('s3',
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_REGION)

    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_KEY)
    objects = response.get('Contents', [])

    if not objects:
        raise Exception("No files found in S3 bucket with prefix.")

    latest_file = sorted(objects, key=lambda obj: obj['LastModified'])[-1]
    file_key = latest_file['Key']

    local_filename = '/tmp/sales_data.csv'
    s3.download_file(BUCKET_NAME, file_key, local_filename)

    print(f"Downloaded {file_key} to {local_filename}")
