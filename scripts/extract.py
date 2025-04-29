import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def download_from_s3():
    s3 = boto3.client('s3',
                      aws_access_key_id=os.getenv('AKIAS74TMDCP3GJNPRI'),
                      aws_secret_access_key=os.getenv('N5g3Lm3BtZFUVe7DJdyhoqdkzApbdYuUJlunljZ'),
                      region_name=os.getenv('ap-southeast-2'))

    bucket_name = os.getenv('salesdatafaris22')
    prefix = os.getenv('sales_data_sample.csv')

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response.get('Contents', [])

    if not objects:
        raise Exception("No files found in S3 bucket with prefix.")

    latest_file = sorted(objects, key=lambda obj: obj['LastModified'])[-1]
    file_key = latest_file['Key']

    local_filename = '/tmp/sales_data.csv'
    s3.download_file(bucket_name, file_key, local_filename)

    print(f"Downloaded {file_key} to {local_filename}")
