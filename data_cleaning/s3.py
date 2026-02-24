import boto3
import os
from dotenv import load_dotenv
load_dotenv()

s3_client = boto3.client('s3')
textract_client = boto3.client('textract')
BUCKET_NAME = 'cafo-contamination'

def test_s3():
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=1)
        print("CONNECTED")
        return True
    except Exception as e:
        print(f"ERROR: {e}")
        return False


def upload_to_s3(local_path, s3_filename):
    try:
        s3_key = f'cleaned-pdfs-2022/{s3_filename}'
        s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
        print(f"Uploaded {s3_filename}")
        return True
    except Exception as e:
        print(f"Failed {e}")
        return False


def batch_upload(folder_path):
    successful, failed = 0, 0
    try:
        pdf_files = [f for f in os.listdir(folder_path) if f.endswith('.pdf')]

        for pdf in pdf_files:
            local_path = os.path.join(folder_path, pdf)
            if upload_to_s3(local_path, pdf):
                successful += 1
            else:
                failed += 1
    
        print(f"Uploaded {successful} documents to S3")
        print(f"Failed to upload {failed} documents to S3")
        return successful
    except Exception as e:
        print("Error {e}")
        return 0



batch_upload('/Users/aayushkumbhare/Desktop/cafo-contamination/cleaned_2022')