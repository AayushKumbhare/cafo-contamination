import pandas as pd
from textract import Textract
import boto3
from dotenv import load_dotenv
import os
import time
import logging

load_dotenv()

logging.basicConfig(filename='example.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ROLE_ARN = os.getenv('ROLE_ARN')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')

df = pd.DataFrame()
sqs_resource = boto3.resource('sqs')


path = '/Users/aayushkumbhare/Desktop/cafo-contamination/cleaned_2022'
filenames = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
textract = Textract('cafo-contamination')
count = 0
total = len(filenames)
for file in filenames:
    print(file)
    file_key = f'cleaned-pdfs-2022/{file}'
    if textract.check_file(file_key):
        try:
            job_id = textract.start_job(file_key)
            print(f"Job ID: {job_id}")
            response = textract.wait_for_job_to_complete(job_id)
            df_row = textract.extract_all_statistics_to_dataframe_row(response)
            df = pd.concat([df, pd.DataFrame([df_row])], ignore_index=True)
            print(f"Successfully added row for {file}")
            count += 1
            print(f"{count}/{total} files extracted.")
        except Exception as e:
            print(f"Error adding {file} to df: {e}")
    else:
        print("File not found, skipping...")
        continue



print(df.head())
df.to_csv('cleaned_2022.csv')
