#geocoding lat-long
import os
from dotenv import load_dotenv
import json
import boto3
import pandas as pd

load_dotenv()


geocode_client = boto3.client('geo-places', region_name='us-west-2')

def get_coordinates(df):
    if 'latitude' not in df.columns:
        df['latitude'] = None

    if 'longitude' not in df.columns:
        df['longitude'] = None
    for idx, row in df.iterrows():
        address = row['address']

        try:
            response = geocode_client.geocode(
                QueryText=address,
                MaxResults=1
            )
        except Exception as e:
            print(f"Error adding coordinate, error: {e}")
        if response['ResultItems']:
            place = response['ResultItems'][0]
            coordinates = place['Position']
            print(f"Latitude: {coordinates[1]}; Longitude: {coordinates[0]}; address: {address}")


            df.at[idx, 'longitude'] = coordinates[0]
            df.at[idx, 'latitude'] = coordinates[1] 
        
            print(f"Added coordinates for address {address}")
        else:
            print(f"Coordinates for {address} not added")
    
    return df

df = pd.read_csv('/Users/aayushkumbhare/Desktop/cafo-contamination/final_csvs/2023_final.csv')
df = get_coordinates(df)
df.to_csv('2023_final.csv')

