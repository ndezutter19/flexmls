import boto3
import os
from boto3.dynamodb.conditions import Key, Attr

access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = 'us-east-2'

dynamodb = boto3.resource('dynamodb', region_name=aws_region)
table = dynamodb.Table('HouseListings')
response = table.scan(Limit=3)

if 'Items' in response:
    for item in response['Items']:
        print(item['address'])
else:
    print("No items found in the table.")