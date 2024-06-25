import traceback
from boto3.dynamodb.conditions import Key, Attr
import util.AddressHelper as AddressHelper
import os
import boto3

access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = 'us-east-2'

dynamodb = boto3.resource('dynamodb')
listing_table = dynamodb.Table('HouseListings')
items = listing_table.scan(Limit=1)
print(items)