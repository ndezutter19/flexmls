import traceback
from boto3.dynamodb.conditions import Key, Attr
import util.AddressHelper as AddressHelper
import os
import boto3
import pprint

access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = 'us-east-2'

dynamodb = boto3.resource('dynamodb')
listing_table = dynamodb.Table('HouseListings')
items = listing_table.scan(Limit=1)
# print(items)

import re

# Define the regex pattern
pattern = (
    r'(?P<houseNum>\d+)\s+'
    r'(?P<streetDirection>[NESW])\s+'
    r'(?P<streetName>[\w\s]+?)'
    r'\s*(?P<streetType>St|Dr|Rd|Ave|Blvd|Way|Ln)?'
    r'(?:\s+Unit\s+(?P<unitNo>\d+))?$'
)
compiled = re.compile(pattern)

# Sample addresses
addresses = [
    "5201 E Calle Redonda",
    "123 N Main St",
    "456 W Elm Ave Unit 5",
    "789 S Pine Blvd",
    "101 E Maple",
    "202 N Oak Ln",
    "303 W Cedar Way Unit 12",
    "2032 W Palmaire Ave"
]

# Function to test regex
def test_regex(pattern, addresses):
    results = []
    for address in addresses:
        match = pattern.match(address)
        if match:
            results.append(match.groupdict())
        else:
            results.append(None)
    return results

# Test the regex pattern
test_results = test_regex(compiled, addresses)
pprint.pp(test_results)
