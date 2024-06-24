import traceback
from boto3.dynamodb.conditions import Key, Attr
import util.AddressHelper as AddressHelper

houses = AddressHelper.get_addresses_csv()
for house in houses:
    address = house['Property Address']
    try:
        breakdown = AddressHelper.parse_address_csv(address)
        
        stNum = breakdown['streetNum']
        stDir = breakdown['streetDirection']
        stType = breakdown['streetType']
        stName = f"{breakdown['streetName']} {stType}"
    except:
        traceback.print_exc()
        print("Error Occurred")