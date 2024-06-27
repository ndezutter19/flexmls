from geopy.geocoders import Nominatim
import time
import csv
import logging
import re

def get_city(geolocator, lat, lng):
    long =  str(lng)
    lati = str(lat)
    
    location = geolocator.reverse(lati+ "," + long)
    address = location.raw['address']
    city = address.get('city', '')
    
    return city

        
def get_addresses_csv():
    with open('data/PhoenixAddr1of2.CSV', mode ='r') as file:    
        csvFile = csv.DictReader(file)
        listed = []
        for line in csvFile:
            listed.append(line)
        return listed

def parse_address_csv(address):
    # Log the input address
    logging.debug(f"Parsing address: {address}")

    # Define the regular expression pattern
    pattern = (
        r'(?P<stNum>\d+)\s+'
        r'(?P<streetDirection>[NESW])\s+'
        r'(?P<streetName>[\w\s]+?)'
        r'\s*(?P<streetType>St|Dr|Rd|Ave|Blvd|Way|Ln)?'
        r'(?:\s+Unit\s+(?P<unitNo>\d+))?$'
    )

    logging.debug(f"Using pattern: {pattern}")

    # Match the pattern against the address
    match = re.match(pattern, address, re.IGNORECASE)
    
    if match:
        logging.debug("Address matched successfully.")
        return match.groupdict()
    else:
        logging.warning(f"Address {address} could not be parsed.")
        return None
