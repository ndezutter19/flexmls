from geopy.geocoders import Nominatim
import json
import requests
import time
import os
import util.ScrapeTools
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

            
def run_thread(complete_flag, buffer: list, end_point: list):
    geolocator = Nominatim(user_agent="code_scrape_proj")
    count = 1
    total = len(buffer)
    # While the listing collection process is not complete and the buffer is not empty
    # continue to get properties from buffer.
    while(complete_flag.flag != True or len(buffer) != 0):
        # If buffer is empty but process is not complete then sleep...
        try:
            house =  buffer.pop()
            print(f"New house proccessing ({count} of {total}): {house['address']}")
            
            lat = house['lat']
            lng = house['lng']
            
            city = get_city(geolocator, lat, lng)
            house['city'] = city
            count += 1
            end_point.append(house)
        except IndexError as e:
            time.sleep(5)
        
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
