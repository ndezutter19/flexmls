from geopy.geocoders import Nominatim
import json
import requests
import time
import os
import csv
import util.ScrapeTools as ScrapeTools
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

def get_addresses():
    overpass_url = "http://overpass-api.de/api/interpreter"
    query = """
    [out:json];
    node["addr:street"]["addr:housenumber"]["addr:city"="Phoenix"](33.29026, -112.32462, 33.83972, -111.92556);
    out;
    """
    
    response = requests.get(overpass_url, params={'data': query})
    resp_json = response.json()
    data = resp_json['elements']
    print(f"Pre-sanitization Length: {len(data)}")
    data = sanitize_data(data)
    print(f"Post-sanitization Length: {len(data)}")
    with open('data/sanitized_addresses.json', 'w') as f:
        addresses = {'addresses': data}
        json.dump(addresses, f)
        
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
        r'(?P<streetNum>\d+)\s+'
        r'(?P<streetDirection>[NSEW])\s+'
        r'(?P<streetName>[a-zA-Z0-9\s]+?)\s+'
        r'(?P<streetType>St|Dr|Rd|Ave|Blvd|Ln|Ct|Pl|Terr|Cir|Pkwy|Way|Trl)\s*'
        r'(?:Unit\s*(?P<unitNo>\d+))?'
    )
    logging.debug(f"Using pattern: {pattern}")

    # Match the pattern against the address
    match = re.match(pattern, address, re.IGNORECASE)
    
    if match:
        logging.debug("Address matched successfully.")
        return match.groupdict()
    else:
        logging.warning("Address could not be parsed.")
        return None
    
def sanitize_data(data: list):
    sanitized = []
    for element in data:
        if 'amenity' in element['tags'].keys() or 'shop' in element['tags'].keys():
            continue
        street_route = element['tags']['addr:street'].split(' ')
        street_direction = (street_route[0])[0]
        street_type = ScrapeTools.get_abbreviated(street_route[len(street_route) - 1])
        name_list = street_route[1:len(street_route) - 1]
        street_name = ' '.join(name_list)
        
        sanitized.append({
            'number': element['tags']['addr:housenumber'],
            'streetName': street_name,
            'streetDir': street_direction,
            'streetType': street_type,
        })
    
    return sanitized