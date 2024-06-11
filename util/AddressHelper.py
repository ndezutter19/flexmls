from geopy.geocoders import Nominatim
import json
import requests
import time

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

def get_addresses_in_city():

    # Define the Overpass API endpoint
    overpass_url = "http://overpass-api.de/api/interpreter"

    # Define the Overpass query
    # This query retrieves the first 10 nodes and ways tagged as residential buildings within Phoenix, AZ.
    overpass_query = """
    [out:json][timeout:25];
    area["name"="Phoenix"]["boundary"="administrative"]["admin_level"="8"]->.searchArea;
    (
    node["building"="residential"](area.searchArea);
    way["building"="residential"](area.searchArea);
    node["building"="house"](area.searchArea);
    way["building"="house"](area.searchArea);
    node["building"="apartments"](area.searchArea);
    way["building"="apartments"](area.searchArea);
    node["building"="detached"](area.searchArea);
    way["building"="detached"](area.searchArea);
    node["building"="semi-detached"](area.searchArea);
    way["building"="semi-detached"](area.searchArea);
    node["building"="terrace"](area.searchArea);
    way["building"="terrace"](area.searchArea);
    );
    out body;
    """

    # Make the request to the Overpass API
    response = requests.post(overpass_url, data={'data': overpass_query})
    data = response.json()
    addresses = []

    for element in data['elements']:
        if 'tags' in element and 'addr:housenumber' in element['tags']:
            address = element['tags'].get('addr:street', '') + ' ' + element['tags']['addr:housenumber']
            addresses.append(address)
    
    return addresses

data = {}
with open("data/PhoenixAddresses.json", 'w') as pj:
    data['addresses'] = get_addresses_in_city()
    serialized = json.dumps(data)
    pj.write(serialized)