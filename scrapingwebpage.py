import os
import requests
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Define the list of listing numbers
listing_numbers = ["6657971", "6655603", "6522854", "6676532", "6676916"]

# Define the cookies
cookies = {
    '_oauth_session': 'w2Fmke7qJj3wLCASLXS73b80z+c7e31bOkP8k0aX6LIiQWFPbF0XI+SrM4oAzOQe8DlvtqbnTMsMpEU7ikSnyiwPMfe807gJzkuvWzr7s3eyotAarNb6--rMC3RBgbxdUabtIw--/gXk6pTmd9tmW1hEYuy66Q==',
    'agent_tech_id': '20080516021715125287000000',
    'ajs_anonymous_id': '"18fef80dd684e-0ea86c01bd3a8a8-42282e32-157188-18fef80dd69f11"',
    'cid': '665f5b04f13f50.00283241',
    'flex_private_oauth_sso_session_id': 'flex_private_oauth_bae201d2e67a6cec51176a0ef8f69ed5',
    'fpjs_user_id': '"20080516021715125287000000"',
    'mp_098dee69-35a9-4daf-bf51-e181f1a314a2_perfalytics': '{"distinct_id": "20080516021715125287000000","$device_id": "18fef80dd684e-0ea86c01bd3a8a8-42282e32-157188-18fef80dd69f11","$auiddc": "1546517856.1717710151","$pageview_id": "190035748cb1a1-0d2c0950e280b7-42282e32-157188-190035748cc12d7","__last_event_time": 1718043333066,"$session_id": "190035748cd122b-083571eca0ebd18-42282e32-157188-190035748cefbb","__first_pageview_in_session_has_occurred": true,"__session_count": 3,"$debug_client_info": {"ctr": {"_sendEvent": 44}},"__initial_utm_props_set": true,"$initial_referrer": "$direct","$initial_referring_domain": "$direct","__first_pageview_occurred": true,"__user_props": {"MlsId": "20070913202326493241000000","LoginName": "mm814"},"$user_id": "20080516021715125287000000","MlsId": "20070913202326493241000000","__last_pageview_time": 1718042970320}',
    'Ticket': 'TFQ9MTcxODA0Mjk2OC4zNDQzMjMmaXA9MTg0LjE3Ni4xNTkuNDMmdGltZT0xNzE4MDQyOTY4JnVzZXI9bW04MTQmaGFzaD05NWFlMmNmNWEwODU5MWJhMThjZDJjMmQ3NmFmYTI3OCZ1YWg9Nzg0OTE5MzZmYWVkNGQyM2E3ZTA5OWMzOGRhYzI2NjY'
}

def get_text(soup, label):
    element = soup.find(string=label)
    if element:
        next_sibling = element.parent.find_next_sibling(string=True)
        if next_sibling:
            return next_sibling.strip()
    return ""

def get_beds_baths(soup):
    beds_baths_label = soup.find('span', string='Beds/Baths:')
    if beds_baths_label:
        beds_baths = beds_baths_label.find_next_sibling(string=True)
        if beds_baths:
            return beds_baths.strip()
    return "N/A"

def clean_sqft(sqft):
    if sqft:
        return sqft.split('/')[0].strip()
    return "N/A"

def get_subdivision(soup):
    subdivision_label = soup.find('span', string='Subdivision:')
    if subdivision_label:
        subdivision = subdivision_label.find_next_sibling(string=True)
        if subdivision:
            return subdivision.strip()
    return "N/A"

def fetch_data(listing_number):
    try:
        # Step 1: Initial request to get the listing ID
        initial_url = f'https://apps.flexmls.com/quick_launch/herald?callback=lookupCallback&_filter={listing_number}&ql=true&search_id=4ff5ce15&client=flexmls&_selfirst=false&parse={listing_number}&_=1718042971014'
        initial_response = requests.get(initial_url, cookies=cookies)
        initial_data = initial_response.text

        # Extract the full listing ID from the initial response
        match = re.search(r'"Id":"(\d+)"', initial_data)
        if match:
            full_listing_id = match.group(1)
        else:
            print(f"Listing ID not found in the initial response for listing number {listing_number}.")
            return

        # Step 2: Use the listing ID in the next request
        final_url = f'https://armls.flexmls.com/cgi-bin/mainmenu.cgi?cmd=url%20reports/dispatcher/display_custom_report.html&wait_var=5&please_wait_override=Y&report_grid=&report_title=&fontsize=&spacing=&auto_print_report=&allow_linkbar=N&s_supp=Y&report=c,20080718115541627326000000,wysr&linkbar_toggle=&report_type=private&buscardselect=generic&override_copyright=system&qcount=1&c1=x%27{full_listing_id}%27&srch_rs=true'

        # Send the final request and get the response
        final_response = requests.get(final_url, cookies=cookies)

        # Extract relevant data from the HTML response
        soup = BeautifulSoup(final_response.text, 'html.parser')
        address_element = soup.find("td", style="text-align: center; vertical-align: top;")
        if address_element:
            address = address_element.text.strip()
            zip_code = re.search(r'\b\d{5}\b', address).group() if re.search(r'\b\d{5}\b', address) else "N/A"
        else:
            address = "N/A"
            zip_code = "N/A"

        beds_baths = get_beds_baths(soup)
        beds, baths = beds_baths.split("/") if beds_baths != "N/A" else ("N/A", "N/A")

        sqft_raw = get_text(soup, "Approx SqFt:")
        sqft = clean_sqft(sqft_raw)

        price_element = soup.find("td", style="text-align: right; vertical-align: top;")
        price = price_element.text.strip() if price_element else "N/A"

        subdivision = get_subdivision(soup)

        data = {
            "listing_id": listing_number,
            "address": address,
            "zip_code": zip_code,
            "price": price,
            "sqft": sqft,
            "# of Interior Levels": get_text(soup, "# of Interior Levels:"),
            "price_per_sqft": get_text(soup, "Price/SqFt:"),
            "year_built": get_text(soup, "Year Built:"),
            "beds": beds.strip(),
            "baths": baths.strip(),
            "subdivision": subdivision
        }

        print(f"Successfully processed listing number {listing_number}")
        return data

    except Exception as e:
        print(f"Error processing listing number {listing_number}: {e}")

# Use ThreadPoolExecutor to run the requests concurrently
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(fetch_data, listing_number) for listing_number in listing_numbers]
    results = []
    for future in as_completed(futures):
        result = future.result()
        if result:
            results.append(result)

# Save results to a JSON file
output_file = 'data/listing_data.json'
with open(output_file, 'w') as file:
    json.dump(results, file, indent=4)

print(f"Data saved to {output_file}")
