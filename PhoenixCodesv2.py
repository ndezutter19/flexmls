import logging
import requests
import time
import util.ScrapeTools as ScrapeTools
import json
import os
from lxml import etree
from datetime import datetime
from dateutil.relativedelta import relativedelta
import concurrent.futures
import threading
from pprint import pprint

MONTHS_RECENT = 6
form_url = "https://nsdonline.phoenix.gov"

def isRecent(open, close):
    # If no close date found then it must be recent or active
    if close == None or close == '':
        return True
    
    # Convert into datetime for comparison
    date = datetime.strptime(open, '%m/%d/%Y')
    current_date = datetime.now()
    
    delta_date = current_date - relativedelta(months=MONTHS_RECENT)
    if delta_date <= date <= current_date:
        return True
    else:
        return False

def violationConfirmed(status: str):
    # If violation status is 'closed no violation' found then return false...
    temp = status.lower()
    if temp == "closed no violation found":
        return False
    return True

def parse_entry(case_a):
    violations = []
    
    entry_url = case_a.get('href')
    case_response = requests.get(form_url + entry_url)
    
    # Get pane containing the titles of violations and extract their text...
    entry_tree = etree.fromstring(case_response.text, parser=etree.HTMLParser())
    property_violations_pane = entry_tree.find(".//div[@id='propertyViolationsPane']")
    
    violation_headers = property_violations_pane.xpath("./div[@class='jumbotron jumbo-org-name']/span/strong")
    for violation in violation_headers:
        trim = violation.text.removeprefix("Violation Code: ")
        violations.append(trim)
    
    return violations
        
def parse_table(html: str):
    cases = {}
    parser = etree.HTMLParser()
    root = etree.fromstring(html, parser)
    
    # Check if h1 is present in html, only present if redirected to access denial page...
    try:
        header = root.find('h1')
        if header is not None:
            print("We've made too many requests ;~;")
            exit(-1)
        search_results = root.xpath(".//table[@class='table table-striped table-condensed']")[0]
    except IndexError:
        return {}

    # Get the table elements...
    table_body = search_results.xpath("./tbody")[0]
    table_results = table_body.xpath('./tr')
    
    # Parse each entry in table...
    for entry in table_results:
        entry_contents = entry.xpath('./td')
        
        # Get open and close dates to check if recent, skip parsing if the status shows no violation found...
        case_status = entry_contents[2].text.strip()
        open_date = entry_contents[3].text.strip()
        close_date = entry_contents[4].text
        if close_date is not None:
            close_date = close_date.strip()
        
        if isRecent(open_date, close_date) and violationConfirmed(case_status):
            case_number = entry[0]
            case_link = case_number.xpath('./a')[0]
            violations = parse_entry(case_link)
            if len(violations) == 0:
                continue
            
            cases[case_link.text] = violations
    
    return cases

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def make_request(stNumber: int, stDirection: chr, stName: str):
    property = {}
    session = requests.Session()
    
    # Set up data for session and requests...
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"}
    form_data = {
        'stNumber': stNumber,
        'stDirection': stDirection[0],
        'stName': stName
    }
    
    # Create session and post to it...
    session.get(form_url, headers=headers)
    response = session.post(f"{form_url}/CodeEnforcement", headers=headers, json=form_data)
    
    try:
        property = parse_table(response.text)
    except:
        logging.error(f"There was an error when parsing address: {stNumber} {stDirection} {stName}", exc_info=True)
    return property


def scrape_violations(address_list, lock):
    data = {}
    while len(address_list) != 0:
        # Get the lock to pop address safely...
        lock.acquire()
        address = address_list.pop()
        lock.release()
        
        print(f"Proccessing Address: {address}")
        breakdown = ScrapeTools.break_down_address_op(address)
        
        try:
            # Breakdown address into constituent parts...
            stNum = breakdown['streetNum']
            stDir = breakdown['streetDirection']
            stType = ScrapeTools.get_abbreviated(breakdown['streetType'])
            stName = f"{breakdown['streetName']} {stType}"
            
            cases = make_request(stNum, stDir, stName)
            if len(cases.keys()) > 0:
                data[address] = cases
        except TypeError:
            # If an error occurs here it should be due to an address being incorrectly formatted so log as warning...
            logging.WARNING(f"Error in input data, address: {address}")
            continue
    
    return data

def run_threads(noThreads, addressList, lock):
    # Simple multithread...
    with concurrent.futures.ThreadPoolExecutor(max_workers=noThreads) as executor:
        # Submit tasks to the executor
        futures = [executor.submit(scrape_violations, addressList, lock) for i in range(noThreads)]
        
        # Collect results as they complete
        results = {}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results = result | results
    
    return results

# Timer for speed run fun...
start_time = time.time()
with open(os.path.join('data/PhoenixAddresses.json'), 'r') as f:
    # Create a lock to pass to all threads to prevent race conditions when accessing houses list...
    lock = threading.Lock()
    houses = json.loads(f.read())['addresses']
    global total_length
    total_length = len(houses)
    output = run_threads(4, houses, lock)
    
    # Write our to results file...
    with open('data/PhoenixAddressesResults.json', 'w') as res:
        res.write(json.dumps(output))

# Compute total time...
end_time = time.time()
delta_time = end_time - start_time
print(f"Altogether took: {delta_time}s")