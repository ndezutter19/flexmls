import logging
import requests
import time
import util.AddressHelper as AddressHelper
import futureproof
from lxml import etree
from datetime import datetime
from dateutil.relativedelta import relativedelta
import concurrent.futures
import threading
import traceback
import json
from tqdm import tqdm

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
    
    if property_violations_pane is None:
        return['LAW ENFORCEMENT EVENT']
    violation_headers = property_violations_pane.xpath("./div[@class='jumbotron jumbo-org-name']/span/strong")
    for violation in violation_headers:
        trim = violation.text.removeprefix("Violation Code: ")
        violations.append(trim)
    
    return violations

def clean_text(text):
    if isinstance(text, str):
        return text.strip()
    elif text is None:
        return None
    else:
        # Handle cases where text is not a string (e.g., numbers, other types)
        return str(text).strip()

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
        case_status = clean_text(entry_contents[2].text)
        open_date = clean_text(entry_contents[3].text)
        close_date = clean_text(entry_contents[4].text)
        owner =  clean_text(entry_contents[5].text)
        
        if isRecent(open_date, close_date) and violationConfirmed(case_status):
            case_number = entry[0]
            case_link = case_number.xpath('./a')[0]
            violations = parse_entry(case_link)
            if len(violations) == 0:
                continue
            
            cases[case_link.text] = {
                'owner': owner,
                'status': case_status,
                'open date': open_date,
                'close date': close_date,
                'violations': violations
            }
    
    return cases

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def make_request(stNumber: int, stDirection: chr, stName: str, unitNo):
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

def write_result(result):    
    with open(f'data/PhoeAddrResults-{time_stamp}.json', 'a') as file_out:
        file_out.write(json.dumps(result) + '\n')

def scrape_violations(address_list, lock, write_lock, prog_bar):
    while len(address_list) != 0:
        # Get the lock to pop address safely...
        lock.acquire()
        address = address_list.pop()
        prog_bar.update(1)
        lock.release()
        
        try:
            # Breakdown address into constituent parts...
            breakdown = AddressHelper.parse_address_csv(address['Property Address'])
            stNum = breakdown['stNum']
            stDir = breakdown['streetDirection']
            stType = breakdown['streetType']
            stName = f"{breakdown['streetName']} {stType}"
            unitNo = breakdown['unitNo']
            if unitNo != None : stName.append(f" {unitNo}")
            
            cases = make_request(stNum, stDir, stName, unitNo)
            if len(cases.keys()) > 0:
                write_lock.acquire()
                write_result({'address': address['Property Address'],
                              'cases': cases})
                write_lock.release()
        except TypeError:
            # If an error occurs here it should be due to an address being incorrectly formatted so log as warning...
            logging.warning(f"Error in input data, address: {address['Property Address']}")
            traceback.print_exc()
            continue
        except:
            print(f"Error occurred, skipping address: {address['Property Address']}")
            traceback.print_exc()
            continue
    return True

def run_threads(noThreads, addressList, lock, write_lock, prog_bar):
    # Simple multithread...
    with futureproof.ThreadPoolExecutor(max_workers=noThreads + 1) as executor:
        # Submit tasks to the executor
        futures = [executor.submit(scrape_violations, addressList, lock, write_lock, prog_bar) for i in range(noThreads)]


# Timer for speed run fun...
start_time = time.time()

result_buffer = []
global total_length
global file_name

# Create a lock to pass to all threads to prevent race conditions when accessing houses list...
lock = threading.Lock()
write_lock = threading.Lock()
res_lock = threading.Lock()
houses_ex = AddressHelper.get_addresses_csv()
houses = [{'Property Address': '2045 W Northview Ave'}, {'Property Address':'2046 W Northview Ave'}, {'Property Address':'2032 W Palmaire Ave'}]
prog_bar = tqdm(total=len(houses), desc='Processing addresses...', unit='Address', bar_format='{l_bar} {bar} Addresses: {n_fmt}/{total_fmt} ({percentage:.1f}%)   Elapsed: {elapsed}   Remaining: {remaining}')
current_time = datetime.now()

time_stamp = f"{current_time.year}-{current_time.month}-{current_time.day}_{current_time.hour}-{current_time.minute}-{current_time.second}"
total_length = len(houses)
file_name = f"data/PhoeAddrResults-{time_stamp}.json"
run_threads(1, houses, lock, write_lock, prog_bar)

# Compute total time...
end_time = time.time()
delta_time = end_time - start_time
print(f"Altogether took: {delta_time}s")
prog_bar.close()
