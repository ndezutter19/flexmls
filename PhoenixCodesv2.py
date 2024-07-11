import logging
import requests
import util.AddressHelper as AddressHelper
from lxml import etree
from datetime import datetime
from dateutil.relativedelta import relativedelta
import traceback
import json

def get_config():
    with open('config.json', 'r') as con_file:
        file_json = json.load(con_file)
        config = file_json['Scraping Constants']['Phoenix Code Scraper']
        return config

config = get_config()

# Load constants...
MONTHS_RECENT = config['IsRecent']
url = config['Url']

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
    if temp == "closed no violation found" or temp == "closed compliance":
        return False
    return True

def parse_entry(case_a):
    violations = []
    
    entry_url = case_a.get('href')
    case_response = requests.get(url + entry_url)
    
    # Get pane containing the titles of violations and extract their text...
    entry_tree = etree.fromstring(case_response.text, parser=etree.HTMLParser())
    property_violations_pane = entry_tree.find(".//div[@id='propertyViolationsPane']")
    
    if property_violations_pane is None:
        return ['LAW ENFORCEMENT EVENT']
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
    session = requests.Session()
    
    # Set up data for session and requests...
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"}
    form_data = {
        'stNumber': stNumber,
        'stDirection': stDirection[0],
        'stName': stName
    }
    
    # Create session and post to it...
    session.get(url, headers=headers)
    response = session.post(f"{url}/CodeEnforcement", headers=headers, json=form_data)

    return response.text

def scrape_violations(property):
        try:
            # Breakdown address into constituent parts...
            breakdown = AddressHelper.parse_address_csv(property['Property Address'])
            stNum = breakdown['stNum']
            stDir = breakdown['streetDirection']
            stType = breakdown['streetType']
            stName = f"{breakdown['streetName']} {stType}"
            unitNo = breakdown['unitNo']
            if unitNo != None : stName.append(f" {unitNo}")
            
            html = make_request(stNum, stDir, stName, unitNo)
                
            cases = parse_table(html)
                
            if len(cases.keys()) == 0:
                return None
            return cases
        except TypeError:
            # If an error occurs here it should be due to an address being incorrectly formatted so log as warning...
            logging.warning(f"Error in input data, address: {property['Property Address']}")
            traceback.print_exc()
            return None
        except:
            print(f"Error occurred, skipping address: {property['Property Address']}")
            traceback.print_exc()
            return None


