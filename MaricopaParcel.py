import requests
import os
from lxml import etree
import json
from util.AddressHelper import get_addresses_csv
from  util.ScrapeTools import xpath_element, find_element
import logging
import pprint
import threading
import traceback
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


def get_config():
    
    with open('config.json', 'r') as con_file:
        file_json = json.load(con_file)
        config = file_json['Scraping Constants']['Maricopa Parcel Scraper']
        return config

config = get_config()

# Load constants
url = config['Url']
property_attributes = config['Property Attributes']
owner_attributes = config['Owner Attributes']
valuation_attributes = config['Valuation Attributes']
additional_prop_attributes = config['Additional Prop Attributes']

chrome_options = Options()
chrome_options.add_argument("--headless")
driver = WebDriver(options=chrome_options)

drive_lock = threading.Lock()

def get_parcel_root(apn):
    # Initiallize driver and get queried page
    parcel_url = url + apn
    drive_lock.acquire()
    driver.get(parcel_url)
    driver.implicitly_wait(1)
    WebDriverWait(driver, 10).until_not(EC.text_to_be_present_in_element("", (By.XPATH, ".//div[@id='valuation']")))
    
    # Wait for source to compile and clean it for lxml root
    all_html = driver.page_source
    drive_lock.release()
    cleaned = ' '.join(all_html.split())
    
    root = etree.fromstring(cleaned, parser=etree.HTMLParser())
    
    return root

def check_house(property):
    # We want property type, whether it's owned by a holdings company, llc, etc. Check if the home is a rental or not.
    apn = property['APN']
    address = property['Property Address']
    
    root = get_parcel_root(apn)
    
    # Acquire the DOM elements of all relevant sections...
    parcel_content = find_element(root, ".//div[@id='parcel-content']")
    prop =  find_element(parcel_content, ".//div[@class='parcel-section bg-white rounded shadow py-3 px-3 pb-4 mb-4 col-12']")
    top_bar = xpath_element(parcel_content, './/div[contains(concat(" ", normalize-space(@class), " "), "parcel-section col-12 ")]')
    owner = find_element(parcel_content, ".//div[@id='owner']")
    # valuation = find_element(parcel_content, ".//div[@id='valuation']")
    additional = find_element(parcel_content, ".//div[@id='AddInfoSection']")
    
    # Confirm that the addresses match...
    full_address = f"{address} {property['City']} {property['ZIP Code']}"
    if not address_match(full_address, prop):
        logging.warning(f'Address from parcel search of APN-{apn} does not match that of the given property @ address {address}')
        return {}
    
    # Run parse functions...
    prop_json = parse_property_section(prop)
    owner_json = parse_owner_section(owner)
    additional_json = parse_additional_section(additional)
    
    aggregate = prop_json | owner_json | additional_json
    
    # Get parcel type...
    parcel_type = xpath_element(top_bar, './/h3[contains(text(), "Parcel")]')
    aggregate['Parcel Type'] = clean_text(parcel_type.text)
    
    return aggregate
    
def parse_property_section(section):
    """Parses the property section of the web page to get the data listed in the config under 'Property Attributes'

    Args:
        section: The web element node that the property section falls under.

    Returns:
        parsed: The data acquired from parsing the table in JSON format.
    """
    if section is None:
        return {}
    
    table = find_element(section, ".//div[@class='col-12 smaller-font']")
    
    children = get_relevant_children(table.getchildren(), property_attributes)
    
    parsed_property = parse_children(children)

    return parsed_property

def parse_children(elements: list):
    """Parses a list of children and forms them into a dict."""
    parsed = {}
    
    for element in elements:
        try:
            children = element.getchildren()
            
            key = clean_text(children[0].text)
            value = clean_text(children[1].text)
            
            parsed[key] = value
        except:
            logging.warning("Error encountered when parsing section, trace: ")
            traceback.print_exc()
            continue
    
    return parsed

def get_relevant_children(children, attributes):
    """Given the list of all children of a node and a list of relevant attributes checks that if the child is
    in the list of relevant attibutes these 'relevant' children will be returned as a list.

    Args:
        children (list): The list of the children of the node being parsed.
        attributes (list): The list of desired attributes as strings.

    Returns:
        list: The list of children whose title was ocntained within the attibutes table.
    """
    
    relevant = []
    
    for child in children:
        try:
            descendants = child.getchildren()
            
            if len(descendants) == 0:
                continue
            
            key = clean_text(descendants[0].text.lower())
            if key in attributes:
                relevant.append(child)
            
        except AttributeError:
            logging.error("Child missing attribute, trace: ")
            traceback.print_exc()
            continue
        except:
            logging.error("Unexpected error occurred while getting table children, trace: ")
            traceback.print_exc()
            continue
    
    return relevant

def parse_owner_section(section):
    """Parses the property section of the web page to get the data listed in the config under 'Owner Attributes'

    Args:
        section: The web element node that the section falls under.

    Returns:
        parsed: The data acquired from parsing the table in JSON format.
    """
    
    if section is None:
        return {}
    
    owner = find_element(section, ".//a").text 
    table = find_element(section, ".//div[@class='col-12 smaller-font']")
    
    children = table.getchildren()
    children = get_relevant_children(children, owner_attributes)
    
    parsed_property = parse_children(children)
    parsed_property['Owner'] = owner
    try: 
        parsed_property['Mailing Address'] = parsed_property['Mailing Address'].split(',')[0]
    except:
        traceback.print_exc()
        return parsed_property

    return parsed_property
    
def clean_text(text):
    if isinstance(text, str):
        cleaned = text.split("\\")
        return cleaned[0].strip()
    elif text is None:
        return None
    else:
        # Handle cases where text is not a string (e.g., numbers, other types)
        return str(text).strip()

def parse_valuation_section(section):
    """Parses the property section of the web page to get the data listed in the config under 'Valuation Attributes'

    Args:
        section: The web element node that the section falls under.

    Returns:
        parsed: The data acquired from parsing the table in JSON format.
    """
    
    if section is None:
        return {}
    
    table = find_element(section, ".//div[@id='valuation-data']")
    
    children = table.getchildren()
    children = get_relevant_children(children, valuation_attributes)
    
    parsed_valuation = {}
    parsed_valuation['valuation'] = parse_table(children)
    
    return parsed_valuation

def parse_table(rows):
    json_array = []
    for i in range(len(rows)):
        children = rows[i].getchildren()
        key = clean_text(children[0].text)
        for i in range(len(children) - 1):
            value = clean_text(children[i +  1].text)
            
            try: 
                (json_array[i])[key] = value
            except IndexError:
                json_array.insert(i, {key: value})
    return json_array
    
def parse_additional_section(section):
    """Parses the property section of the web page to get the data listed in the config under 'Additional Prop Attributes'

    Args:
        section: The web element node that the section falls under.

    Returns:
        parsed: The data acquired from parsing the table in JSON format.
    """
    
    if section is None:
        return {}

    table = find_element(section, ".//div[@id='AdditionalInfoPanel']")
    
    children = table.getchildren()
    children = get_relevant_children(children, additional_prop_attributes)
    
    parsed_additional = parse_children(children)
    
    return parsed_additional

def address_match(address, info_element):
    parcel_address = find_element(info_element, ".//div[@class='col-md-11 pt-3 banner-text']/a").text
    
    if parcel_address.lower() == address.lower():
        return True
    return False
