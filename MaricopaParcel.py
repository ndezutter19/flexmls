import requests
import os
from lxml import etree
import json
from util.AddressHelper import get_addresses_csv
from  util.ScrapeTools import get_element
import logging
import pprint
import traceback
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


global config


with open('config.json', 'r') as con_file:
    file_json = json.load(con_file)
    config = file_json['Scraping Constants']['Maricopa Parcel Scraper']


def load_constants():
    global url, property_attributes, owner_attributes, valuation_attributes, additional_prop_attributes
    
    url = config['Url']
    property_attributes = config['Property Attributes']
    owner_attributes = config['Owner Attributes']
    valuation_attributes = config['Valuation Attributes']
    additional_prop_attributes = config['Additional Prop Attributes']


def get_parcel_root(apn):
    # Initiallize driver and get queried page
    global driver
    driver = WebDriver()
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    
    driver.get(url + apn)
    driver.implicitly_wait(1)
    WebDriverWait(driver, 10).until_not(EC.text_to_be_present_in_element("", (By.XPATH, ".//div[@id='valuation']")))
    
    # Wait for source to compile and clean it for lxml root
    all_html = driver.page_source
    cleaned = ' '.join(all_html.split())
    
    root = etree.fromstring(cleaned, parser=etree.HTMLParser())
    
    return root

def check_house(house):
    # We want property type, whether it's owned by a holdings company, llc, etc. Check if the home is a rental or not.
    print("Things")
    apn = house['APN']
    address = house['Property Address']
    
    root = get_parcel_root(apn)
    
    parcel_content = get_element(root, ".//div[@id='parcel-content']")
    prop =  get_element(parcel_content, ".//div[@class='parcel-section bg-white rounded shadow py-3 px-3 pb-4 mb-4 col-12']")    
    owner = get_element(parcel_content, ".//div[@id='owner']")
    valuation = get_element(parcel_content, ".//div[@id='valuation']")
    additional = get_element(parcel_content, ".//div[@id='AddInfoSection']")
    
    # Confirm that the addresses match
    if not address_match(f'{address} {house['City']} {house['ZIP Code']}', prop):
        logging.warning(f'Address from parcel search of APN-{apn} does not match that of the given property @ address {address}')
        return {}
    
    prop_json = parse_property_section(prop)
    owner_json = parse_owner_section(owner)
    valuation_json = parse_valuation_section(valuation)
    additional_json = parse_additional_section(additional)
    
    total = prop_json | owner_json | valuation_json | additional_json
    
    return total
    
def parse_property_section(section):
    if section is None:
        return {}
    
    table = get_element(section, ".//div[@class='col-12 smaller-font']")
    
    children = get_relevant_children(table.getchildren(), property_attributes)
    
    parsed_property = parse_children(children)

    return parsed_property

def parse_children(elements: list):
    parsed = {}
    
    for element in elements:
        try:
            children = element.getchildren()
            
            key = children[0].text
            value = children[1].text
            
            parsed[key] = value
        except:
            logging.warning("Error encountered when parsing section, trace: ")
            traceback.print_exc()
            continue
    
    return parsed

def get_relevant_children(children, attributes):
    relevant = []
    
    for child in children:
        try:
            descendants = child.getchildren()
            
            if len(descendants) == 0:
                continue
            
            key = descendants[0].text.lower()
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
    if section is None:
        return {}
    
    owner = get_element(section, ".//a").text 
    table = get_element(section, ".//div[@class='col-12 smaller-font']")
    
    children = table.getchildren()
    children = get_relevant_children(children, owner_attributes)
    
    parsed_property = parse_children(children)
    parsed_property['Owner'] = owner
    return parsed_property
    


def parse_valuation_section(section):
    if section is None:
        return {}
    
    table = get_element(section, ".//div[@id='valuation-data']")
    
    children = table.getchildren()
    children = get_relevant_children(children, valuation_attributes)
    
    parsed_valuation = parse_children(children)
    
    return parsed_valuation
    
    
def parse_additional_section(section):
    if section is None:
        return {}

    table = get_element(section, ".//div[@id='AdditionalInfoPanel']")
    
    children = table.getchildren()
    children = get_relevant_children(children, additional_prop_attributes)
    
    parsed_additional = parse_children(children)
    
    return parsed_additional

def address_match(address, info_element):
    parcel_address = get_element(info_element, ".//div[@class='col-md-11 pt-3 banner-text']/a").text
    
    if parcel_address.lower() == address.lower():
        return True
    return False
