import re
import logging
from lxml import etree

def break_down_address(address):
    # Define the regular expression pattern
    pattern = r'(?P<streetNum>\d+)\s+(?P<streetDirection>[NSEW])\s+(?P<streetName>[a-zA-Z0-9\s]+)\s+(?P<streetType>[a-zA-Z]+)(?:\s+#(?P<unitNo>\d+))?'

    # Match the pattern against the address
    match = re.match(pattern, address, re.IGNORECASE)
    
    if match:
        logging.debug(f"Successfully Parsed Address: {address}")
        return match.groupdict()
    else:
        logging.warning(f"Could Not Parse Address: {address}")
        return None

def break_down_address_op(address):
    # Log the input address
    logging.debug(f"Parsing address: {address}")

    # Define the regular expression pattern
    # Define the regular expression pattern
    pattern = (r'(?P<streetDirection>[NSEW][a-z]*)\s+'
               r'(?P<streetName>[a-zA-Z0-9\s]+)\s+'
               r'(?P<streetType>[a-zA-Z]+)\s+'
               r'(?:(?P<suffixDirection>[NSEW])\s+)?'
               r'(?P<streetNum>\d+)')

    # Match the pattern against the address
    match = re.match(pattern, address, re.IGNORECASE)
    
    if match:
        logging.debug("Address matched successfully.")
        return match.groupdict()
    else:
        logging.warning("Address could not be parsed.")
        return None

def get_abbreviated(suffix: str):
    temp = suffix.lower()
    
    # Dictionary mapping full street suffixes to USPS standard abbreviations
    suffix_dict = {
        'alley': 'ALY',
        'avenue': 'AVE',
        'boulevard': 'BLVD',
        'circle': 'CIR',
        'court': 'CT',
        'drive': 'DR',
        'expressway': 'EXPY',
        'highway': 'HWY',
        'lane': 'LN',
        'parkway': 'PKWY',
        'place': 'PL',
        'road': 'RD',
        'square': 'SQ',
        'street': 'ST',
        'trail': 'TRL',
        'way': 'WAY'
    }
    
    try:
        abbreviated = suffix_dict[temp]
        return abbreviated
    except KeyError:
        logging.warning(f"Abbreviated version of {temp} not found, returning input string, append to dictionary if need be.")
        return temp.upper()
    
def xpath_element(root, xpath_str):
    """
    Helper function to attempt to get something with xpath AND using the xpath function which allows for inline functions to be executed,
    if error is thrown because no such element exists then None will be returned instead.
    """
    
    try:
        elements = root.xpath(xpath_str)
        if len(elements) > 1:
            return elements
        return elements[0]
    except:
        return None
    
def find_element(root, xpath_str):
    """
    Helper function to attempt to get something via with xpath but using the find function, if error is thrown because no such
    element exists then None will be returned instead.
    """
    
    try:
        element = root.find(xpath_str)
        return element
    except:
        return None