from enum import Enum
from pyairtable import Api
from pyairtable import Table
from pyairtable.formulas import match
import futureproof
import threading
import json
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
import traceback

# Entry format enum.
class EntryFormat(Enum):
    ADDRESS = {
        'Address': '',
        'APN': '',
        'Cases': [],
        'Lot Size': None,
        'High School District': None,
        'Elementary School District': None,
        'Local Jurisdiction': None,
        'Sale Date': None,
        'Sale Price': None,
        'Owner': None,
        'Mailing Address': None,
        'Construction Year': None,
        'Living Area': None,
        'Parcel Type': None
    }
    CASE_ENTRY = {
        'Case Number': '',
        'Owner/Occupant': '',
        'Status': '',
        'Open Date': '',
        'Close Date': '',
        'Address': [],
        'Type of Violations': []
    }
    TYPE_ENTRY = {
        'ID': '',
        'Name': '',
    }


def get_airtable(access_token):
    """
    Returns the airtable object to get tables and make requests with.
    """
    at = Api(access_token)
    return at

# 
def get_violation_types(table):
    """
    Returns the types of violations by querying the table and returning the IDs of all object
    in order to cache the violation types. Saves time by not requiring you to make a request to
    this table every single time you add a record. After this cache is initialized if a type is
    found that does not exist in the cache then it will be added to both the cache and the 
    airtable database.
    """
    
    types = table.all()
    cache = {}
    for item in types:
        cache[item['fields'].get('ID')] = item['id']
    return cache

#
def import_violations_data(file, file_lock: threading.Lock):
    """
    Add all violation data from the JSON file to the Airtable database.

    Args:
        file (_type_): _description_
        file_lock (threading.Lock): _description_
    """
    # Initialize api variables...
    base_id = 'appKY7QOvCIoZwI6b'
    key = os.getenv('AIRTABLE_API_KEY')
    api = get_airtable(key)
    
    # Initialize table objects...
    address_table = api.table(base_id, 'Addresses')
    cases_table = api.table(base_id, 'Complaint Cases')
    type_table = api.table(base_id, 'Type of Violations')
    
    # Initialize other holder variables...
    new_entry = None
    address_rec_id = None
    case_id = None
    type_id = None
    
    # Get violation types and cache them to prevent unneccessary requests...
    cached_types = get_violation_types(type_table)
    
    # Read initiali line
    file_lock.acquire()
    line = file.readline()
    file_lock.release()
    
    while True:
        if line == '':
            break
        
        as_json = json.loads(line)
        
        address = as_json['address']
        cases =  as_json['cases']
        parcel_data = as_json['parcel data']
        
        #in_mls = element_in_mls(address)
        formula = match({'Address': address})
        arg_dict: dict =  EntryFormat.ADDRESS.value
        arg_dict['Address'] = address
        arg_dict['APN'] = as_json['apn']
        arg_dict = process_parcel(arg_dict | parcel_data)
        
        address_rec_id = element_in_airtable(address_table, EntryFormat.ADDRESS, formula, **arg_dict)

        record_ids = []
        for case_id, case in cases.items():
            # Check if case exists...
            owner = case['owner']
            status = case['status']
            open_date = case['open date']
            close_date = case['close date']
            if close_date == '':
                close_date = None
            
            # Match to case number using a formula...
            formula = match({'Case Number': case_id})
            arg_dict =  {'Case Number': case_id, 'Owner/Occupant': owner, 'Status': status, 'Open Date': open_date, 'Close Date': close_date}
            
            # Get the id of the case whether it existed or not...
            case_id = element_in_airtable(cases_table, EntryFormat.CASE_ENTRY, formula, **arg_dict)
            record_ids.append(case_id)

            # Get the violations of this particular case...
            violation_types = case['violations']
            reference_ids = get_violation_ids(type_table, violation_types, cached_types)
            cases_table.update(case_id, {'Type of Violations': reference_ids})
        
        # Append the violations and case to the address.
        response = address_table.get(record_id=address_rec_id)
        curr_refs = response['fields'].get('Cases')
        if curr_refs != None:
            for item in curr_refs:
                if item != None and item not in record_ids:
                    record_ids.append(curr_refs)
        address_table.update(address_rec_id, {'Cases': record_ids})
        
        # Acquire the lock and read in the next object if it exists...
        file_lock.acquire()
        line = file.readline()
        file_lock.release()    


def process_parcel(data: dict):
    """
    Processses the json object 'Parcel Data' that is acquired in the address object from the json file.

    Args:
        data (dict): The parcel data object.

    Returns:
        dict: Returns the parcel data as a dict after parsing its values from strings into their respective types.
    """
    
    # Make a shallow copy to avoid modifying the original...
    shallow = data.copy()

    # Lot Size and Living area both need their prefix and commas removed to allow thee int constructor to convert them...
    if shallow['Lot Size'] != None:
        lot_size = shallow['Lot Size']
        lot_size = lot_size.removesuffix(" sq ft.")
        lot_size = int(lot_size.replace(',', ''))
        shallow['Lot Size'] = lot_size
    
    if shallow['Living Area'] != None:
        living_area = shallow['Living Area']
        living_area = living_area.removesuffix(" sq ft.")
        living_area = int(living_area.replace(',', ''))
        shallow['Living Area'] = living_area
    
    # Any parcel that hasn't been sold recently and has this field in the details will be 'n/a' instead change it to None...
    if shallow['Sale Date'] == 'n/a':
        shallow['Sale Date'] = None
    
    if shallow['Sale Price'] == 'n/a':
        shallow['Sale Price'] = None
    elif shallow['Sale Price'] != None:
        sale_price = shallow['Sale Price']
        sale_price = sale_price.removeprefix('$')
        sale_price = int(sale_price.replace(',', ''))
        shallow['Sale Price'] = sale_price
    
    if shallow['Construction Year'] != None:
        construction_year = int(shallow['Construction Year'])
        shallow['Construction Year'] = construction_year
    
    # Any key that is none should be removed since None type is not valid for Airtable...
    for key in data.keys():
        if shallow[key] == None:
            shallow.pop(key)
            
    return shallow
    
# 
def element_in_mls(address):
    try:
        response = listing_table.scan(
            FilterExpression=Attr('address').eq(address.lower())
        )
    except:
        traceback.print_exc()

# 
def element_in_airtable(table: Table, template, form, **kwargs):
    if not isinstance(template, EntryFormat):
        raise TypeError('Provided format is invalid.')
    
    resp = table.all(formula=form)
    if resp == []:
        new_entry = template.value.copy()
        for key, value in kwargs.items():
            if key in new_entry:
                new_entry[key] = value
            else:
                print(f"Warning: {key} is not a recognized key in the case entry template.")
                new_entry[key] = value
        
        resp = table.create(new_entry)
    else:
        resp = resp[0]
        check_case_updated(table, resp, **kwargs)
        
    resp_id = resp.get('id')
    
    return resp_id

# 
def check_case_updated(table: Table, resp, **kwargs):
    r_keys = list(resp['fields'].keys())
    for key, value in kwargs.items():
        if key in r_keys and resp['fields'][key] == value:
            continue
        table.update(record_id=resp['id'], fields=kwargs)
        break

# 
def get_violation_ids(table: Table, violation_types, cache):
    ref = []
    for entry in violation_types:
        split = entry.split(' ', 1)
        vio_id = split[0]
        vio_type = split[1]
        
        if vio_id not in list(cache.keys()):
            new_entry = EntryFormat.TYPE_ENTRY.value.copy()
            new_entry['ID'] = vio_id
            new_entry['Name'] = vio_type
            response = table.create(new_entry)
            cache[vio_id] = response['id']
        
        if cache[vio_id] not in ref:
            ref.append(cache[vio_id])
    return ref

# 
def run_threads(noThreads, file, file_lock):
    # Simple multithread...
    with futureproof.ThreadPoolExecutor(max_workers=noThreads) as executor:
        # Submit tasks to the executor
        futures = [executor.submit(import_violations_data, file, file_lock) for i in range(noThreads)]

global dynamodb, listing_table

access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = 'us-east-2'

dynamodb = boto3.resource('dynamodb')
listing_table = dynamodb.Table('HouseListings')


with open('data/PhxScrape.json') as f:
    file_lock = threading.Lock()
    run_threads(4, f, file_lock)
    