from enum import Enum
from pyairtable import Api
from pyairtable import Table
from pyairtable.formulas import match
import futureproof
import threading
import json
import time
import os

class EntryFormat(Enum):
    CODE_VIO = {
        'Address': '',
        'Cases': []
    }
    CASE_ENTRY = {
        'Case Number': '',
        'Owner/Occupant': '',
        'Status': '',
        'Open Date': '',
        'Address': [],
        'Type of Violations': []
    }
    TYPE_ENTRY = {
        'ID': '',
        'Name': '',
    }

def get_airtable(access_token):
    at = Api(access_token)
    return at

def get_violation_types(table):
    types = table.all()
    cache = {}
    for item in types:
        cache[item['fields'].get('ID')] = item['id']
    return cache

def import_violations_data(file, file_lock: threading.Lock):
    # Initialize api variables...
    base_id = 'appKY7QOvCIoZwI6b'
    key = os.getenv('AIRTABLE_API_KEY')
    api = get_airtable(key)
    
    # Initialize table objects...
    code_vio_table = api.table(base_id, 'Code Violations')
    cases_table = api.table(base_id, 'Complaint Cases')
    type_table = api.table(base_id, 'Type of Violations')
    
    # Initialize other holder variables...
    new_entry = None
    code_vio_id = None
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
        

        formula = match({'Address': address})
        code_vio_id = does_element_exist(code_vio_table, EntryFormat.CODE_VIO, formula, **{'Address': address})

        record_ids = []
        for case_id, case in cases.items():
            # Check if case exists...
            owner = case['owner']
            status = case['status']
            open_date = case['open date']
            close_date = case['close date']
            
            formula = match({'Case Number': case_id})
            arg_dict =  {'Case Number': case_id, 'Owner/Occupant': owner, 'Status': status, 'Open Date': open_date}
            if not (close_date == '' or close_date is None):
                arg_dict['Close Date'] = close_date
            
            case_id = does_element_exist(cases_table, EntryFormat.CASE_ENTRY, formula, **arg_dict)
            record_ids.append(case_id)

            violation_types = case['violations']
            reference_ids = get_violation_ids(type_table, violation_types, cached_types)
            cases_table.update(case_id, {'Type of Violations': reference_ids})
        
        response = code_vio_table.get(record_id=code_vio_id)
        curr_refs = response['fields'].get('Cases')
        if curr_refs != None:
            for item in curr_refs:
                if item != None and item not in record_ids:
                    record_ids.append(curr_refs)
        code_vio_table.update(code_vio_id, {'Cases': record_ids})
        
        file_lock.acquire()
        line = file.readline()
        file_lock.release()    

def does_element_exist(table: Table, template, form, **kwargs):
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
        
        resp = table.create(new_entry)
    elif template is EntryFormat.CASE_ENTRY:
        check_case_updated(table, resp[0], **kwargs)
        
    resp_id = resp[0].get('id')
    
    return resp_id

def check_case_updated(table: Table, resp, **kwargs):
    r_keys = list(resp['fields'].keys())
    for key, value in kwargs.items():
        if key in r_keys and resp['fields'][key] == value:
            continue
        table.update(record_id=resp['id'], fields=kwargs)
        break
    

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

def run_threads(noThreads, file, file_lock):
    # Simple multithread...
    with futureproof.ThreadPoolExecutor(max_workers=noThreads) as executor:
        # Submit tasks to the executor
        futures = [executor.submit(import_violations_data, file, file_lock) for i in range(noThreads)]

        
with open('data/PhoeAddrResultsTransfer2.json') as f:
    file_lock = threading.Lock()
    run_threads(6, f, file_lock)
    