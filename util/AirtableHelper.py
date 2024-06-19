from pyairtable import Api
from pyairtable import Table
from pyairtable.formulas import match
import futureproof
import threading
import json
import time
import os

code_vio_entry = {
    'Address': '',
    'Cases': []
}

case_entry = {
    'Case Number': '',
    'Address': [],
    'Type of Violations': []
}

violation_entry = {
    'ID': '',
    'Name': '',
}

def get_airtable(access_token):
    at = Api(access_token)
    return at

def import_violations_data(file, file_lock: threading.Lock):
    base_id = 'appKY7QOvCIoZwI6b'
    key = os.getenv('AIRTABLE_API_KEY')
    api = get_airtable(key)
    
    file_lock.acquire()
    line = file.readline()
    file_lock.release()
    
    code_vio_table = api.table(base_id, 'Code Violations')
    cases_table = api.table(base_id, 'Complaint Cases')
    type_table = api.table(base_id, 'Type of Violations')
    
    new_entry = None
    
    types = type_table.all()
    cached_types = {}
    for item in types:
        cached_types[item['fields'].get('ID')] = item['id']
    
    code_vio_id = None
    case_id = None
    type_id = None
    
    while True:
        if line == '':
            break
        as_json = json.loads(line)
        
        address = list(as_json.keys())[0]
        print(f"Adding {address} to airtable.")
        
        cases =  as_json[address]
        case_ids = list(cases.keys())

        frm = match({'Address': address})
        response = code_vio_table.all(formula=frm)
        if response == []:
            new_entry = code_vio_entry
            new_entry['Address'] = address
            response = code_vio_table.create(new_entry)
            code_vio_id = response['id']
        else:
            code_vio_id = response[0].get('id')
        
        record_ids = []
        for id in case_ids:
            # Check if case exists...
            frm = match({'Case Number': id})
            response = cases_table.all(formula=frm)
            time.sleep(0.1)
            if response == []:
                new_entry = case_entry
                new_entry['Case Number'] = id
                new_entry['Address'] = [code_vio_id]
                response = cases_table.create(new_entry)
                time.sleep(0.1)
                case_id = response['id']
            else:
                case_id = response[0].get('id')
            record_ids.append(case_id)

            # Check if the violation types exist within the violations table...
            violation_types = cases[id]
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


def get_violation_ids(table: Table, violation_types, cache):
    ref = []
    for entry in violation_types:
        split = entry.split(' ', 1)
        vio_id = split[0]
        vio_type = split[1]
        
        if vio_id not in list(cache.keys()):
            new_entry = violation_entry
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

        
with open('data/PhoenixAddressesResultsTransfer.json') as f:
    file_lock = threading.Lock()
    run_threads(4, f, file_lock)