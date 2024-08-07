import concurrent.futures
import MaricopaParcel as mp
import PhoenixCodesv2 as pc
import threading
import util.AddressHelper as AddressHelper
from tqdm import tqdm
import time
from datetime import datetime
import concurrent
import json

global todo, write_file

def load_constants():
    global filename, lock
    
    # Intialize thread safety features...
    lock = threading.Lock()
    
    # Get current date for timestamp to attach to written out file name...
    current_time = datetime.now()
    time_stamp = f"{current_time.year}-{current_time.month}-{current_time.day}_{current_time.hour}-{current_time.minute}-{current_time.second}"
    filename = f"data/PhxScrapeResults{time_stamp}.json"

    open(filename, 'w')

def run():
    todo = AddressHelper.get_addresses_csv()
    prog_bar = tqdm(total=len(todo), desc='Processing addresses...', unit='Address', bar_format='{l_bar} {bar} Addresses: {n_fmt}/{total_fmt} ({percentage:.1f}%)   Elapsed: {elapsed}   Remaining: {remaining}')

    # Timer for speed run fun...
    start_time = time.time()
    
    # Seperate threads into IO bound and CPU bound, request threads pull html data, CPU threads analyze and operate on them.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(scrape_address, property) for property in todo]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                prog_bar.update()
            except:
                continue
            
    prog_bar.close()
    
    # Compute total time...
    end_time = time.time()
    delta_time = end_time - start_time
    print(f"Altogether took: {delta_time}s")
    
def write_out(result: dict):
    with open(filename, 'a') as write_file:
        write_file.write(json.dumps(result) + "\n")
        write_file.close()

def scrape_address(property: dict):
    result = {'apn': property['APN'], 'address': property['Property Address']}
    codes_result = pc.scrape_violations(property)
    
    if codes_result is not None:
        result['cases'] = codes_result
        parcel_result = mp.check_house(property)
        result['parcel data'] = parcel_result
        
        write_out(result)
    
example = {'APN': '123-16-047', 'FIPS Code': '4013', 'County Name': 'Maricopa County', 'Property Address': '4034 E Pecan Rd', 'City': 'Phoenix', 'State': 'AZ', 'ZIP Code': '85040', 'Owner Name(s) Formatted': 'Neal & Regina Ruggie', 'Mailing Address': '4034 E Pecan Rd', 'Mailing City': 'Phoenix', 'Mailing State': 'AZ', 'Mailing ZIP Code': '85040', 'Subdivision': 'Knoell Garden Groves Unit 6', 'Detailed Property Type': 'Single Family Residential'}

load_constants()
run()
