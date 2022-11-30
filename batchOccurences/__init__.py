import logging
import os
import azure.functions as func
import pyodbc
import requests
from datetime import date
import json
import time
import math
from itertools import islice

db_url = os.environ['DB_URL']
db = os.environ['DB']
db_username = os.environ['DB_USERNAME']
db_password = os.environ['DB_PASSWORD']
db_driver = os.environ['DB_DRIVER']

#serverless DB retry
for i in range(0, 4):
    while True:
        try:
            cnxn = pyodbc.connect('DRIVER='+db_driver+';SERVER='+db_url+';PORT=1433;DATABASE='+db+';UID='+db_username+';PWD='+db_password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
        except pyodbc.Error as ex:
            time.sleep(2.0)
            continue
        break

cursor = cnxn.cursor()

hohk_api_url = os.environ['HOHK_API_URL']
hohk_api_username = os.environ['HOHK_API_USERNAME']
hohk_api_password = os.environ['HOHK_API_PASSWORD']

occurrence_url_prefix = os.environ['THIS_API_URL'] + '/occurrence?code=' + os.environ['OCCURRENCE_FUNCTION_CODE'] + '&occurrenceId='
jobmap_url = os.environ['THIS_API_URL'] + '/jobmap?code=' + os.environ['JOBMAP_FUNCTION_CODE']

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Call batchOccurences function.')

    start_time = time.time()

    r = requests.get(jobmap_url)
    json_response = r.json()
    #json_response = ['a0CBV000000NiT32AK','a0CBV000000OP132AG','a0CBV000000OP1B2AW','a0CBV000000OP182AG','a0CBV000000OP1A2AW','a0CBV000000OP1C2AW','a0CBV000000OP142AG','a0CBV000000OP192AG','a0CBV000000OP152AG','a0CBV000000OP2T2AW','a0CBV000000OP2Y2AW','a0CBV000000OP2a2AG','a0CBV000000OP2b2AG','a0CBV000000OP2n2AG','a0CBV000000OP2q2AG','a0CBV000000OP2V2AW','a0CBV000000OP2p2AG','a0CBV000000OP2c2AG','a0CBV000000OP2Z2AW','a0CBV000000OP5x2AG','a0CBV000000OP5z2AG','a0CBV000000OP5y2AG','a0CBV000000OP6H2AW']
    batch_size = 100
    total_record_count = 0
    batches_sent = 0
    
    #sending it to SOLR with too many occurrenceIds (100+) seems to cause problems
    l = []
    for occurrence_batch in batched(json_response, batch_size):
        occurrenceIds = ','.join(occurrence_batch)
        r2 = requests.get(occurrence_url_prefix + occurrenceIds)

        if r2.status_code == 200:
            l.extend(r2.json())
        else:
            logging.error(f"Received status code {r2.status_code} for {occurrence_url_prefix + occurrenceIds}")

    total_record_count = len(l)
    logging.info(f"Received {total_record_count} results to send")
    for batch in batched(l, batch_size):
        #send batch
        batches_sent += 1
        #need to get ones that are errored, or just ignore them until next day

    end_time = time.time()

    return func.HttpResponse(
        "Sent " + str(total_record_count) + " record(s) in " + str(batches_sent) + " batches, in " + str(end_time-start_time) + " seconds",
        status_code=200
    )

def batched(iterable, n):
    "Batch data into lists of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := list(islice(it, n))):
        yield batch