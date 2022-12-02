import logging
import os
import azure.functions as func
import pyodbc
import requests
from datetime import date
import time
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
    batch_size = 100
    jc_batch_size = 10
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

    accessToken = getAccessToken()
    if accessToken is None:
        return func.HttpResponse(
            "Could not obtain accessToken",
            status_code=400
        )

    for batch in batched(l, jc_batch_size):
        #send batch
        upsertVOs(accessToken, batch)
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

jc_api_url = os.environ['JC_API_URL']
jc_api_username = os.environ['JC_API_USERNAME']
jc_api_login_path = os.environ['JC_API_LOGIN_PATH']
def getAccessToken():
    retries = 1
    while retries < 3:
        r = requests.post(f"http://{jc_api_url}/{jc_api_login_path}", json={'email': jc_api_username})
        if r.status_code == 200:
            return r.json().get('accessToken')
        else:
            wait = retries * 3 
            time.sleep(wait)
            retries += 1
    
    return None

jc_api_upsert_path = os.environ['JC_API_UPSERT_PATH']
def upsertVOs(accessToken, list):
    retries = 1
    head = {'Authorization': 'Bearer ' + accessToken}
    while retries < 3:
        r = requests.post(f"http://{jc_api_url}/{jc_api_upsert_path}", json=list, headers=head)
        if r.status_code == 200:
            #logging.info(r.json())
            return
        else:
            wait = retries * 3 
            time.sleep(wait)
            retries += 1
    
    logging.info("Failed to upsert")
