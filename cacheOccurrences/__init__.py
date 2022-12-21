import logging
import os
import azure.functions as func
import pyodbc
import requests
from datetime import date
import time
from itertools import islice

import azure.functions as func

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
    logging.info('Call cacheOccurences function.')

    r = requests.get(jobmap_url)
    json_response = r.json()
    batch_size = 100

    #check to see which ones are in the DB and which aren't

    #sending it to SOLR with too many occurrenceIds (100+) seems to cause problems
    l = []
    for occurrence_batch in batched(json_response, batch_size):
        occurrenceIds = ','.join(occurrence_batch)
        r2 = requests.get(occurrence_url_prefix + occurrenceIds)

        if r2.status_code == 200:
            l.extend(r2.json())
        else:
            logging.error(f"Received status code {r2.status_code} for {occurrence_url_prefix + occurrenceIds}")


def batched(iterable, n):
    "Batch data into lists of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while (batch := list(islice(it, n))):
        yield batch